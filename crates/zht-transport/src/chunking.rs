//! Big Data Transfer — chunked message protocol for large payloads.
//!
//! When a serialized `ZPack` exceeds the configured maximum transfer size,
//! it is split into fixed-size chunks. Each chunk carries a 38-byte header
//! compatible with the original C++ ZHT `bigdata_transfer` protocol.
//!
//! # Wire Format (per chunk)
//!
//! ```text
//! [20 bytes: uuid] [7 bytes: seq_num] [7 bytes: total] [4 bytes: payload_size] [N bytes: payload]
//! ```
//!
//! - **uuid**: Unique identifier for the message being chunked (padded to 20 bytes).
//! - **seq_num**: Zero-indexed sequence number of this chunk (padded to 7 bytes).
//! - **total**: Total number of chunks in this message (padded to 7 bytes).
//! - **payload_size**: Byte count of the payload in *this* chunk (padded to 4 bytes).
//! - **payload**: Raw bytes of this chunk's slice of the original message.
//!
//! # Reassembly
//!
//! Chunks are collected into a `HashMap<u64, Vec<u8>>` keyed by sequence number.
//! When all `total` chunks have arrived, they are concatenated in order to
//! reconstruct the original message.
//!
//! # Performance Notes
//!
//! The chunk size is configurable. The default (`CHUNK_PAYLOAD_SIZE = 512`)
//! matches the C++ original's `BUF_SIZE - 38 = 512`. Larger chunk sizes
//! reduce the number of round-trips but increase per-datagram size for UDP.

use std::collections::HashMap;

use zht_common::error::{ZhtError, ZhtResult};
use zht_common::id_helper::IdHelper;

use crate::metrics::TransportMetrics;

/// Default payload size per chunk (512 bytes), matching C++ `BUF_SIZE - 38`.
pub const CHUNK_PAYLOAD_SIZE: usize = 512;

/// Total chunk header size: uuid(20) + seq_num(7) + total(7) + payload_size(4) = 38.
pub const CHUNK_HEADER_SIZE: usize = 38;

/// Maximum message size before chunking is triggered (1 MB by default).
/// Messages smaller than this threshold are sent as a single frame without
/// chunking. This can be overridden by the `MSG_MAXSIZE` config parameter.
pub const DEFAULT_CHUNK_THRESHOLD: usize = 1_048_576;

/// A single chunk of a large message, ready for transmission.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Serialized header + payload bytes.
    pub data: Vec<u8>,
    /// Sequence number of this chunk (0-indexed).
    pub seq_num: u64,
    /// Total number of chunks in this message.
    pub total: u64,
}

/// State for reassembling incoming chunks into a complete message.
///
/// Thread safety is provided by wrapping in `tokio::sync::Mutex` when used
/// in an async context. The struct itself is `Send` but not `Sync` because
/// it contains non-atomic state.
pub struct ChunkReassembler {
    /// Collected chunks: seq_num -> payload bytes.
    chunks: HashMap<u64, Vec<u8>>,
    /// Total number of chunks expected.
    total: u64,
    /// The unique ID for this message.
    msg_id: u64,
    /// Payload size for this message (used to pre-allocate the final buffer).
    full_size: usize,
}

impl ChunkReassembler {
    /// Create a new reassembler for a message with the given ID and total chunks.
    pub fn new(msg_id: u64, total: u64, full_size: usize) -> Self {
        Self {
            chunks: HashMap::with_capacity(total as usize),
            total,
            msg_id,
            full_size,
        }
    }

    /// Get the message ID.
    pub fn msg_id(&self) -> u64 {
        self.msg_id
    }

    /// Insert a chunk into the reassembler.
    ///
    /// Returns `true` if this was the final chunk needed to complete the message.
    pub fn insert(&mut self, seq_num: u64, payload: Vec<u8>) -> bool {
        self.chunks.insert(seq_num, payload);
        self.chunks.len() == self.total as usize
    }

    /// Check if all chunks have been received.
    pub fn is_complete(&self) -> bool {
        self.chunks.len() == self.total as usize
    }

    /// Reassemble the complete message from all received chunks.
    ///
    /// # Panics
    /// Panics if not all chunks have been received. Call `is_complete()` first.
    pub fn reassemble(&self) -> Vec<u8> {
        assert!(self.is_complete(), "Cannot reassemble incomplete message");

        let mut result = Vec::with_capacity(self.full_size);
        for seq in 0..self.total {
            let payload = self.chunks.get(&seq).expect("Missing chunk during reassembly");
            result.extend_from_slice(payload);
        }
        result
    }

    /// Return the number of chunks received so far.
    pub fn received_count(&self) -> usize {
        self.chunks.len()
    }
}

/// Fragment a byte payload into chunks ready for network transmission.
///
/// # Arguments
/// * `data` - The complete serialized message to chunk.
/// * `chunk_payload_size` - Maximum payload bytes per chunk (default: `CHUNK_PAYLOAD_SIZE`).
///
/// # Returns
/// A `Vec<Chunk>` in sequence order, along with the message ID assigned.
///
/// # Performance
/// For payloads smaller than `chunk_payload_size`, returns a single chunk.
pub fn chunk_message(data: &[u8], chunk_payload_size: usize) -> (u64, Vec<Chunk>) {
    let msg_id = IdHelper::gen_id();
    let total = if data.is_empty() {
        1
    } else {
        (data.len() + chunk_payload_size - 1) / chunk_payload_size
    };

    let mut chunks = Vec::with_capacity(total);
    for seq in 0..total {
        let start = seq * chunk_payload_size;
        let end = std::cmp::min(start + chunk_payload_size, data.len());
        let payload = &data[start..end];

        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + payload.len());

        // UUID (20 bytes, left-padded with spaces)
        let uuid_str = format!("{:>20}", msg_id);
        buf.extend_from_slice(uuid_str.as_bytes());

        // Sequence number (7 bytes, right-padded with spaces)
        let seq_str = format!("{:<7}", seq);
        buf.extend_from_slice(seq_str.as_bytes());

        // Total chunks (7 bytes, right-padded with spaces)
        let total_str = format!("{:<7}", total);
        buf.extend_from_slice(total_str.as_bytes());

        // Payload size (4 bytes, right-padded with spaces)
        let size_str = format!("{:<4}", payload.len());
        buf.extend_from_slice(size_str.as_bytes());

        // Payload
        buf.extend_from_slice(payload);

        chunks.push(Chunk {
            data: buf,
            seq_num: seq as u64,
            total: total as u64,
        });
    }

    (msg_id, chunks)
}

/// Parse a single chunk from raw bytes.
///
/// # Arguments
/// * `raw` - Raw bytes received from the network (must be >= 38 bytes).
///
/// # Returns
/// A tuple of (msg_id, seq_num, total, payload_bytes).
///
/// # Errors
/// Returns `ZhtError::RecvFailed` if the raw data is too short for the header.
pub fn parse_chunk(raw: &[u8]) -> ZhtResult<(u64, u64, u64, &[u8])> {
    if raw.len() < CHUNK_HEADER_SIZE {
        return Err(ZhtError::RecvFailed(format!(
            "Chunk too short: expected >= {} bytes, got {}",
            CHUNK_HEADER_SIZE,
            raw.len()
        )));
    }

    // Parse uuid (20 bytes)
    let uuid_str = std::str::from_utf8(&raw[0..20])
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid UUID encoding: {}", e)))?;
    let msg_id: u64 = uuid_str
        .trim()
        .parse()
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid UUID '{}': {}", uuid_str.trim(), e)))?;

    // Parse seq_num (7 bytes)
    let seq_str = std::str::from_utf8(&raw[20..27])
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid seq_num encoding: {}", e)))?;
    let seq_num: u64 = seq_str
        .trim()
        .parse()
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid seq_num '{}': {}", seq_str.trim(), e)))?;

    // Parse total (7 bytes)
    let total_str = std::str::from_utf8(&raw[27..34])
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid total encoding: {}", e)))?;
    let total: u64 = total_str
        .trim()
        .parse()
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid total '{}': {}", total_str.trim(), e)))?;

    // Parse payload_size (4 bytes)
    let size_str = std::str::from_utf8(&raw[34..38])
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid payload_size encoding: {}", e)))?;
    let payload_size: usize = size_str
        .trim()
        .parse()
        .map_err(|e| ZhtError::RecvFailed(format!("Invalid payload_size '{}': {}", size_str.trim(), e)))?;

    if raw.len() < CHUNK_HEADER_SIZE + payload_size {
        return Err(ZhtError::RecvFailed(format!(
            "Chunk payload truncated: expected {} bytes, got {}",
            payload_size,
            raw.len() - CHUNK_HEADER_SIZE
        )));
    }

    let payload = &raw[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + payload_size];
    Ok((msg_id, seq_num, total, payload))
}

/// A chunk-aware receiver that collects chunks and reassembles complete messages.
///
/// Tracks in-flight reassemblers by message ID. When all chunks for a
/// message arrive, the reassembled bytes are returned.
pub struct ChunkReceiver {
    /// In-flight reassemblers: msg_id -> ChunkReassembler.
    inflight: HashMap<u64, ChunkReassembler>,
    /// Optional metrics reference.
    metrics: Option<std::sync::Arc<TransportMetrics>>,
}

impl ChunkReceiver {
    /// Create a new chunk receiver.
    pub fn new() -> Self {
        Self {
            inflight: HashMap::new(),
            metrics: None,
        }
    }

    /// Attach a metrics instance for tracking chunk operations.
    pub fn with_metrics(mut self, metrics: std::sync::Arc<TransportMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Feed a raw chunk into the receiver.
    ///
    /// Returns `Some(reassembled_bytes)` when all chunks for a message
    /// have been received, or `None` if more chunks are still needed.
    pub fn receive(&mut self, raw: &[u8]) -> ZhtResult<Option<Vec<u8>>> {
        let (msg_id, seq_num, total, payload) = parse_chunk(raw)?;

        if let Some(ref metrics) = self.metrics {
            metrics.inc_chunks_received();
        }

        // New message — create reassembler
        if !self.inflight.contains_key(&msg_id) {
            // Estimate full size from total chunks
            let full_size = total as usize * CHUNK_PAYLOAD_SIZE;
            self.inflight.insert(msg_id, ChunkReassembler::new(msg_id, total, full_size));
        }

        let reassembler = self.inflight.get_mut(&msg_id).unwrap();
        let is_complete = reassembler.insert(seq_num, payload.to_vec());

        if is_complete {
            let reassembler = self.inflight.remove(&msg_id).unwrap();
            Ok(Some(reassembler.reassemble()))
        } else {
            Ok(None)
        }
    }

    /// Return the number of in-flight (incomplete) messages.
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    /// Discard all in-flight reassemblers (e.g., on connection close).
    pub fn clear(&mut self) {
        self.inflight.clear();
    }
}

impl Default for ChunkReceiver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_message_single_chunk() {
        let data = b"hello world";
        let (msg_id, chunks) = chunk_message(data, 512);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].seq_num, 0);
        assert_eq!(chunks[0].total, 1);

        // Verify the chunk can be parsed back
        let (id, seq, total, payload) = parse_chunk(&chunks[0].data).unwrap();
        assert_eq!(id, msg_id);
        assert_eq!(seq, 0);
        assert_eq!(total, 1);
        assert_eq!(payload, data);
    }

    #[test]
    fn test_chunk_message_multiple_chunks() {
        // 1500 bytes / 512 payload per chunk = 3 chunks (512 + 512 + 476)
        let data = vec![0xABu8; 1500];
        let (msg_id, chunks) = chunk_message(&data, 512);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].seq_num, 0);
        assert_eq!(chunks[1].seq_num, 1);
        assert_eq!(chunks[2].seq_num, 2);

        for c in &chunks {
            assert_eq!(c.total, 3);
        }

        // Verify each chunk is parseable
        for c in &chunks {
            let (id, _, _, payload) = parse_chunk(&c.data).unwrap();
            assert_eq!(id, msg_id);
            assert!(payload.len() <= 512);
        }
    }

    #[test]
    fn test_chunk_empty_data() {
        let data: &[u8] = b"";
        let (_msg_id, chunks) = chunk_message(data, 512);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_chunk_exact_boundary() {
        // Exactly 512 bytes = 1 chunk
        let data = vec![0xFFu8; 512];
        let (_msg_id, chunks) = chunk_message(&data, 512);
        assert_eq!(chunks.len(), 1);

        // 513 bytes = 2 chunks (512 + 1)
        let data = vec![0xFFu8; 513];
        let (_msg_id, chunks) = chunk_message(&data, 512);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn test_chunk_header_size() {
        let (msg_id, chunks) = chunk_message(b"test", 512);
        assert_eq!(chunks[0].data.len(), CHUNK_HEADER_SIZE + 4); // 38 + 4 bytes payload
        assert!(chunks[0].data.len() >= CHUNK_HEADER_SIZE);
    }

    #[test]
    fn test_parse_chunk_too_short() {
        let result = parse_chunk(&[0u8; 10]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_parse_chunk_invalid_uuid() {
        let mut raw = vec![0u8; CHUNK_HEADER_SIZE + 4];
        // Put invalid UTF-8 in uuid area
        raw[0] = 0xFF;
        raw[1] = 0xFE;
        let result = parse_chunk(&raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_reassembler_basic() {
        let mut r = ChunkReassembler::new(42, 3, 1500);
        assert!(!r.is_complete());

        r.insert(0, vec![1; 512]);
        assert!(!r.is_complete());
        assert_eq!(r.received_count(), 1);

        r.insert(1, vec![2; 512]);
        assert!(!r.is_complete());

        r.insert(2, vec![3; 476]);
        assert!(r.is_complete());

        let assembled = r.reassemble();
        assert_eq!(assembled.len(), 1500);
        assert_eq!(&assembled[0..512], &[1u8; 512]);
        assert_eq!(&assembled[512..1024], &[2u8; 512]);
        assert_eq!(&assembled[1024..1500], &[3u8; 476]);
    }

    #[test]
    fn test_reassembler_out_of_order() {
        let mut r = ChunkReassembler::new(99, 3, 6);
        r.insert(2, vec![3]);
        r.insert(0, vec![1]);
        assert!(!r.is_complete());
        r.insert(1, vec![2]);
        assert!(r.is_complete());

        let assembled = r.reassemble();
        assert_eq!(assembled, vec![1, 2, 3]);
    }

    #[test]
    fn test_chunk_receiver_roundtrip() {
        let original = vec![0x42u8; 1500];
        let (msg_id, chunks) = chunk_message(&original, 512);

        let mut receiver = ChunkReceiver::new();
        assert_eq!(receiver.inflight_count(), 0);

        // Send first two chunks — should not complete
        assert!(receiver.receive(&chunks[0].data).unwrap().is_none());
        assert!(receiver.receive(&chunks[1].data).unwrap().is_none());
        assert_eq!(receiver.inflight_count(), 1);

        // Send final chunk — should complete
        let result = receiver.receive(&chunks[2].data).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), original);
        assert_eq!(receiver.inflight_count(), 0);
    }

    #[test]
    fn test_chunk_receiver_multiple_messages() {
        let data1 = vec![0xAA; 600];
        let data2 = vec![0xBB; 200];

        let (_, chunks1) = chunk_message(&data1, 512);
        let (_, chunks2) = chunk_message(&data2, 512);

        let mut receiver = ChunkReceiver::new();

        // Interleave chunks from two messages
        assert!(receiver.receive(&chunks1[0].data).unwrap().is_none());
        assert!(receiver.receive(&chunks2[0].data).unwrap().is_some()); // data2 is 1 chunk
        assert!(receiver.receive(&chunks1[1].data).unwrap().is_some()); // data1 completes
    }

    #[test]
    fn test_chunk_receiver_clear() {
        let data = vec![0u8; 1500];
        let (_, chunks) = chunk_message(&data, 512);

        let mut receiver = ChunkReceiver::new();
        receiver.receive(&chunks[0].data).unwrap();
        assert_eq!(receiver.inflight_count(), 1);

        receiver.clear();
        assert_eq!(receiver.inflight_count(), 0);
    }

    #[test]
    fn test_chunk_receiver_with_metrics() {
        let metrics = std::sync::Arc::new(TransportMetrics::new());
        let mut receiver = ChunkReceiver::new().with_metrics(metrics.clone());

        let data = vec![0u8; 200];
        let (_, chunks) = chunk_message(&data, 512);
        receiver.receive(&chunks[0].data).unwrap();

        let snap = metrics.snapshot();
        assert_eq!(snap.chunks_received, 1);
    }
}
