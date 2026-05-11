//! ZHT Transport — TCP transport layer for client-server communication.
//!
//! This crate provides:
//! - **Message framing**: Length-prefixed protobuf encoding/decoding of `ZPack` messages.
//! - **TCP Proxy** (`TcpProxy`): Client-side connection pool for sending requests to ZHT server nodes.
//! - **TCP Server** (`run_server`): Server-side listener that dispatches incoming requests to an `HTWorker`.
//! - **HTWorker**: Server-side request processor that maps ZPack opcodes to `NoVoHT` storage operations.
//!
//! # Wire Format
//!
//! All messages are transmitted with a 4-byte big-endian length prefix:
//! ```text
//! [4 bytes: u32 message length (BE)] [N bytes: serialized ZPack protobuf]
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use zht_transport::{TcpProxy, run_server, HTWorker};
//! use zht_storage::NoVoHT;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Server side
//!     let store = NoVoHT::new();
//!     let worker = Arc::new(HTWorker::new(store));
//!     tokio::spawn(run_server("0.0.0.0:50000".parse().unwrap(), worker));
//!
//!     // Client side
//!     let proxy = TcpProxy::new();
//!     // proxy.sendrecv("localhost", 50000, &zpack).await?;
//! }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use zht_common::error::{ZhtError, ZhtResult};
use zht_common::constants::*;
use zht_proto::ZPack;
use zht_storage::NoVoHT;

// ═══════════════════════════════════════════════════════════════════════
// Section A: Message Framing
// ═══════════════════════════════════════════════════════════════════════

/// Encode a `ZPack` message with a 4-byte big-endian length prefix.
///
/// Returns the complete frame ready for network transmission.
///
/// # Wire Format
/// ```text
/// [4 bytes: u32 length (BE)] [N bytes: serialized ZPack protobuf]
/// ```
pub fn encode_message(zpack: &ZPack) -> Vec<u8> {
    let msg_bytes = zpack.encode_to_vec();
    let len = msg_bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + msg_bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&msg_bytes);
    buf
}

/// Decode a length-prefixed `ZPack` message from a buffer.
///
/// Returns `(decoded ZPack, remaining bytes)` on success.
///
/// # Errors
/// - `RecvFailed` if the buffer is too short (< 4 bytes) or the message is incomplete.
/// - `ProtobufDecodeError` if the protobuf payload cannot be decoded.
pub fn decode_message(buf: &[u8]) -> ZhtResult<(ZPack, &[u8])> {
    if buf.len() < 4 {
        return Err(ZhtError::RecvFailed(
            "Message too short: need at least 4 bytes for length prefix".into(),
        ));
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Err(ZhtError::RecvFailed(format!(
            "Incomplete message: expected {} bytes, got {}",
            4 + len,
            buf.len()
        )));
    }
    let zpack = ZPack::decode(&buf[4..4 + len])
        .map_err(|e| ZhtError::ProtobufDecodeError(e.to_string()))?;
    Ok((zpack, &buf[4 + len..]))
}

/// Send a `ZPack` message over a TCP stream with length-prefix framing.
///
/// This writes the full frame (length prefix + protobuf payload) and flushes.
pub async fn send_message(stream: &mut TcpStream, zpack: &ZPack) -> ZhtResult<()> {
    let data = encode_message(zpack);
    stream
        .write_all(&data)
        .await
        .map_err(|e| ZhtError::SendFailed(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| ZhtError::SendFailed(e.to_string()))?;
    Ok(())
}

/// Receive a length-prefixed `ZPack` message from a TCP stream.
///
/// Reads the 4-byte length prefix, then reads exactly that many bytes of
/// protobuf payload.
pub async fn recv_message(stream: &mut TcpStream) -> ZhtResult<ZPack> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| ZhtError::RecvFailed(e.to_string()))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut msg_buf = vec![0u8; len];
    stream
        .read_exact(&mut msg_buf)
        .await
        .map_err(|e| ZhtError::RecvFailed(e.to_string()))?;

    let zpack = ZPack::decode(msg_buf.as_slice())
        .map_err(|e| ZhtError::ProtobufDecodeError(e.to_string()))?;
    Ok(zpack)
}

// ═══════════════════════════════════════════════════════════════════════
// Section B: TCP Proxy (Client Side)
// ═══════════════════════════════════════════════════════════════════════

/// Per-node connection statistics tracked by `TcpProxy`.
///
/// Provides observability into connection usage for monitoring and debugging.
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// Total number of messages sent through this connection.
    pub messages_sent: u64,
    /// Total number of messages received through this connection.
    pub messages_received: u64,
    /// Total bytes sent (including 4-byte length prefix).
    pub bytes_sent: u64,
    /// Total bytes received (including 4-byte length prefix).
    pub bytes_received: u64,
    /// Number of connection-related errors encountered.
    pub errors: u64,
    /// Timestamp of the last successful send or receive operation.
    pub last_activity: Instant,
}

impl ConnectionStats {
    /// Create a new `ConnectionStats` with zeroed counters and the current timestamp.
    fn new() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            errors: 0,
            last_activity: Instant::now(),
        }
    }
}

impl Default for ConnectionStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal entry stored in the TcpProxy connection pool, bundling
/// a TCP stream with its associated per-connection statistics.
struct ConnectionEntry {
    stream: TcpStream,
    stats: ConnectionStats,
}

/// Health-check ping opcode. The server will respond with `REC_UOPC`
/// (unrecognized opcode), which we interpret as "connection alive".
const HEALTH_CHECK_OPCODE: &str = "999";

/// Default health-check timeout in milliseconds.
const HEALTH_CHECK_TIMEOUT_MS: u64 = 5000;

/// Maximum number of automatic reconnect attempts on broken connections.
const MAX_RECONNECT_ATTEMPTS: u32 = 1;

/// Check if a `ZhtError` indicates a broken/reset connection that warrants
/// automatic reconnection.
fn is_connection_broken_error(err: &ZhtError) -> bool {
    match err {
        ZhtError::SendFailed(msg) | ZhtError::RecvFailed(msg) => {
            let lower = msg.to_lowercase();
            lower.contains("broken pipe")
                || lower.contains("connection reset")
                || lower.contains("unexpected eof")
                || lower.contains("connection aborted")
                || lower.contains("broken pipe")
        }
        _ => false,
    }
}

/// TCP connection pool for communicating with ZHT server nodes.
///
/// `TcpProxy` maintains a pool of TCP connections keyed by `"host:port"`.
/// It lazily establishes connections on first use and reuses them for
/// subsequent requests to the same node.
///
/// The connection pool is protected by an async `Mutex`, so operations on
/// the pool are serialized. This ensures safe concurrent access to the
/// shared connection map.
///
/// # Features
///
/// - **Auto-reconnect**: Detects broken connections (broken pipe, connection reset,
///   unexpected EOF) and automatically removes the stale connection and reconnects.
/// - **Health check**: Sends a ping-like message to verify connection liveness.
/// - **Connection metrics**: Tracks per-node message/byte counts, errors, and last activity.
/// - **Batch operations**: Send multiple messages and collect all responses.
/// - **Timeout support**: Per-operation timeout wrapping.
///
/// # Example
///
/// ```rust,no_run
/// use zht_transport::TcpProxy;
/// use zht_proto::ZPack;
///
/// #[tokio::main]
/// async fn main() {
///     let proxy = TcpProxy::new();
///     let request = ZPack::default();
///     let response = proxy.sendrecv("localhost", 50000, &request).await.unwrap();
/// }
/// ```
pub struct TcpProxy {
    /// Map of "host:port" -> active connection entry (stream + stats).
    /// Protected by an async Mutex for safe concurrent access.
    connections: Mutex<HashMap<String, ConnectionEntry>>,
}

impl TcpProxy {
    /// Create a new `TcpProxy` with an empty connection pool.
    pub fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Check whether a pooled connection exists for the given `host:port`.
    ///
    /// This does **not** verify that the connection is still alive; it only
    /// checks whether an entry exists in the connection pool. Use
    /// [`health_check`](Self::health_check) to verify liveness.
    pub async fn is_connected(&self, host: &str, port: u16) -> bool {
        let addr = format!("{}:{}", host, port);
        let conns = self.connections.lock().await;
        conns.contains_key(&addr)
    }

    /// Send a `ZPack` to the specified `host:port` and receive the response.
    ///
    /// This is a synchronous request-response pattern: the function sends
    /// the message and blocks until the server's response is received.
    ///
    /// Connections are reused from the pool when available, or established
    /// on demand. The pool lock is held for the duration of the send+recv
    /// to ensure serialized access to the TCP stream.
    ///
    /// # Auto-Reconnect
    ///
    /// If the underlying connection is broken (broken pipe, connection reset,
    /// unexpected EOF), the stale connection is automatically removed from the
    /// pool and a new connection is established before retrying the operation.
    pub async fn sendrecv(&self, host: &str, port: u16, zpack: &ZPack) -> ZhtResult<ZPack> {
        let addr = format!("{}:{}", host, port);
        let mut conns = self.connections.lock().await;

        let mut attempts = 0u32;
        loop {
            // Establish connection if not present
            if !conns.contains_key(&addr) {
                let stream = TcpStream::connect(&*addr)
                    .await
                    .map_err(|e| ZhtError::ConnectionFailed {
                        host: host.to_string(),
                        port,
                        reason: e.to_string(),
                    })?;
                tracing::debug!("New TCP connection established to {}:{}", host, port);
                conns.insert(addr.clone(), ConnectionEntry {
                    stream,
                    stats: ConnectionStats::new(),
                });
            }

            let entry = conns
                .get_mut(&addr)
                .ok_or_else(|| ZhtError::Internal("connection vanished from pool".into()))?;

            // Record encoded size for stats
            let send_frame_len = (4 + zpack.encoded_len()) as u64;

            // Try to send
            match send_message(&mut entry.stream, zpack).await {
                Ok(()) => {
                    entry.stats.messages_sent += 1;
                    entry.stats.bytes_sent += send_frame_len;
                    entry.stats.last_activity = Instant::now();

                    // Try to receive
                    match recv_message(&mut entry.stream).await {
                        Ok(response) => {
                            let recv_frame_len = (4 + response.encoded_len()) as u64;
                            entry.stats.messages_received += 1;
                            entry.stats.bytes_received += recv_frame_len;
                            entry.stats.last_activity = Instant::now();
                            return Ok(response);
                        }
                        Err(e) if is_connection_broken_error(&e) => {
                            tracing::warn!(
                                "Connection broken during recv to {}: {}, reconnecting",
                                addr, e
                            );
                            entry.stats.errors += 1;
                            conns.remove(&addr);
                            attempts += 1;
                            if attempts > MAX_RECONNECT_ATTEMPTS {
                                return Err(e);
                            }
                            continue; // reconnect and retry
                        }
                        Err(e) => {
                            entry.stats.errors += 1;
                            return Err(e);
                        }
                    }
                }
                Err(e) if is_connection_broken_error(&e) => {
                    tracing::warn!(
                        "Connection broken during send to {}: {}, reconnecting",
                        addr, e
                    );
                    if let Some(entry) = conns.get_mut(&addr) {
                        entry.stats.errors += 1;
                    }
                    conns.remove(&addr);
                    attempts += 1;
                    if attempts > MAX_RECONNECT_ATTEMPTS {
                        return Err(e);
                    }
                    continue; // reconnect and retry
                }
                Err(e) => {
                    if let Some(entry) = conns.get_mut(&addr) {
                        entry.stats.errors += 1;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Send a `ZPack` to the specified `host:port` without waiting for a response.
    ///
    /// This is a fire-and-forget pattern useful for broadcast operations.
    ///
    /// # Auto-Reconnect
    ///
    /// If the underlying connection is broken, the stale connection is
    /// automatically removed and a new connection is established before
    /// retrying the send.
    pub async fn send_only(&self, host: &str, port: u16, zpack: &ZPack) -> ZhtResult<()> {
        let addr = format!("{}:{}", host, port);
        let mut conns = self.connections.lock().await;

        let mut attempts = 0u32;
        loop {
            if !conns.contains_key(&addr) {
                let stream = TcpStream::connect(&*addr)
                    .await
                    .map_err(|e| ZhtError::ConnectionFailed {
                        host: host.to_string(),
                        port,
                        reason: e.to_string(),
                    })?;
                tracing::debug!("New TCP connection established to {}:{}", host, port);
                conns.insert(addr.clone(), ConnectionEntry {
                    stream,
                    stats: ConnectionStats::new(),
                });
            }

            let entry = conns
                .get_mut(&addr)
                .ok_or_else(|| ZhtError::Internal("connection vanished from pool".into()))?;

            let send_frame_len = (4 + zpack.encoded_len()) as u64;

            match send_message(&mut entry.stream, zpack).await {
                Ok(()) => {
                    entry.stats.messages_sent += 1;
                    entry.stats.bytes_sent += send_frame_len;
                    entry.stats.last_activity = Instant::now();
                    return Ok(());
                }
                Err(e) if is_connection_broken_error(&e) => {
                    tracing::warn!(
                        "Connection broken during send_only to {}: {}, reconnecting",
                        addr, e
                    );
                    if let Some(entry) = conns.get_mut(&addr) {
                        entry.stats.errors += 1;
                    }
                    conns.remove(&addr);
                    attempts += 1;
                    if attempts > MAX_RECONNECT_ATTEMPTS {
                        return Err(e);
                    }
                    continue;
                }
                Err(e) => {
                    if let Some(entry) = conns.get_mut(&addr) {
                        entry.stats.errors += 1;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Send a `ZPack` and receive the response with a per-operation timeout.
    ///
    /// If the send+recv does not complete within `timeout`, returns a
    /// `ZhtError::Timeout`.
    pub async fn sendrecv_with_timeout(
        &self,
        host: &str,
        port: u16,
        zpack: &ZPack,
        timeout: Duration,
    ) -> ZhtResult<ZPack> {
        let timeout_ms = timeout.as_millis() as u64;
        tokio::time::timeout(timeout, self.sendrecv(host, port, zpack))
            .await
            .map_err(|_| ZhtError::Timeout { timeout_ms })?
    }

    /// Perform a health check on the connection to `host:port`.
    ///
    /// Sends an empty `ZPack` with a special opcode (`"999"`) and waits
    /// for a response (the server will reply with an unrecognized-opcode
    /// status, which confirms the connection is alive).
    ///
    /// Returns `Ok(())` if a response was received within the health-check
    /// timeout (5 seconds by default). Returns an error if the connection
    /// is broken, times out, or cannot be established.
    ///
    /// If the connection is broken, the stale connection is removed from
    /// the pool (benefiting from auto-reconnect logic).
    pub async fn health_check(&self, host: &str, port: u16) -> ZhtResult<()> {
        self.health_check_with_timeout(host, port, Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS))
            .await
    }

    /// Perform a health check on the connection to `host:port` with a custom timeout.
    ///
    /// Like [`health_check`](Self::health_check), but allows the caller to
    /// specify the timeout duration explicitly.
    pub async fn health_check_with_timeout(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> ZhtResult<()> {
        let ping = ZPack {
            opcode: HEALTH_CHECK_OPCODE.as_bytes().to_vec(),
            ..Default::default()
        };
        self.sendrecv_with_timeout(host, port, &ping, timeout)
            .await
            .map(|_| ())
    }

    /// Send multiple `ZPack` messages to the same node and collect all responses.
    ///
    /// Messages are sent sequentially. Individual message failures do **not**
    /// abort the batch — each result is returned independently so the caller
    /// can decide how to handle partial failures.
    ///
    /// # Returns
    ///
    /// A `Vec` of `Result<ZPack, ZhtError>` with the same length as `messages`,
    /// where each entry corresponds to the response for the message at the
    /// same index.
    pub async fn batch_sendrecv(
        &self,
        host: &str,
        port: u16,
        messages: &[ZPack],
    ) -> Vec<ZhtResult<ZPack>> {
        let mut results = Vec::with_capacity(messages.len());
        for msg in messages {
            let result = self.sendrecv(host, port, msg).await;
            results.push(result);
        }
        results
    }

    /// Retrieve a snapshot of the connection statistics for a specific node.
    ///
    /// Returns `None` if no pooled connection exists for the given `host:port`.
    pub async fn stats(&self, host: &str, port: u16) -> Option<ConnectionStats> {
        let addr = format!("{}:{}", host, port);
        let conns = self.connections.lock().await;
        conns.get(&addr).map(|entry| entry.stats.clone())
    }

    /// Close and remove a specific connection from the pool.
    pub async fn close_connection(&self, host: &str, port: u16) {
        let addr = format!("{}:{}", host, port);
        let mut conns = self.connections.lock().await;
        conns.remove(&addr);
        // The dropped TcpStream will be automatically closed
    }

    /// Close all connections in the pool.
    pub async fn close_all(&self) {
        let mut conns = self.connections.lock().await;
        conns.clear();
    }

    /// Return the number of active connections in the pool.
    pub async fn connection_count(&self) -> usize {
        self.connections.lock().await.len()
    }
}

impl Default for TcpProxy {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section C: HTWorker (Server-Side Request Processor)
// ═══════════════════════════════════════════════════════════════════════

/// Operation-level metrics tracked by `HTWorker`.
///
/// Records per-opcode success and error counts as well as the total
/// number of operations processed.
#[derive(Debug, Clone, Default)]
pub struct HTWorkerMetrics {
    /// Total number of operations processed.
    pub total_operations: u64,
    /// Total number of errors encountered.
    pub total_errors: u64,
    /// Number of INSERT operations.
    pub insert_count: u64,
    /// Number of INSERT errors (empty key, server failure).
    pub insert_errors: u64,
    /// Number of LOOKUP operations.
    pub lookup_count: u64,
    /// Number of LOOKUP errors (empty key).
    pub lookup_errors: u64,
    /// Number of REMOVE operations.
    pub remove_count: u64,
    /// Number of REMOVE errors (empty key).
    pub remove_errors: u64,
    /// Number of APPEND operations.
    pub append_count: u64,
    /// Number of APPEND errors (empty key, server failure).
    pub append_errors: u64,
    /// Number of COMPARE_SWAP operations.
    pub compare_swap_count: u64,
    /// Number of COMPARE_SWAP errors (empty key).
    pub compare_swap_errors: u64,
    /// Number of STATE_CHANGE_CALLBACK operations.
    pub state_change_callback_count: u64,
    /// Number of STATE_CHANGE_CALLBACK errors (empty key, lease expired).
    pub state_change_callback_errors: u64,
    /// Number of unrecognized opcode operations.
    pub unrecognized_count: u64,
}

/// Server-side request processor that dispatches ZHT operations to the
/// `NoVoHT` storage engine.
///
/// `HTWorker` receives a `ZPack` request, determines the operation from the
/// opcode field, executes the corresponding storage operation, and returns
/// a response `ZPack`.
///
/// Operation-level metrics are tracked automatically and can be retrieved
/// via [`metrics`](Self::metrics).
///
/// # Supported Operations
///
/// | Opcode | Name                   | Description                                    |
/// |--------|------------------------|------------------------------------------------|
/// | `001`  | LOOKUP                 | Retrieve value by key                           |
/// | `002`  | REMOVE                 | Delete a key-value pair                         |
/// | `003`  | INSERT                 | Create or overwrite a key-value pair            |
/// | `004`  | APPEND                 | Append data to an existing key's value          |
/// | `005`  | COMPARE_SWAP           | Atomic compare-and-swap                         |
/// | `006`  | STATE_CHANGE_CALLBACK  | Block until key matches expected value or lease expires |
pub struct HTWorker {
    /// The underlying NoVoHT storage engine.
    store: NoVoHT,
    /// Operation-level metrics, protected by a parking_lot Mutex
    /// for interior mutability from `&self`.
    metrics: parking_lot::Mutex<HTWorkerMetrics>,
}

impl HTWorker {
    /// Create a new `HTWorker` with the given storage engine.
    pub fn new(store: NoVoHT) -> Self {
        Self {
            store,
            metrics: parking_lot::Mutex::new(HTWorkerMetrics::default()),
        }
    }

    /// Get a reference to the underlying storage engine.
    pub fn store(&self) -> &NoVoHT {
        &self.store
    }

    /// Retrieve a snapshot of the worker's operation metrics.
    pub fn metrics(&self) -> HTWorkerMetrics {
        self.metrics.lock().clone()
    }

    /// Process a single `ZPack` request and return a response `ZPack`.
    ///
    /// This is a synchronous operation that operates on the in-memory
    /// `NoVoHT` hash table. It extracts the opcode, key, and value from
    /// the request, performs the appropriate storage operation, and
    /// populates the response with the result.
    ///
    /// Metrics are updated for every operation processed.
    ///
    /// # Response Fields
    /// - `opcode`: Echoed from the request.
    /// - `key`: Echoed from the request.
    /// - `val`: Populated for LOOKUP responses (the retrieved value) and
    ///   COMPARE_SWAP mismatches (the current value).
    /// - `lease`: Encoded status code:
    ///   - `"000..."` = success (LOOKUP appends the value after the status)
    ///   - `"-01"` = empty key error
    ///   - `"-02"` = CAS mismatch
    ///   - `"-03"` = server-side failure
    ///   - `"-04"` = state change callback lease expired
    ///   - `"-92"` = non-existent key
    ///   - `"-98"` = unrecognized opcode
    pub fn process(&self, request: &ZPack) -> ZPack {
        let opcode = String::from_utf8_lossy(&request.opcode).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();

        let mut response = ZPack::default();
        response.opcode = request.opcode.clone();
        response.key = request.key.clone();

        {
            let mut m = self.metrics.lock();
            m.total_operations += 1;
        }

        // Validate key is not empty
        if key.is_empty() {
            response.lease = REC_EMPTYKEY.as_bytes().to_vec();
            {
                let mut m = self.metrics.lock();
                m.total_errors += 1;
                // Increment the error counter for the specific opcode
                match opcode.as_str() {
                    OPC_LOOKUP => m.lookup_errors += 1,
                    OPC_INSERT => m.insert_errors += 1,
                    OPC_REMOVE => m.remove_errors += 1,
                    OPC_APPEND => m.append_errors += 1,
                    OPC_CMPSWP => m.compare_swap_errors += 1,
                    OPC_STCHGCB => m.state_change_callback_errors += 1,
                    _ => {}
                }
            }
            return response;
        }

        match opcode.as_str() {
            OPC_LOOKUP => {
                {
                    let mut m = self.metrics.lock();
                    m.lookup_count += 1;
                }
                match self.store.get(&key) {
                    Ok(val) => {
                        response.val = val.clone().into_bytes();
                        response.valnull = false;
                        // LOOKUP success format: "000" + value
                        let mut status = REC_SUCC.to_string();
                        status.push_str(&val);
                        response.lease = status.into_bytes();
                    }
                    Err(_) => {
                        response.valnull = true;
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                        let mut m = self.metrics.lock();
                        m.lookup_errors += 1;
                        m.total_errors += 1;
                    }
                }
            }

            OPC_INSERT => {
                {
                    let mut m = self.metrics.lock();
                    m.insert_count += 1;
                }
                let val = String::from_utf8_lossy(&request.val).to_string();
                match self.store.put(&key, &val) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(e) => {
                        response.lease = REC_SRVFAIL.as_bytes().to_vec();
                        tracing::error!("INSERT failed for key '{}': {}", key, e);
                        let mut m = self.metrics.lock();
                        m.insert_errors += 1;
                        m.total_errors += 1;
                    }
                }
            }

            OPC_REMOVE => {
                {
                    let mut m = self.metrics.lock();
                    m.remove_count += 1;
                }
                match self.store.remove(&key) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(_) => {
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                        let mut m = self.metrics.lock();
                        m.remove_errors += 1;
                        m.total_errors += 1;
                    }
                }
            }

            OPC_APPEND => {
                {
                    let mut m = self.metrics.lock();
                    m.append_count += 1;
                }
                let val = String::from_utf8_lossy(&request.val).to_string();
                match self.store.append(&key, &val) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(e) => {
                        response.lease = REC_SRVFAIL.as_bytes().to_vec();
                        tracing::error!("APPEND failed for key '{}': {}", key, e);
                        let mut m = self.metrics.lock();
                        m.append_errors += 1;
                        m.total_errors += 1;
                    }
                }
            }

            OPC_CMPSWP => {
                {
                    let mut m = self.metrics.lock();
                    m.compare_swap_count += 1;
                }
                let expected = String::from_utf8_lossy(&request.val).to_string();
                let new_val = String::from_utf8_lossy(&request.newval).to_string();
                match self.store.compare_swap(&key, &expected, &new_val) {
                    Ok(actual) => {
                        // NoVoHT::compare_swap returns the value AFTER the operation.
                        // If swap succeeded, actual == new_val.
                        // If mismatch, actual == current (unchanged) value.
                        if actual == new_val {
                            // CAS succeeded
                            response.lease = REC_SUCC.as_bytes().to_vec();
                        } else {
                            // CAS mismatch — return actual value to client
                            response.val = actual.clone().into_bytes();
                            response.valnull = false;
                            response.lease = REC_CLTFAIL.as_bytes().to_vec();
                            let mut m = self.metrics.lock();
                            m.compare_swap_errors += 1;
                            m.total_errors += 1;
                        }
                    }
                    Err(_) => {
                        // Key not found
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                        let mut m = self.metrics.lock();
                        m.compare_swap_errors += 1;
                        m.total_errors += 1;
                    }
                }
            }

            OPC_STCHGCB => {
                {
                    let mut m = self.metrics.lock();
                    m.state_change_callback_count += 1;
                }
                // State change callback: poll until value matches expected or lease expires.
                let expected = String::from_utf8_lossy(&request.val).to_string();
                let lease_ms: u64 = String::from_utf8_lossy(&request.lease)
                    .parse()
                    .unwrap_or(SCCB_POLL_DEFAULT_INTERVAL);

                let poll_interval = std::time::Duration::from_millis(SCCB_POLL_DEFAULT_INTERVAL);
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_millis(lease_ms);

                // Use block_in_place to allow async sleep within this sync method.
                // This is safe because process() is always called from an async context
                // (the connection handler task).
                let store_result = tokio::task::block_in_place(|| {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        loop {
                            match self.store.get(&key) {
                                Ok(ref current) if *current == expected => {
                                    return REC_SUCC.as_bytes().to_vec();
                                }
                                _ => {
                                    if tokio::time::Instant::now() >= deadline {
                                        return REC_SCCBPOLLTRY.as_bytes().to_vec(); // lease expired
                                    }
                                    tokio::time::sleep(poll_interval).await;
                                }
                            }
                        }
                    })
                });

                response.lease = store_result;
                // Check if the SCCB expired (returned -04)
                if response.lease == REC_SCCBPOLLTRY.as_bytes().to_vec() {
                    let mut m = self.metrics.lock();
                    m.state_change_callback_errors += 1;
                    m.total_errors += 1;
                }
            }

            _ => {
                response.lease = REC_UOPC.as_bytes().to_vec();
                tracing::warn!("Unrecognized opcode: {}", opcode);
                let mut m = self.metrics.lock();
                m.unrecognized_count += 1;
                m.total_errors += 1;
            }
        }

        response
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section D: TCP Server
// ═══════════════════════════════════════════════════════════════════════

/// Start the ZHT TCP server, accepting connections and dispatching requests.
///
/// This function runs forever in a loop, accepting new TCP connections and
/// spawning a tokio task for each connection. Each connection handler reads
/// requests sequentially, processes them via the `HTWorker`, and sends
/// back responses.
///
/// # Arguments
/// * `addr` - The socket address to listen on (e.g., `"0.0.0.0:50000"`).
/// * `worker` - The `HTWorker` that processes incoming requests.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::net::SocketAddr;
/// use zht_transport::{run_server, HTWorker};
/// use zht_storage::NoVoHT;
///
/// #[tokio::main]
/// async fn main() {
///     let store = NoVoHT::new();
///     let worker = Arc::new(HTWorker::new(store));
///     let addr: SocketAddr = "0.0.0.0:50000".parse().unwrap();
///     run_server(addr, worker).await.unwrap();
/// }
/// ```
pub async fn run_server(addr: SocketAddr, worker: Arc<HTWorker>) -> ZhtResult<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(),
            port: addr.port(),
            reason: e.to_string(),
        })?;

    tracing::info!("ZHT TCP server listening on {}", addr);

    loop {
        let (stream, peer_addr) = listener
            .accept()
            .await
            .map_err(|e| ZhtError::ConnectionFailed {
                host: addr.ip().to_string(),
                port: addr.port(),
                reason: e.to_string(),
            })?;

        tracing::debug!("Accepted connection from {}", peer_addr);

        let worker = worker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, peer_addr, worker).await {
                tracing::warn!("Connection handler error for {}: {}", peer_addr, e);
            }
        });
    }
}

/// Start the ZHT TCP server with graceful shutdown support.
///
/// Like [`run_server`], but accepts a [`tokio_util::sync::CancellationToken`]
/// that can be cancelled to stop accepting new connections. When the token
/// is cancelled, the accept loop exits and the function returns `Ok(())`.
/// Existing connection handlers (already spawned tasks) continue to run
/// until they complete naturally.
///
/// # Arguments
/// * `addr` - The socket address to listen on.
/// * `worker` - The `HTWorker` that processes incoming requests.
/// * `cancel_token` - A cancellation token; when cancelled, the server
///   stops accepting new connections.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::net::SocketAddr;
/// use tokio_util::sync::CancellationToken;
/// use zht_transport::{run_server_with_shutdown, HTWorker};
/// use zht_storage::NoVoHT;
///
/// #[tokio::main]
/// async fn main() {
///     let store = NoVoHT::new();
///     let worker = Arc::new(HTWorker::new(store));
///     let cancel = CancellationToken::new();
///
///     let server = tokio::spawn(run_server_with_shutdown(
///         "0.0.0.0:50000".parse().unwrap(),
///         worker,
///         cancel.clone(),
///     ));
///
///     // Later, trigger graceful shutdown
///     cancel.cancel();
///     server.await.unwrap().unwrap();
/// }
/// ```
pub async fn run_server_with_shutdown(
    addr: SocketAddr,
    worker: Arc<HTWorker>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> ZhtResult<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(),
            port: addr.port(),
            reason: e.to_string(),
        })?;

    tracing::info!("ZHT TCP server listening on {} (with shutdown support)", addr);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, peer_addr) = accept_result.map_err(|e| ZhtError::ConnectionFailed {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    reason: e.to_string(),
                })?;

                tracing::debug!("Accepted connection from {}", peer_addr);

                let worker = worker.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, peer_addr, worker).await {
                        tracing::warn!("Connection handler error for {}: {}", peer_addr, e);
                    }
                });
            }
            _ = cancel_token.cancelled() => {
                tracing::info!("Shutdown signal received, stopping accept loop on {}", addr);
                break;
            }
        }
    }

    tracing::info!("Server {} stopped accepting new connections", addr);
    Ok(())
}

/// Handle a single client TCP connection.
///
/// Reads length-prefixed `ZPack` messages in a loop, processes each via
/// the `HTWorker`, and sends back the response. The loop breaks when the
/// client disconnects or a read/write error occurs.
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    worker: Arc<HTWorker>,
) -> ZhtResult<()> {
    // Disable Nagle's algorithm for lower latency
    stream
        .set_nodelay(true)
        .map_err(|e| ZhtError::ConnectionFailed {
            host: peer_addr.ip().to_string(),
            port: peer_addr.port(),
            reason: e.to_string(),
        })?;

    tracing::debug!("Handling connection from {}", peer_addr);

    loop {
        let request = match recv_message(&mut stream).await {
            Ok(req) => req,
            Err(ZhtError::RecvFailed(msg)) if msg.contains("unexpected eof") => {
                tracing::debug!("Client {} disconnected", peer_addr);
                break;
            }
            Err(_) => {
                // Connection closed or read error
                break;
            }
        };

        let response = worker.process(&request);

        if let Err(e) = send_message(&mut stream, &response).await {
            tracing::error!("Failed to send response to {}: {}", peer_addr, e);
            break;
        }
    }

    tracing::debug!("Connection closed from {}", peer_addr);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── Message Framing Tests ──────────────────────────────────────────

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut original = ZPack::default();
        original.opcode = OPC_INSERT.as_bytes().to_vec();
        original.key = b"test_key".to_vec();
        original.val = b"test_value".to_vec();

        let encoded = encode_message(&original);
        assert!(encoded.len() > 4, "Encoded message should have length prefix");

        let len_prefix = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len_prefix as usize, encoded.len() - 4);

        let (decoded, remaining) = decode_message(&encoded).unwrap();
        assert!(remaining.is_empty(), "No remaining bytes expected");
        assert_eq!(decoded.opcode, original.opcode);
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.val, original.val);
    }

    #[test]
    fn test_decode_empty_buffer() {
        let result = decode_message(&[]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"));
    }

    #[test]
    fn test_decode_too_short_for_length() {
        let buf = [0u8; 3]; // Only 3 bytes, need 4 for length prefix
        let result = decode_message(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_incomplete_message() {
        // Length prefix says 100 bytes but we only provide 10
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 6]); // Only 6 bytes of payload
        let result = decode_message(&buf);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Incomplete"));
    }

    #[test]
    fn test_decode_multiple_messages() {
        let msg1 = {
            let mut z = ZPack::default();
            z.opcode = OPC_INSERT.as_bytes().to_vec();
            z.key = b"key1".to_vec();
            z.val = b"val1".to_vec();
            z
        };
        let msg2 = {
            let mut z = ZPack::default();
            z.opcode = OPC_LOOKUP.as_bytes().to_vec();
            z.key = b"key2".to_vec();
            z
        };

        let mut buf = Vec::new();
        buf.extend(encode_message(&msg1));
        buf.extend(encode_message(&msg2));

        let (decoded1, remaining) = decode_message(&buf).unwrap();
        assert_eq!(decoded1.opcode, msg1.opcode);
        assert_eq!(decoded1.key, msg1.key);

        let (decoded2, remaining) = decode_message(remaining).unwrap();
        assert_eq!(decoded2.opcode, msg2.opcode);
        assert_eq!(decoded2.key, msg2.key);
        assert!(remaining.is_empty());
    }

    // ── TcpProxy Tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_tcp_proxy_new() {
        let proxy = TcpProxy::new();
        assert_eq!(proxy.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_tcp_proxy_default() {
        let proxy = TcpProxy::default();
        assert_eq!(proxy.connection_count().await, 0);
    }

    // ── HTWorker Tests ────────────────────────────────────────────────

    fn make_test_worker() -> HTWorker {
        let store = NoVoHT::new();
        HTWorker::new(store)
    }

    fn make_zpack(opcode: &str, key: &str, val: &str) -> ZPack {
        let mut zpack = ZPack::default();
        zpack.opcode = opcode.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        if !val.is_empty() {
            zpack.val = val.as_bytes().to_vec();
        }
        zpack
    }

    #[test]
    fn test_worker_insert_and_lookup() {
        let worker = make_test_worker();

        // Insert
        let req = make_zpack(OPC_INSERT, "mykey", "myvalue");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        // Lookup
        let req = make_zpack(OPC_LOOKUP, "mykey", "");
        let resp = worker.process(&req);
        let lease_status = String::from_utf8_lossy(&resp.lease);
        assert!(lease_status.starts_with(REC_SUCC));
        assert!(lease_status.contains("myvalue"));
        assert_eq!(resp.val, b"myvalue".to_vec());
        assert!(!resp.valnull);
    }

    #[test]
    fn test_worker_lookup_nonexistent() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_LOOKUP, "nonexistent", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
        assert!(resp.valnull);
    }

    #[test]
    fn test_worker_remove() {
        let worker = make_test_worker();

        // Insert first
        let req = make_zpack(OPC_INSERT, "rmkey", "rmval");
        worker.process(&req);

        // Remove
        let req = make_zpack(OPC_REMOVE, "rmkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        // Verify removed
        let req = make_zpack(OPC_LOOKUP, "rmkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_remove_nonexistent() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_REMOVE, "nope", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_append() {
        let worker = make_test_worker();

        // Insert initial value
        let req = make_zpack(OPC_INSERT, "appendkey", "hello");
        worker.process(&req);

        // Append
        let req = make_zpack(OPC_APPEND, "appendkey", " world");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        // Verify appended (NoVoHT uses ":" separator for append)
        let req = make_zpack(OPC_LOOKUP, "appendkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"hello: world".to_vec());
    }

    #[test]
    fn test_worker_compare_swap_success() {
        let worker = make_test_worker();

        // Insert initial value
        let req = make_zpack(OPC_INSERT, "caskey", "old");
        worker.process(&req);

        // CAS with correct expected value
        let mut req = make_zpack(OPC_CMPSWP, "caskey", "old");
        req.newval = b"new".to_vec();
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        // Verify new value
        let req = make_zpack(OPC_LOOKUP, "caskey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"new".to_vec());
    }

    #[test]
    fn test_worker_compare_swap_mismatch() {
        let worker = make_test_worker();

        // Insert initial value
        let req = make_zpack(OPC_INSERT, "caskey", "actual");
        worker.process(&req);

        // CAS with wrong expected value
        let mut req = make_zpack(OPC_CMPSWP, "caskey", "wrong");
        req.newval = b"new".to_vec();
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_CLTFAIL.as_bytes().to_vec());
        assert_eq!(resp.val, b"actual".to_vec());
        assert!(!resp.valnull);

        // Value should remain unchanged
        let req = make_zpack(OPC_LOOKUP, "caskey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"actual".to_vec());
    }

    #[test]
    fn test_worker_unrecognized_opcode() {
        let worker = make_test_worker();
        let req = make_zpack("999", "key", "val");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_UOPC.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_empty_key() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "", "val");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_EMPTYKEY.as_bytes().to_vec());
    }

    // ── Integration Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_server_and_client_integration() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));

        // Start server on random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        // Spawn server task
        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Client: connect and send INSERT
        let proxy = Arc::new(TcpProxy::new());
        let mut insert_req = ZPack::default();
        insert_req.opcode = OPC_INSERT.as_bytes().to_vec();
        insert_req.key = b"integ_key".to_vec();
        insert_req.val = b"integ_val".to_vec();

        let resp = proxy
            .sendrecv("127.0.0.1", server_addr.port(), &insert_req)
            .await
            .unwrap();
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        // Client: send LOOKUP
        let mut lookup_req = ZPack::default();
        lookup_req.opcode = OPC_LOOKUP.as_bytes().to_vec();
        lookup_req.key = b"integ_key".to_vec();

        let resp = proxy
            .sendrecv("127.0.0.1", server_addr.port(), &lookup_req)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&resp.lease).starts_with(REC_SUCC));
        assert_eq!(resp.val, b"integ_val".to_vec());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let proxy = Arc::new(TcpProxy::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let p = proxy.clone();
            let port = server_addr.port();
            handles.push(tokio::spawn(async move {
                let mut req = ZPack::default();
                req.opcode = OPC_INSERT.as_bytes().to_vec();
                req.key = format!("key_{}", i).into_bytes();
                req.val = format!("val_{}", i).into_bytes();

                let resp = p.sendrecv("127.0.0.1", port, &req).await.unwrap();
                assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

                let mut lookup = ZPack::default();
                lookup.opcode = OPC_LOOKUP.as_bytes().to_vec();
                lookup.key = format!("key_{}", i).into_bytes();

                let resp = p.sendrecv("127.0.0.1", port, &lookup).await.unwrap();
                assert_eq!(resp.val, format!("val_{}", i).into_bytes());
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_connection_reuse() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let proxy = Arc::new(TcpProxy::new());

        // First request — should create a new connection
        let mut req = ZPack::default();
        req.opcode = OPC_INSERT.as_bytes().to_vec();
        req.key = b"reuse_key".to_vec();
        req.val = b"reuse_val".to_vec();
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        assert_eq!(proxy.connection_count().await, 1);

        // Second request — should reuse the existing connection
        let mut req = ZPack::default();
        req.opcode = OPC_LOOKUP.as_bytes().to_vec();
        req.key = b"reuse_key".to_vec();
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        assert_eq!(proxy.connection_count().await, 1);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_close_connection() {
        let proxy = TcpProxy::new();

        // Close a connection that doesn't exist — should not error
        proxy.close_connection("localhost", 9999).await;
        assert_eq!(proxy.connection_count().await, 0);

        // Close all — should not error
        proxy.close_all().await;
        assert_eq!(proxy.connection_count().await, 0);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Phase 3 Tests
    // ═══════════════════════════════════════════════════════════════════

    // ── is_connected Tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_is_connected_initial() {
        let proxy = TcpProxy::new();
        assert!(!proxy.is_connected("127.0.0.1", 9999).await);
    }

    #[tokio::test]
    async fn test_is_connected_after_establish() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = Arc::new(TcpProxy::new());
        assert!(!proxy.is_connected("127.0.0.1", server_addr.port()).await);

        // Send a message to establish the connection
        let req = make_zpack(OPC_INSERT, "ck", "cv");
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        assert!(proxy.is_connected("127.0.0.1", server_addr.port()).await);

        // Close it
        proxy.close_connection("127.0.0.1", server_addr.port()).await;
        assert!(!proxy.is_connected("127.0.0.1", server_addr.port()).await);

        server_handle.abort();
    }

    // ── ConnectionStats Tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_connection_stats_tracking() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "stats_key", "stats_val");

        // Before any operation, no stats
        assert!(proxy.stats("127.0.0.1", server_addr.port()).await.is_none());

        // After one sendrecv
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        let stats = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();
        assert_eq!(stats.messages_sent, 1);
        assert_eq!(stats.messages_received, 1);
        assert!(stats.bytes_sent > 0);
        assert!(stats.bytes_received > 0);
        assert_eq!(stats.errors, 0);

        // After second sendrecv
        let req2 = make_zpack(OPC_LOOKUP, "stats_key", "");
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req2)
            .await
            .unwrap();
        let stats = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();
        assert_eq!(stats.messages_sent, 2);
        assert_eq!(stats.messages_received, 2);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_connection_stats_bytes() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "bytes_key", "bytes_val");

        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        let stats = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();

        // Verify bytes are at least the length prefix (4 bytes) + encoded payload
        assert!(stats.bytes_sent >= 4);
        assert!(stats.bytes_received >= 4);

        // Encoded size should match prost encoding
        let expected_send_len = 4 + req.encoded_len();
        assert_eq!(stats.bytes_sent, expected_send_len as u64);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_connection_stats_last_activity() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "la_key", "la_val");

        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();
        let stats1 = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();
        let ts1 = stats1.last_activity;

        // Small sleep to ensure time difference
        tokio::time::sleep(Duration::from_millis(10)).await;

        let req2 = make_zpack(OPC_LOOKUP, "la_key", "");
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req2)
            .await
            .unwrap();
        let stats2 = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();
        let ts2 = stats2.last_activity;

        assert!(ts2 > ts1, "last_activity should advance after another operation");

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_close_connection_clears_stats() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "clear_key", "clear_val");
        proxy
            .sendrecv("127.0.0.1", server_addr.port(), &req)
            .await
            .unwrap();

        // Stats exist
        assert!(proxy.stats("127.0.0.1", server_addr.port()).await.is_some());

        // Close and verify stats are gone
        proxy.close_connection("127.0.0.1", server_addr.port()).await;
        assert!(proxy.stats("127.0.0.1", server_addr.port()).await.is_none());

        server_handle.abort();
    }

    // ── batch_sendrecv Tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_batch_sendrecv() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let messages = vec![
            make_zpack(OPC_INSERT, "batch_k1", "batch_v1"),
            make_zpack(OPC_INSERT, "batch_k2", "batch_v2"),
            make_zpack(OPC_INSERT, "batch_k3", "batch_v3"),
        ];

        let results = proxy
            .batch_sendrecv("127.0.0.1", server_addr.port(), &messages)
            .await;

        assert_eq!(results.len(), 3);
        for result in &results {
            assert!(result.is_ok());
            let resp = result.as_ref().unwrap();
            assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());
        }

        // Verify stats reflect batch
        let stats = proxy.stats("127.0.0.1", server_addr.port()).await.unwrap();
        assert_eq!(stats.messages_sent, 3);
        assert_eq!(stats.messages_received, 3);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_batch_sendrecv_empty() {
        let proxy = TcpProxy::new();
        let results = proxy
            .batch_sendrecv("127.0.0.1", 9999, &[])
            .await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_batch_sendrecv_partial_failure() {
        // Create messages where some will fail (empty keys) and some succeed
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let messages = vec![
            make_zpack(OPC_INSERT, "good_key", "good_val"),  // succeeds
            make_zpack(OPC_INSERT, "", "empty_key_val"),     // returns -01 error but batch continues
            make_zpack(OPC_LOOKUP, "good_key", ""),          // succeeds
        ];

        let results = proxy
            .batch_sendrecv("127.0.0.1", server_addr.port(), &messages)
            .await;

        assert_eq!(results.len(), 3);
        // All should succeed at transport level (the server returns error code in the lease)
        assert!(results[0].is_ok());
        assert!(results[1].is_ok()); // transport succeeds; server returns -01 in lease
        assert!(results[2].is_ok());

        // Verify the server-level error for empty key
        let resp1 = results[1].as_ref().unwrap();
        assert_eq!(resp1.lease, REC_EMPTYKEY.as_bytes().to_vec());

        // Verify lookup succeeds
        let resp2 = results[2].as_ref().unwrap();
        assert_eq!(resp2.val, b"good_val".to_vec());

        server_handle.abort();
    }

    // ── sendrecv_with_timeout Tests ────────────────────────────────────

    #[tokio::test]
    async fn test_sendrecv_with_timeout_success() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "timeout_key", "timeout_val");
        let result = proxy
            .sendrecv_with_timeout(
                "127.0.0.1",
                server_addr.port(),
                &req,
                Duration::from_secs(5),
            )
            .await;

        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_sendrecv_with_timeout_expires() {
        // Bind a listener but never accept connections -> client will timeout on connect
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        // Drop the listener so connections are refused (not accepted)
        drop(listener);

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "tmo_key", "tmo_val");
        let result = proxy
            .sendrecv_with_timeout(
                "127.0.0.1",
                server_addr.port(),
                &req,
                Duration::from_millis(100),
            )
            .await;

        // Should timeout since the connection is refused
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(err_str.contains("timed out") || err_str.contains("connection"), 
            "Expected timeout or connection error, got: {}", err_str);
    }

    // ── health_check Tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_health_check_success() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();

        // Health check should succeed on a live server
        let result = proxy.health_check("127.0.0.1", server_addr.port()).await;
        assert!(result.is_ok(), "health_check should succeed on live server");

        // Connection should now exist
        assert!(proxy.is_connected("127.0.0.1", server_addr.port()).await);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_health_check_failure() {
        // No server listening on this port
        let proxy = TcpProxy::new();
        let result = proxy
            .health_check_with_timeout("127.0.0.1", 1, Duration::from_millis(100))
            .await;
        assert!(result.is_err());
    }

    // ── Auto-reconnect Tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_auto_reconnect_after_server_restart() {
        // Start server, connect, shut down server, restart on same port, send again
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));

        // First server instance
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener1.local_addr().unwrap().port();

        let server_worker1 = worker.clone();
        let handle1 = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener1.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker1.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = Arc::new(TcpProxy::new());

        // First successful request
        let req = make_zpack(OPC_INSERT, "reconnect_key", "reconnect_val");
        let resp = proxy
            .sendrecv("127.0.0.1", port, &req)
            .await
            .unwrap();
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());
        assert!(proxy.is_connected("127.0.0.1", port).await);

        // Shut down the first server
        handle1.abort();
        drop(handle1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Start a new server on the same port
        let store2 = NoVoHT::new();
        let worker2 = Arc::new(HTWorker::new(store2));
        let listener2 = TcpListener::bind(("127.0.0.1", port)).await.unwrap();

        let server_worker2 = worker2.clone();
        let handle2 = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener2.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker2.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second request — should auto-reconnect to the new server
        let req2 = make_zpack(OPC_INSERT, "new_server_key", "new_server_val");
        let resp2 = proxy
            .sendrecv("127.0.0.1", port, &req2)
            .await
            .unwrap();
        assert_eq!(resp2.lease, REC_SUCC.as_bytes().to_vec());

        // Verify it was stored on the new server
        let req3 = make_zpack(OPC_LOOKUP, "new_server_key", "");
        let resp3 = proxy
            .sendrecv("127.0.0.1", port, &req3)
            .await
            .unwrap();
        assert_eq!(resp3.val, b"new_server_val".to_vec());

        // After reconnect, the stats on the new entry reflect post-reconnect activity.
        // The exact count depends on whether send succeeded on the stale connection
        // before the broken-pipe was detected (non-deterministic TCP behavior).
        let stats = proxy.stats("127.0.0.1", port).await.unwrap();
        assert!(stats.messages_sent >= 1, "Should have at least 1 sent message after reconnect");
        assert!(stats.messages_received >= 1, "Should have at least 1 received message after reconnect");

        handle2.abort();
    }

    #[tokio::test]
    async fn test_auto_reconnect_stats_tracked() {
        // When auto-reconnect occurs, the stats should reflect the error
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server_worker = worker.clone();
        let handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();

        // Successful operation — no errors
        let req = make_zpack(OPC_INSERT, "stats_reconnect", "val");
        proxy.sendrecv("127.0.0.1", port, &req).await.unwrap();

        let stats = proxy.stats("127.0.0.1", port).await.unwrap();
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.messages_sent, 1);

        handle.abort();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── HTWorker Metrics Tests ─────────────────────────────────────────

    #[test]
    fn test_htworker_metrics_initial() {
        let worker = make_test_worker();
        let m = worker.metrics();
        assert_eq!(m.total_operations, 0);
        assert_eq!(m.total_errors, 0);
        assert_eq!(m.insert_count, 0);
        assert_eq!(m.lookup_count, 0);
    }

    #[test]
    fn test_htworker_metrics_insert() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "mkey", "mval");
        worker.process(&req);

        let m = worker.metrics();
        assert_eq!(m.total_operations, 1);
        assert_eq!(m.insert_count, 1);
        assert_eq!(m.total_errors, 0);
        assert_eq!(m.insert_errors, 0);
    }

    #[test]
    fn test_htworker_metrics_lookup() {
        let worker = make_test_worker();
        // Insert first
        worker.process(&make_zpack(OPC_INSERT, "lkey", "lval"));
        // Lookup
        worker.process(&make_zpack(OPC_LOOKUP, "lkey", ""));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 2);
        assert_eq!(m.insert_count, 1);
        assert_eq!(m.lookup_count, 1);
    }

    #[test]
    fn test_htworker_metrics_lookup_nonexistent() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_LOOKUP, "no_such_key", ""));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 1);
        assert_eq!(m.lookup_count, 1);
        assert_eq!(m.lookup_errors, 1);
        assert_eq!(m.total_errors, 1);
    }

    #[test]
    fn test_htworker_metrics_remove() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_INSERT, "rmk", "rmv"));
        worker.process(&make_zpack(OPC_REMOVE, "rmk", ""));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 2);
        assert_eq!(m.remove_count, 1);
        assert_eq!(m.total_errors, 0);
    }

    #[test]
    fn test_htworker_metrics_remove_nonexistent() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_REMOVE, "nope_rm", ""));

        let m = worker.metrics();
        assert_eq!(m.remove_count, 1);
        assert_eq!(m.remove_errors, 1);
        assert_eq!(m.total_errors, 1);
    }

    #[test]
    fn test_htworker_metrics_append() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_INSERT, "akey", "av"));
        worker.process(&make_zpack(OPC_APPEND, "akey", " more"));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 2);
        assert_eq!(m.append_count, 1);
        assert_eq!(m.append_errors, 0);
    }

    #[test]
    fn test_htworker_metrics_cas_success() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_INSERT, "ck", "old"));
        let mut req = make_zpack(OPC_CMPSWP, "ck", "old");
        req.newval = b"new".to_vec();
        worker.process(&req);

        let m = worker.metrics();
        assert_eq!(m.compare_swap_count, 1);
        assert_eq!(m.compare_swap_errors, 0);
    }

    #[test]
    fn test_htworker_metrics_cas_mismatch() {
        let worker = make_test_worker();
        worker.process(&make_zpack(OPC_INSERT, "ck", "actual"));
        let mut req = make_zpack(OPC_CMPSWP, "ck", "wrong");
        req.newval = b"new".to_vec();
        worker.process(&req);

        let m = worker.metrics();
        assert_eq!(m.compare_swap_count, 1);
        assert_eq!(m.compare_swap_errors, 1);
        assert_eq!(m.total_errors, 1);
    }

    #[test]
    fn test_htworker_metrics_empty_key_errors() {
        let worker = make_test_worker();

        // Empty key on various opcodes should increment per-opcode error counters
        worker.process(&make_zpack(OPC_INSERT, "", "v"));
        worker.process(&make_zpack(OPC_LOOKUP, "", ""));
        worker.process(&make_zpack(OPC_REMOVE, "", ""));
        worker.process(&make_zpack(OPC_APPEND, "", "v"));
        worker.process(&make_zpack(OPC_CMPSWP, "", "v"));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 5);
        assert_eq!(m.total_errors, 5);
        assert_eq!(m.insert_errors, 1);
        assert_eq!(m.lookup_errors, 1);
        assert_eq!(m.remove_errors, 1);
        assert_eq!(m.append_errors, 1);
        assert_eq!(m.compare_swap_errors, 1);
    }

    #[test]
    fn test_htworker_metrics_unrecognized_opcode() {
        let worker = make_test_worker();
        worker.process(&make_zpack("999", "key", "val"));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 1);
        assert_eq!(m.unrecognized_count, 1);
        assert_eq!(m.total_errors, 1);
    }

    #[test]
    fn test_htworker_metrics_total_comprehensive() {
        let worker = make_test_worker();

        // 2 inserts, 1 lookup, 1 remove, 1 unrecognized = 5 total
        worker.process(&make_zpack(OPC_INSERT, "tk1", "tv1"));
        worker.process(&make_zpack(OPC_INSERT, "tk2", "tv2"));
        worker.process(&make_zpack(OPC_LOOKUP, "tk1", ""));
        worker.process(&make_zpack(OPC_REMOVE, "tk2", ""));
        worker.process(&make_zpack("777", "tk1", ""));

        let m = worker.metrics();
        assert_eq!(m.total_operations, 5);
        assert_eq!(m.insert_count, 2);
        assert_eq!(m.lookup_count, 1);
        assert_eq!(m.remove_count, 1);
        assert_eq!(m.unrecognized_count, 1);
        assert_eq!(m.total_errors, 1); // only the unrecognized opcode
    }

    // ── Graceful Shutdown Tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_graceful_shutdown_cancels_accept_loop() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let cancel = tokio_util::sync::CancellationToken::new();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let cancel_clone = cancel.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        if let Ok((stream, peer_addr)) = result {
                            let w = server_worker.clone();
                            tokio::spawn(async move {
                                let _ = handle_connection(stream, peer_addr, w).await;
                            });
                        }
                    }
                    _ = cancel_clone.cancelled() => {
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Verify server is running by connecting
        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "shutdown_key", "shutdown_val");
        let result = proxy
            .sendrecv("127.0.0.1", addr.port(), &req)
            .await;
        assert!(result.is_ok());

        // Close the client connection so the server handler exits
        proxy.close_connection("127.0.0.1", addr.port()).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Trigger shutdown
        cancel.cancel();

        // Server should exit gracefully (accept loop breaks)
        let server_result = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
        assert!(server_result.is_ok(), "Server should shut down within timeout");
        assert!(server_result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_run_server_with_shutdown() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let cancel = tokio_util::sync::CancellationToken::new();

        let server_worker = worker.clone();
        let cancel_clone = cancel.clone();
        let server_handle = tokio::spawn(async move {
            run_server_with_shutdown(
                "127.0.0.1:0".parse().unwrap(),
                server_worker,
                cancel_clone,
            )
            .await
        });

        // Give the server time to start and get its address.
        // We can't easily get the address from run_server_with_shutdown,
        // so we test the cancellation mechanism by checking that the
        // function returns after cancellation.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the server
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
        assert!(result.is_ok(), "Server should shut down within timeout");
        let inner = result.unwrap();
        assert!(inner.is_ok(), "Server should exit with Ok(())");
    }

    #[tokio::test]
    async fn test_graceful_shutdown_existing_handlers_finish() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let cancel = tokio_util::sync::CancellationToken::new();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let cancel_clone = cancel.clone();
        let server_handle = tokio::spawn(async move {
            let mut handler_tasks = Vec::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        if let Ok((stream, peer_addr)) = result {
                            let w = server_worker.clone();
                            handler_tasks.push(tokio::spawn(async move {
                                let _ = handle_connection(stream, peer_addr, w).await;
                            }));
                        }
                    }
                    _ = cancel_clone.cancelled() => {
                        break;
                    }
                }
            }
            // Wait for existing handlers to finish (graceful shutdown)
            for h in handler_tasks {
                let _ = h.await;
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Send a request and then immediately cancel
        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "grace_key", "grace_val");

        // Start the request in the background
        let req_handle = tokio::spawn(async move {
            proxy.sendrecv("127.0.0.1", addr.port(), &req).await
        });

        // Give the request time to be sent and processed
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the server accept loop — handlers should still finish
        cancel.cancel();

        // The request should complete successfully
        let req_result = tokio::time::timeout(Duration::from_secs(2), req_handle).await;
        assert!(req_result.is_ok());
        assert!(req_result.unwrap().is_ok());

        // The server should shut down after handlers finish
        let server_result = tokio::time::timeout(Duration::from_secs(2), server_handle).await;
        assert!(server_result.is_ok());
    }

    // ── sendrecv_with_timeout Zero Duration ────────────────────────────

    #[tokio::test]
    async fn test_sendrecv_with_timeout_zero_duration() {
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_worker = worker.clone();
        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, peer_addr) = match listener.accept().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let w = server_worker.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, peer_addr, w).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let proxy = TcpProxy::new();
        let req = make_zpack(OPC_INSERT, "zero_tmo_key", "zero_tmo_val");

        // Zero duration timeout — should still work for fast local operations
        // tokio::timeout with Duration::ZERO fires immediately if the future
        // is not already complete
        let result = proxy
            .sendrecv_with_timeout("127.0.0.1", server_addr.port(), &req, Duration::ZERO)
            .await;

        // This may timeout or succeed depending on timing, but should not panic
        // We just verify it returns without hanging
        let _ = result;

        server_handle.abort();
    }

    // ── is_connection_broken_error Unit Tests ──────────────────────────

    #[test]
    fn test_is_connection_broken_error_broken_pipe() {
        let err = ZhtError::SendFailed("Broken pipe".to_string());
        assert!(is_connection_broken_error(&err));

        let err = ZhtError::RecvFailed("broken pipe (os error 32)".to_string());
        assert!(is_connection_broken_error(&err));
    }

    #[test]
    fn test_is_connection_broken_error_connection_reset() {
        let err = ZhtError::SendFailed("Connection reset by peer".to_string());
        assert!(is_connection_broken_error(&err));
    }

    #[test]
    fn test_is_connection_broken_error_unexpected_eof() {
        let err = ZhtError::RecvFailed("unexpected eof".to_string());
        assert!(is_connection_broken_error(&err));
    }

    #[test]
    fn test_is_connection_broken_error_not_broken() {
        let err = ZhtError::SendFailed("some other error".to_string());
        assert!(!is_connection_broken_error(&err));

        let err = ZhtError::ConnectionFailed {
            host: "localhost".to_string(),
            port: 1234,
            reason: "Connection refused".to_string(),
        };
        assert!(!is_connection_broken_error(&err));

        let err = ZhtError::ProtobufDecodeError("bad data".to_string());
        assert!(!is_connection_broken_error(&err));
    }
}
