//! Transport metrics for monitoring and observability.
//!
//! All counters use `std::sync::atomic::AtomicU64` for lock-free, thread-safe
//! increments. Metrics are aggregated across all connections managed by a
//! transport instance.
//!
//! # Usage
//!
//! ```rust
//! use zht_transport::metrics::TransportMetrics;
//!
//! let metrics = TransportMetrics::new();
//! metrics.inc_messages_sent();
//! metrics.add_bytes_sent(1024);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Lock-free counters for transport-level metrics.
///
/// Each field is an `AtomicU64` that can be incremented from any thread
/// without holding a lock. Use `snapshot()` to get a consistent point-in-time
/// copy of all counters.
#[derive(Debug)]
pub struct TransportMetrics {
    /// Total number of messages sent (requests).
    messages_sent: AtomicU64,
    /// Total number of messages received (responses).
    messages_received: AtomicU64,
    /// Total bytes sent on the wire (including framing overhead).
    bytes_sent: AtomicU64,
    /// Total bytes received on the wire (including framing overhead).
    bytes_received: AtomicU64,
    /// Number of active connections currently in the pool.
    active_connections: AtomicU64,
    /// Number of new connections established.
    connections_opened: AtomicU64,
    /// Number of connections closed (either explicitly or due to error).
    connections_closed: AtomicU64,
    /// Number of send failures.
    send_errors: AtomicU64,
    /// Number of receive failures.
    recv_errors: AtomicU64,
    /// Number of reconnection attempts (stale connections replaced).
    reconnections: AtomicU64,
    /// Number of server-side connections accepted.
    connections_accepted: AtomicU64,
    /// Number of requests processed by the server handler.
    requests_processed: AtomicU64,
    /// Number of unrecognized opcodes received.
    unknown_opcodes: AtomicU64,
    /// Total number of chunks sent (big data transfer).
    chunks_sent: AtomicU64,
    /// Total number of chunks received (big data transfer).
    chunks_received: AtomicU64,
    /// Monotonic timestamp of when these metrics were created.
    created_at: Instant,
}

impl TransportMetrics {
    /// Create a new zeroed metrics instance.
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            connections_opened: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            recv_errors: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            requests_processed: AtomicU64::new(0),
            unknown_opcodes: AtomicU64::new(0),
            chunks_sent: AtomicU64::new(0),
            chunks_received: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    // ── Client-side counters ───────────────────────────────────────────

    /// Increment the message-sent counter by 1.
    pub fn inc_messages_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the message-received counter by 1.
    pub fn inc_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n` bytes to the bytes-sent counter.
    pub fn add_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    /// Add `n` bytes to the bytes-received counter.
    pub fn add_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    /// Increment the send-error counter by 1.
    pub fn inc_send_errors(&self) {
        self.send_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the recv-error counter by 1.
    pub fn inc_recv_errors(&self) {
        self.recv_errors.fetch_add(1, Ordering::Relaxed);
    }

    // ── Connection counters ────────────────────────────────────────────

    /// Increment the active-connection count (also opens counter).
    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.connections_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active-connection count (also closes counter).
    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the reconnection counter by 1.
    pub fn inc_reconnections(&self) {
        self.reconnections.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the active connection count directly (used on close_all).
    pub fn set_active_connections(&self, n: u64) {
        self.active_connections.store(n, Ordering::Relaxed);
    }

    // ── Server-side counters ───────────────────────────────────────────

    /// Increment the connections-accepted counter by 1.
    pub fn inc_connections_accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the requests-processed counter by 1.
    pub fn inc_requests_processed(&self) {
        self.requests_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the unknown-opcode counter by 1.
    pub fn inc_unknown_opcodes(&self) {
        self.unknown_opcodes.fetch_add(1, Ordering::Relaxed);
    }

    // ── Chunking counters ──────────────────────────────────────────────

    /// Increment the chunks-sent counter by 1.
    pub fn inc_chunks_sent(&self) {
        self.chunks_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the chunks-received counter by 1.
    pub fn inc_chunks_received(&self) {
        self.chunks_received.fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot ───────────────────────────────────────────────────────

    /// Take a consistent point-in-time snapshot of all counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            connections_opened: self.connections_opened.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            recv_errors: self.recv_errors.load(Ordering::Relaxed),
            reconnections: self.reconnections.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            requests_processed: self.requests_processed.load(Ordering::Relaxed),
            unknown_opcodes: self.unknown_opcodes.load(Ordering::Relaxed),
            chunks_sent: self.chunks_sent.load(Ordering::Relaxed),
            chunks_received: self.chunks_received.load(Ordering::Relaxed),
            elapsed_secs: self.created_at.elapsed().as_secs_f64(),
        }
    }

    /// Reset all counters to zero (except `created_at`, which is updated).
    pub fn reset(&self) {
        self.messages_sent.store(0, Ordering::Relaxed);
        self.messages_received.store(0, Ordering::Relaxed);
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.active_connections.store(0, Ordering::Relaxed);
        self.connections_opened.store(0, Ordering::Relaxed);
        self.connections_closed.store(0, Ordering::Relaxed);
        self.send_errors.store(0, Ordering::Relaxed);
        self.recv_errors.store(0, Ordering::Relaxed);
        self.reconnections.store(0, Ordering::Relaxed);
        self.connections_accepted.store(0, Ordering::Relaxed);
        self.requests_processed.store(0, Ordering::Relaxed);
        self.unknown_opcodes.store(0, Ordering::Relaxed);
        self.chunks_sent.store(0, Ordering::Relaxed);
        self.chunks_received.store(0, Ordering::Relaxed);
    }
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// An immutable snapshot of transport metrics at a point in time.
///
/// Useful for logging, reporting, and external monitoring systems that
/// need a stable set of values.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub active_connections: u64,
    pub connections_opened: u64,
    pub connections_closed: u64,
    pub send_errors: u64,
    pub recv_errors: u64,
    pub reconnections: u64,
    pub connections_accepted: u64,
    pub requests_processed: u64,
    pub unknown_opcodes: u64,
    pub chunks_sent: u64,
    pub chunks_received: u64,
    pub elapsed_secs: f64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Transport Metrics (uptime: {:.1}s)", self.elapsed_secs)?;
        writeln!(f, "  Messages: {} sent / {} received", self.messages_sent, self.messages_received)?;
        writeln!(f, "  Bytes: {} sent / {} received", self.bytes_sent, self.bytes_received)?;
        writeln!(f, "  Connections: {} active / {} opened / {} closed", self.active_connections, self.connections_opened, self.connections_closed)?;
        writeln!(f, "  Errors: {} send / {} recv", self.send_errors, self.recv_errors)?;
        if self.reconnections > 0 {
            writeln!(f, "  Reconnections: {}", self.reconnections)?;
        }
        if self.connections_accepted > 0 {
            writeln!(f, "  Server accepted: {} connections / {} requests processed", self.connections_accepted, self.requests_processed)?;
        }
        if self.chunks_sent > 0 || self.chunks_received > 0 {
            writeln!(f, "  Chunks: {} sent / {} received", self.chunks_sent, self.chunks_received)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initial_state() {
        let m = TransportMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.messages_sent, 0);
        assert_eq!(s.active_connections, 0);
        assert!(s.elapsed_secs >= 0.0);
    }

    #[test]
    fn test_metrics_increment() {
        let m = TransportMetrics::new();
        m.inc_messages_sent();
        m.inc_messages_sent();
        m.inc_messages_received();
        m.add_bytes_sent(1024);
        m.add_bytes_received(512);
        m.inc_active_connections();
        m.inc_connections_accepted();
        m.inc_requests_processed();
        m.inc_chunks_sent();
        m.inc_chunks_received();
        m.inc_send_errors();
        m.inc_recv_errors();
        m.inc_reconnections();
        m.inc_unknown_opcodes();

        let s = m.snapshot();
        assert_eq!(s.messages_sent, 2);
        assert_eq!(s.messages_received, 1);
        assert_eq!(s.bytes_sent, 1024);
        assert_eq!(s.bytes_received, 512);
        assert_eq!(s.active_connections, 1);
        assert_eq!(s.connections_opened, 1);
        assert_eq!(s.connections_accepted, 1);
        assert_eq!(s.requests_processed, 1);
        assert_eq!(s.chunks_sent, 1);
        assert_eq!(s.chunks_received, 1);
        assert_eq!(s.send_errors, 1);
        assert_eq!(s.recv_errors, 1);
        assert_eq!(s.reconnections, 1);
        assert_eq!(s.unknown_opcodes, 1);
    }

    #[test]
    fn test_metrics_connection_lifecycle() {
        let m = TransportMetrics::new();
        m.inc_active_connections();
        m.inc_active_connections();
        assert_eq!(m.snapshot().active_connections, 2);
        assert_eq!(m.snapshot().connections_opened, 2);

        m.dec_active_connections();
        assert_eq!(m.snapshot().active_connections, 1);
        assert_eq!(m.snapshot().connections_closed, 1);

        m.set_active_connections(0);
        assert_eq!(m.snapshot().active_connections, 0);
    }

    #[test]
    fn test_metrics_reset() {
        let m = TransportMetrics::new();
        m.inc_messages_sent();
        m.add_bytes_sent(999);
        m.reset();
        let s = m.snapshot();
        assert_eq!(s.messages_sent, 0);
        assert_eq!(s.bytes_sent, 0);
    }

    #[test]
    fn test_metrics_display() {
        let m = TransportMetrics::new();
        m.inc_messages_sent();
        m.inc_active_connections();
        let s = m.snapshot();
        let text = format!("{}", s);
        assert!(text.contains("Messages: 1 sent"));
        assert!(text.contains("Connections: 1 active"));
    }

    #[test]
    fn test_metrics_default() {
        let m = TransportMetrics::default();
        assert_eq!(m.snapshot().messages_sent, 0);
    }
}
