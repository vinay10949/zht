//! Error types for ZHT.
//!
//! Provides a typed error enum via `thiserror` and a convenience `ZhtResult`
//! alias that uses `anyhow` for ergonomic error propagation in application code.

use std::path::PathBuf;
use thiserror::Error;

/// The primary error type for ZHT operations.
#[derive(Debug, Error)]
pub enum ZhtError {
    // ── Configuration errors ──────────────────────────────────────────
    #[error("configuration file not found: {path}")]
    ConfigNotFound { path: PathBuf },

    #[error("failed to parse configuration file: {path}: {reason}")]
    ConfigParseError { path: PathBuf, reason: String },

    #[error("missing required configuration parameter: {param}")]
    MissingConfigParam { param: String },

    #[error("invalid configuration value for '{param}': {reason}")]
    InvalidConfigValue { param: String, reason: String },

    // ── Network / Transport errors ────────────────────────────────────
    #[error("connection failed to {host}:{port}: {reason}")]
    ConnectionFailed { host: String, port: u16, reason: String },

    #[error("send failed: {0}")]
    SendFailed(String),

    #[error("receive failed: {0}")]
    RecvFailed(String),

    #[error("unsupported protocol: {0}")]
    UnsupportedProtocol(String),

    // ── DHT Operation errors ──────────────────────────────────────────
    #[error("empty key in operation")]
    EmptyKey,

    #[error("key not found: {0}")]
    KeyNotFound(String),

    #[error("compare-and-swap mismatch: expected '{expected}', got '{actual}'")]
    CasMismatch { expected: String, actual: String },

    #[error("state change callback lease expired after {lease_ms}ms")]
    LeaseExpired { lease_ms: u64 },

    #[error("no destination node for key: {0}")]
    NoDestinationNode(String),

    // ── Storage errors ────────────────────────────────────────────────
    #[error("storage error: {0}")]
    StorageError(String),

    #[error("failed to persist data to file: {path}: {reason}")]
    PersistenceError { path: PathBuf, reason: String },

    // ── Serialization errors ──────────────────────────────────────────
    #[error("protobuf serialization failed: {0}")]
    ProtobufEncodeError(String),

    #[error("protobuf deserialization failed: {0}")]
    ProtobufDecodeError(String),

    // ── Generic ───────────────────────────────────────────────────────
    #[error("operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("unrecognized operation code: {0}")]
    UnrecognizedOpCode(String),

    #[error("{0}")]
    Internal(String),
}

/// Convenience result alias for ZHT operations.
pub type ZhtResult<T> = Result<T, ZhtError>;

// Note: prost::EncodeError/DecodeError conversions are provided
// in the zht-client and zht-proto crates where prost is a dependency.
