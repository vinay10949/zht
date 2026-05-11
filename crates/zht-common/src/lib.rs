//! ZHT Common — Core data structures, error types, constants, and utilities.
//!
//! This crate contains the foundational building blocks shared across all
//! other ZHT crates: error types, operation/return codes, configuration
//! types, host entities, and utility functions (hashing, timing, ID generation).

// ── Modules ──────────────────────────────────────────────────────────

pub mod error;
pub mod constants;
pub mod host_entity;
pub mod conf_entry;
pub mod hash_util;
pub mod time_util;
pub mod id_helper;

// ── Re-exports ───────────────────────────────────────────────────────

pub use error::{ZhtError, ZhtResult};
pub use constants::*;
pub use host_entity::HostEntity;
pub use conf_entry::ConfEntry;
pub use hash_util::HashUtil;
pub use time_util::TimeUtil;
pub use id_helper::IdHelper;
