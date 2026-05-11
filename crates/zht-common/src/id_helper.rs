//! ID generation utility.
//!
//! Provides unique ID generation for big data transfer chunking
//! and other identification purposes.

use crate::hash_util::HashUtil;

/// ID generation helper.
///
/// Generates unique identifiers by hashing random strings.
/// Used for big data transfer chunk identification.
pub struct IdHelper;

impl IdHelper {
    /// Default length of generated IDs.
    pub const ID_LEN: usize = 20;

    /// Generate a unique 64-bit ID.
    ///
    /// Creates a random string of length 62 and hashes it to produce
    /// a deterministic but unique 64-bit identifier.
    pub fn gen_id() -> u64 {
        HashUtil::gen_hash(HashUtil::random_string(62).as_bytes())
    }

    /// Generate a unique hex string ID of the specified length.
    pub fn gen_id_string(len: usize) -> String {
        format!("{:0width$x}", Self::gen_id(), width = len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_gen_id_returns_u64() {
        let id = IdHelper::gen_id();
        // Should produce a valid u64
        let _ = id;
    }

    #[test]
    fn test_gen_ids_are_mostly_unique() {
        let mut ids = HashSet::new();
        for _ in 0..1000 {
            ids.insert(IdHelper::gen_id());
        }
        // With 1000 random 64-bit IDs, should have very few collisions
        // if any (birthday paradox: ~50% collision at 2^32)
        assert!(ids.len() > 990, "Too many ID collisions");
    }

    #[test]
    fn test_gen_id_string_length() {
        let id_str = IdHelper::gen_id_string(16);
        assert_eq!(id_str.len(), 16);
    }

    #[test]
    fn test_gen_id_string_is_hex() {
        let id_str = IdHelper::gen_id_string(20);
        assert!(id_str.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
