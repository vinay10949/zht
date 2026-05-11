//! Hash utility functions.
//!
//! Provides the same Jenkins-one-at-a-time inspired hash function used
//! in the original ZHT for consistent key-to-node routing.

/// Hash utility for key-based routing.
///
/// The hash function is a Jenkins-one-at-a-time inspired algorithm that
/// produces a 64-bit hash. It is used for "zero-hop" routing where the
/// client computes `hash(key) % num_nodes` to determine the target server
/// directly, without any indirection.
pub struct HashUtil;

impl HashUtil {
    /// Maximum 64-bit unsigned integer value.
    pub const ULL_MAX: u64 = u64::MAX;

    /// Generate a 64-bit hash from a byte string.
    ///
    /// This replicates the original C++ hash function:
    /// ```c
    /// uint64_t hash = 0;
    /// while (c = (*pc++)) {
    ///     hash = c + (hash << 6) + (hash << 16) - hash;
    /// }
    /// ```
    pub fn gen_hash(data: &[u8]) -> u64 {
        let mut hash: u64 = 0;
        for &byte in data {
            // Only process non-zero bytes (mirrors C while-loop termination)
            if byte == 0 {
                break;
            }
            let c = byte as u64;
            // Wrapping arithmetic to match C overflow behavior
            hash = c
                .wrapping_add(hash << 6)
                .wrapping_add(hash << 16)
                .wrapping_sub(hash);
        }
        hash
    }

    /// Generate a 64-bit hash from a string.
    pub fn gen_hash_str(s: &str) -> u64 {
        Self::gen_hash(s.as_bytes())
    }

    /// Generate a base string from host and port (e.g., "host:port").
    pub fn gen_base(host: &str, port: u16) -> String {
        format!("{}:{}", host, port)
    }

    /// Generate a random alphanumeric string of the given length.
    ///
    /// Uses a simple Xorshift PRNG seeded from the system time.
    /// Not cryptographically secure — used only for ID generation.
    pub fn random_string(len: usize) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(42);

        const ALPHANUM: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut state: u64 = seed as u64;

        (0..len)
            .map(|_| {
                // Simple Xorshift64 PRNG
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                ALPHANUM[(state % ALPHANUM.len() as u64) as usize] as char
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_hash_empty() {
        let hash = HashUtil::gen_hash(b"");
        assert_eq!(hash, 0);
    }

    #[test]
    fn test_gen_hash_deterministic() {
        let h1 = HashUtil::gen_hash(b"hello");
        let h2 = HashUtil::gen_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_gen_hash_different_keys() {
        let h1 = HashUtil::gen_hash(b"key1");
        let h2 = HashUtil::gen_hash(b"key2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_gen_hash_str() {
        let h1 = HashUtil::gen_hash_str("test_key");
        let h2 = HashUtil::gen_hash(b"test_key");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_gen_hash_null_terminator() {
        // Hash should stop at null byte (matching C string behavior)
        let h1 = HashUtil::gen_hash(b"hello\0world");
        let h2 = HashUtil::gen_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_gen_base() {
        assert_eq!(HashUtil::gen_base("localhost", 50000), "localhost:50000");
    }

    #[test]
    fn test_random_string_length() {
        let s = HashUtil::random_string(20);
        assert_eq!(s.len(), 20);
        assert!(s.chars().all(|c| c.is_alphanumeric()));
    }

    #[test]
    fn test_hash_distribution() {
        // Verify hash produces reasonable distribution across 100 keys
        let hashes: std::collections::HashSet<u64> = (0..100)
            .map(|i| HashUtil::gen_hash_str(&format!("key_{}", i)))
            .collect();
        // All 100 hashes should be unique
        assert_eq!(hashes.len(), 100);
    }

    #[test]
    fn test_routing_consistency() {
        // Simulate the zero-hop routing: hash(key) % num_nodes
        let num_nodes = 8usize;
        let keys = vec!["alpha", "beta", "gamma", "delta", "epsilon"];

        // Route each key — should always produce same node index
        for key in &keys {
            let hash = HashUtil::gen_hash_str(key);
            let idx = (hash as usize) % num_nodes;
            // Verify consistency: same key always routes to same node
            let idx2 = (HashUtil::gen_hash_str(key) as usize) % num_nodes;
            assert_eq!(idx, idx2, "Key '{}' routes inconsistently", key);
        }
    }
}
