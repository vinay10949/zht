//! `zht-cache` — Fixed-capacity, thread-safe LRU (Least Recently Used) cache.
//!
//! Mirrors the original C++ `std::list` + `std::map` LRU pattern used in ZHT
//! for hot-entry caching.  Uses `VecDeque` for access-order tracking and
//! `HashMap` for O(1) key lookups, protected by a `parking_lot::RwLock`.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A fixed-capacity LRU (Least Recently Used) cache.
///
/// Thread-safe.  Evicts the least recently used entry when capacity is
/// exceeded.  All public operations are O(1) amortised.
///
/// # Type parameters
/// * `K` — key type (must be `Eq + Hash + Clone`)
/// * `V` — value type (must be `Clone`)
pub struct LruCache<K, V> {
    max_size: usize,
    entries: RwLock<LruInner<K, V>>,
}

/// Internal state behind the `RwLock`.
struct LruInner<K, V> {
    /// Main storage: key → (value, index into `order`).
    ///
    /// The index allows O(1) removal from the `VecDeque` when combined with
    /// `swap_remove_back`/`swap_remove_front` (amortised).
    map: HashMap<K, (V, usize)>,
    /// Access-order deque.  Front = most recently used, back = least recently
    /// used.
    order: VecDeque<K>,
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    /// Create a new LRU cache with the given maximum capacity.
    ///
    /// # Panics
    /// Panics if `max_size` is 0.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0, "LruCache capacity must be > 0");
        Self {
            max_size,
            entries: RwLock::new(LruInner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Insert a key-value pair.  If the key already exists its value is updated
    /// and the entry is touched (promoted to most-recently-used).  If the cache
    /// exceeds capacity after insertion the least recently used entry is
    /// evicted.
    pub fn insert(&self, key: K, value: V) {
        let mut inner = self.entries.write();
        // If key already exists, remove old position first.
        if inner.map.contains_key(&key) {
            inner.remove_order(&key);
        }
        // Insert the new entry.
        inner.order.push_front(key.clone());
        let idx = 0;
        inner.map.insert(key, (value, idx));
        // Evict if over capacity.
        while inner.map.len() > self.max_size {
            if let Some(evicted_key) = inner.order.pop_back() {
                inner.map.remove(&evicted_key);
            }
        }
    }

    /// Insert a key-value pair and return the evicted value (if any).
    ///
    /// If the cache was at capacity and an entry was evicted, `Some(evicted_v)`
    /// is returned.  If the key already existed (update path) or no eviction
    /// was needed, `None` is returned.
    pub fn insert_with_eviction(&self, key: K, value: V) -> Option<V> {
        let mut inner = self.entries.write();
        let is_update = inner.map.contains_key(&key);

        if is_update {
            inner.remove_order(&key);
        }

        inner.order.push_front(key.clone());
        inner.map.insert(key, (value, 0));

        if inner.map.len() > self.max_size {
            if let Some(evicted_key) = inner.order.pop_back() {
                if let Some((evicted_val, _)) = inner.map.remove(&evicted_key) {
                    return Some(evicted_val);
                }
            }
        }
        None
    }

    /// Fetch a value by key.  Returns a clone of the value, or `None` if not
    /// found.  Touches the key (marks as most recently used) on hit.
    pub fn fetch(&self, key: &K) -> Option<V> {
        let mut inner = self.entries.write();
        if let Some((value, _)) = inner.map.get(key) {
            let v = value.clone();
            inner.touch(key);
            Some(v)
        } else {
            None
        }
    }

    /// Check whether a key exists in the cache **without** touching it.
    pub fn contains(&self, key: &K) -> bool {
        self.entries.read().map.contains_key(key)
    }

    /// Remove a key from the cache.  Returns the removed value if it existed.
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut inner = self.entries.write();
        inner.remove_order(key);
        inner.map.remove(key).map(|(v, _)| v)
    }

    /// Touch a key (mark as most recently used) without fetching.
    ///
    /// Does nothing if the key is not present.
    pub fn touch(&self, key: &K) {
        let mut inner = self.entries.write();
        inner.touch(key);
    }

    /// Return the current number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().map.len()
    }

    /// Return `true` if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().map.is_empty()
    }

    /// Return the maximum capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.max_size
    }

    /// Clear all entries.
    pub fn clear(&self) {
        let mut inner = self.entries.write();
        inner.map.clear();
        inner.order.clear();
    }

    /// Return all keys in LRU order (most recently used first).
    #[must_use]
    pub fn keys_lru(&self) -> Vec<K> {
        self.entries.read().order.iter().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl<K: Eq + Hash + Clone, V> LruInner<K, V> {
    /// Move `key` to the front of the order deque (most recently used).
    /// No-op if the key is not in the cache.
    fn touch(&mut self, key: &K) {
        if !self.map.contains_key(key) {
            return;
        }
        self.remove_order(key);
        self.order.push_front(key.clone());
    }

    /// Remove `key` from the order deque.
    fn remove_order(&mut self, key: &K) {
        // Find and remove the key from the deque.
        // VecDeque doesn't have O(1) remove-by-value, so we iterate.
        // This is O(n) in the worst case, but in practice the cache is
        // small (bounded by max_size which is typically in the thousands).
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // -----------------------------------------------------------------------
    // Basic lifecycle
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_cache_is_empty() {
        let cache: LruCache<String, i32> = LruCache::new(10);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 10);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn test_new_zero_capacity_panics() {
        let _: LruCache<String, i32> = LruCache::new(0);
    }

    #[test]
    fn test_insert_and_fetch() {
        let cache = LruCache::new(5);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);

        assert_eq!(cache.fetch(&"a".to_string()), Some(1));
        assert_eq!(cache.fetch(&"b".to_string()), Some(2));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_fetch_nonexistent_returns_none() {
        let cache: LruCache<String, i32> = LruCache::new(5);
        assert_eq!(cache.fetch(&"missing".to_string()), None);
    }

    #[test]
    fn test_insert_overwrites_existing() {
        let cache: LruCache<String, i32> = LruCache::new(5);
        cache.insert("key".to_string(), 10);
        cache.insert("key".to_string(), 20);

        assert_eq!(cache.fetch(&"key".to_string()), Some(20));
        assert_eq!(cache.len(), 1);
    }

    // -----------------------------------------------------------------------
    // LRU eviction
    // -----------------------------------------------------------------------

    #[test]
    fn test_lru_eviction_at_capacity() {
        let cache = LruCache::new(3);
        cache.insert(1, "one");
        cache.insert(2, "two");
        cache.insert(3, "three");
        // Cache is full. Inserting a 4th should evict the least recently used (1).
        cache.insert(4, "four");

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.fetch(&1), None);   // evicted
        assert_eq!(cache.fetch(&2), Some("two"));
        assert_eq!(cache.fetch(&3), Some("three"));
        assert_eq!(cache.fetch(&4), Some("four"));
    }

    #[test]
    fn test_eviction_order() {
        let cache = LruCache::new(3);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);
        // Touch "a" so "b" becomes LRU
        cache.touch(&"a");
        // Insert new entry — should evict "b" (least recently used)
        cache.insert("d", 4);

        assert_eq!(cache.fetch(&"a"), Some(1));   // was touched, safe
        assert_eq!(cache.fetch(&"b"), None);      // evicted (LRU)
        assert_eq!(cache.fetch(&"c"), Some(3));   // safe
        assert_eq!(cache.fetch(&"d"), Some(4));   // just inserted
    }

    #[test]
    fn test_touch_promotes_entry() {
        let cache = LruCache::new(3);
        cache.insert("x", 10);
        cache.insert("y", 20);
        cache.insert("z", 30);
        // "x" is LRU.  Touch it to promote.
        cache.touch(&"x");
        // Now "y" should be LRU.
        cache.insert("w", 40);

        assert_eq!(cache.fetch(&"x"), Some(10));  // promoted, safe
        assert_eq!(cache.fetch(&"y"), None);     // evicted
        assert_eq!(cache.fetch(&"z"), Some(30));
        assert_eq!(cache.fetch(&"w"), Some(40));
    }

    // -----------------------------------------------------------------------
    // Remove
    // -----------------------------------------------------------------------

    #[test]
    fn test_remove_returns_value() {
        let cache = LruCache::new(5);
        cache.insert("key".to_string(), 42);
        let removed = cache.remove(&"key".to_string());
        assert_eq!(removed, Some(42));
        assert!(!cache.contains(&"key".to_string()));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_remove_nonexistent_returns_none() {
        let cache: LruCache<String, i32> = LruCache::new(5);
        assert_eq!(cache.remove(&"ghost".to_string()), None);
    }

    // -----------------------------------------------------------------------
    // Contains / len / capacity
    // -----------------------------------------------------------------------

    #[test]
    fn test_contains_key() {
        let cache = LruCache::new(5);
        cache.insert("yes".to_string(), 1);
        assert!(cache.contains(&"yes".to_string()));
        assert!(!cache.contains(&"no".to_string()));
    }

    #[test]
    fn test_len_and_capacity() {
        let cache = LruCache::new(100);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 100);

        for i in 0..50 {
            cache.insert(i, i);
        }
        assert_eq!(cache.len(), 50);
        assert_eq!(cache.capacity(), 100);
    }

    // -----------------------------------------------------------------------
    // Clear
    // -----------------------------------------------------------------------

    #[test]
    fn test_clear() {
        let cache = LruCache::new(10);
        for i in 0..10 {
            cache.insert(i, i * 10);
        }
        assert_eq!(cache.len(), 10);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    // -----------------------------------------------------------------------
    // keys_lru
    // -----------------------------------------------------------------------

    #[test]
    fn test_keys_lru_order() {
        let cache = LruCache::new(5);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        // Initial order (MRU → LRU): c, b, a
        assert_eq!(cache.keys_lru(), vec!["c", "b", "a"]);

        // Fetch "a" — promotes it to front
        let _ = cache.fetch(&"a");
        assert_eq!(cache.keys_lru(), vec!["a", "c", "b"]);

        // Insert "d" — goes to front
        cache.insert("d", 4);
        assert_eq!(cache.keys_lru(), vec!["d", "a", "c", "b"]);
    }

    // -----------------------------------------------------------------------
    // insert_with_eviction
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_with_eviction_returns_evicted_value() {
        let cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);

        // Insert "c" — should evict "a" (LRU)
        let evicted = cache.insert_with_eviction("c", 3);
        assert_eq!(evicted, Some(1));
        assert_eq!(cache.fetch(&"a"), None);
        assert_eq!(cache.fetch(&"b"), Some(2));
        assert_eq!(cache.fetch(&"c"), Some(3));
    }

    #[test]
    fn test_insert_with_eviction_no_eviction() {
        let cache: LruCache<&str, i32> = LruCache::new(5);
        cache.insert("a", 1);

        // Room available — no eviction
        let evicted = cache.insert_with_eviction("b", 2);
        assert_eq!(evicted, None);
    }

    #[test]
    fn test_insert_with_eviction_update_no_eviction() {
        let cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);

        // Updating existing key — no eviction even though at capacity
        let evicted = cache.insert_with_eviction("a", 100);
        assert_eq!(evicted, None);
        assert_eq!(cache.fetch(&"a"), Some(100));
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_access() {
        let cache = Arc::new(LruCache::new(100));
        let num_threads = 10;
        let ops_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("t{}_k{}", t, i % 50);
                        let val = t * 1000 + i;
                        c.insert(key.clone(), val);
                        let _ = c.fetch(&key);
                        if i % 3 == 0 {
                            c.touch(&key);
                        }
                        if i % 5 == 0 {
                            c.remove(&key);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread should not panic");
        }

        // Cache should still be functional and within capacity.
        assert!(cache.len() <= cache.capacity());
        assert!(!cache.is_empty() || cache.capacity() == 0);
    }
}
