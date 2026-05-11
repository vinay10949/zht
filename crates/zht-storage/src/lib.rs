//! ZHT Storage — NoVoHT (Non-Volatile Hash Table) storage engine.
//!
//! This crate provides a file-backed hash table that replicates the behavior of
//! the original C++ NoVoHT class from the ZHT project. It supports in-memory
//! operations with optional disk persistence in a simple tab-separated format.
//!
//! # Persistence Format
//!
//! The file format is tab-separated, one entry per line:
//! ```text
//! key1\tvalue1\t
//! key2\tvalue2\t
//! ```
//!
//! For removals, entries are prefixed with `~`:
//! ```text
//! ~key1\t
//! ```
//!
//! `flush()` performs a clean rewrite of the entire file with the current
//! in-memory state. Individual `put`/`append` operations append entries to the
//! file for append-only durability.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use tracing::{debug, warn};
use zht_common::error::{ZhtError, ZhtResult};

/// Non-Volatile Hash Table — a thread-safe, optionally file-backed key-value store.
///
/// Replicates the behavior of the C++ `NoVoHT` class:
/// - In-memory `HashMap` wrapped in a `parking_lot::RwLock` for concurrency.
/// - Optional file-backed persistence in a simple tab-separated format.
/// - Support for `put`, `get`, `remove`, `append`, and `compare_swap`.
///
/// # Thread Safety
///
/// All operations are thread-safe. Read operations (`get`, `contains`, `len`,
/// `is_empty`, `keys`, `entries`) acquire a read lock. Write operations
/// (`put`, `remove`, `append`, `compare_swap`, `flush`) acquire a write lock.
pub struct NoVoHT {
    /// In-memory hash table: key -> value.
    map: RwLock<HashMap<String, String>>,
    /// Path to the database file (empty string = no persistence).
    db_path: PathBuf,
    /// Whether persistence is enabled.
    persistent: bool,
}

impl NoVoHT {
    /// Create a new in-memory NoVoHT (no persistence).
    ///
    /// Equivalent to the C++ default constructor `NoVoHT()` with an empty filename.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            db_path: PathBuf::new(),
            persistent: false,
        }
    }

    /// Create a NoVoHT with file-backed persistence.
    ///
    /// If `db_path` points to an existing file, its contents are loaded into
    /// memory. If the file does not exist, it is created. A clean `flush()`
    /// is performed after loading to normalize the file (strip tombstones).
    ///
    /// Equivalent to the C++ constructor `NoVoHT(const string& filename)`.
    pub fn with_persistence<P: AsRef<Path>>(db_path: P) -> ZhtResult<Self> {
        let path = db_path.as_ref().to_path_buf();
        let persistent = true;

        // Open or create the file.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| ZhtError::PersistenceError {
                path: path.clone(),
                reason: format!("failed to open database file: {e}"),
            })?;

        let novoht = Self {
            map: RwLock::new(HashMap::new()),
            db_path: path.clone(),
            persistent,
        };

        // Load existing data from file.
        {
            let reader = BufReader::new(&file);
            novoht.read_file_from_reader(reader)?;
        }

        // Clean rewrite after loading (strips tombstones, normalizes file).
        // This mirrors the C++ behavior where readFile() calls writeFile() at the end.
        drop(file);
        novoht.flush()?;

        debug!(path = %path.display(), "NoVoHT initialized with persistence");

        Ok(novoht)
    }

    /// Insert or update a key-value pair.
    ///
    /// If the key already exists, its value is replaced. If persistence is
    /// enabled, the entry is appended to the database file.
    ///
    /// C++ equivalent: `int NoVoHT::put(string k, string v)`
    /// Returns `Ok(())` on success, `Err(ZhtError::EmptyKey)` if key is empty.
    pub fn put(&self, key: &str, value: &str) -> ZhtResult<()> {
        if key.is_empty() {
            return Err(ZhtError::EmptyKey);
        }

        let mut map = self.map.write();
        map.insert(key.to_string(), value.to_string());

        // Persist to file if enabled.
        if self.persistent {
            if let Err(e) = self.write_entry(key, value) {
                warn!(key, error = %e, "failed to persist put to file");
            }
        }

        debug!(key, "put");
        Ok(())
    }

    /// Look up a value by key.
    ///
    /// Returns `Ok(value)` if found, `Err(ZhtError::KeyNotFound)` if not.
    ///
    /// C++ equivalent: `string* NoVoHT::get(string k)` (returns NULL if not found).
    pub fn get(&self, key: &str) -> ZhtResult<String> {
        let map = self.map.read();
        map.get(key)
            .cloned()
            .ok_or_else(|| ZhtError::KeyNotFound(key.to_string()))
    }

    /// Check if a key exists in the hash table.
    pub fn contains(&self, key: &str) -> bool {
        let map = self.map.read();
        map.contains_key(key)
    }

    /// Remove a key from the hash table.
    ///
    /// Returns `Ok(())` if the key was found and removed,
    /// `Err(ZhtError::KeyNotFound)` if the key does not exist.
    ///
    /// C++ equivalent: `int NoVoHT::remove(string k)` (returns -1 if not found).
    pub fn remove(&self, key: &str) -> ZhtResult<()> {
        let mut map = self.map.write();

        if map.remove(key).is_none() {
            return Err(ZhtError::KeyNotFound(key.to_string()));
        }

        // Persist removal to file if enabled.
        if self.persistent {
            if let Err(e) = self.write_removal(key) {
                warn!(key, error = %e, "failed to persist removal to file");
            }
        }

        debug!(key, "remove");
        Ok(())
    }

    /// Append a value to an existing key, concatenating with `":"` separator.
    ///
    /// If the key does not exist, it is created with the given value
    /// (no separator added). This matches the C++ behavior where a new key
    /// is inserted at the head of the bucket chain without concatenation.
    ///
    /// C++ equivalent: `int NoVoHT::append(string k, string aval)`
    pub fn append(&self, key: &str, value: &str) -> ZhtResult<()> {
        if key.is_empty() {
            return Err(ZhtError::EmptyKey);
        }

        let mut map = self.map.write();

        let new_value = if let Some(existing) = map.get_mut(key) {
            // Key exists: concatenate with ":" separator.
            let concatenated = format!("{}:{}", existing, value);
            *existing = concatenated.clone();
            concatenated
        } else {
            // Key doesn't exist: insert as new entry.
            let v = value.to_string();
            map.insert(key.to_string(), v.clone());
            v
        };

        // Persist to file if enabled.
        if self.persistent {
            if let Err(e) = self.write_entry(key, &new_value) {
                warn!(key, error = %e, "failed to persist append to file");
            }
        }

        debug!(key, "append");
        Ok(())
    }

    /// Atomic compare-and-swap operation.
    ///
    /// If the current value of `key` matches `expected`, it is replaced with
    /// `new_value`. Otherwise, the value is left unchanged.
    ///
    /// **Always returns `Ok(actual_value)`** -- the value that was in the map
    /// after the operation. This lets the caller determine whether the swap
    /// succeeded by comparing the return value with `expected`.
    ///
    /// If the key does not exist, it is created with `new_value` (matching
    /// the C++ behavior where `get` returns NULL and the caller decides).
    ///
    /// C++ equivalent: The `compare_swap` operation in ZHT's `HTWorker`.
    pub fn compare_swap(
        &self,
        key: &str,
        expected: &str,
        new_value: &str,
    ) -> ZhtResult<String> {
        let mut map = self.map.write();

        let actual = if let Some(current) = map.get_mut(key) {
            if current == expected {
                *current = new_value.to_string();
                debug!(key, "compare_swap: matched, swapped");
            } else {
                debug!(key, expected, actual = current.as_str(), "compare_swap: mismatch");
            }
            current.clone()
        } else {
            // Key doesn't exist: insert with new_value.
            map.insert(key.to_string(), new_value.to_string());
            debug!(key, "compare_swap: key not found, inserted");
            new_value.to_string()
        };

        // Persist to file if enabled.
        if self.persistent {
            if let Err(e) = self.write_entry(key, &actual) {
                warn!(key, error = %e, "failed to persist compare_swap to file");
            }
        }

        Ok(actual)
    }

    /// Number of elements in the hash table.
    ///
    /// C++ equivalent: `int NoVoHT::getSize()`
    pub fn len(&self) -> usize {
        let map = self.map.read();
        map.len()
    }

    /// Check if the hash table is empty.
    pub fn is_empty(&self) -> bool {
        let map = self.map.read();
        map.is_empty()
    }

    /// Get all keys in the hash table.
    ///
    /// Returns keys in arbitrary order. Used by `HTWorker` for iteration.
    pub fn keys(&self) -> Vec<String> {
        let map = self.map.read();
        map.keys().cloned().collect()
    }

    /// Get all key-value pairs in the hash table.
    pub fn entries(&self) -> Vec<(String, String)> {
        let map = self.map.read();
        map.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Persist all in-memory data to the database file.
    ///
    /// Performs a clean rewrite: truncates the file and writes all current
    /// entries. This strips any tombstone markers from previous removals.
    ///
    /// C++ equivalent: `int NoVoHT::writeFile()`
    pub fn flush(&self) -> ZhtResult<()> {
        if !self.persistent {
            return Ok(());
        }

        let map = self.map.read();

        // Open file in truncate mode for clean rewrite.
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.db_path)
            .map_err(|e| ZhtError::PersistenceError {
                path: self.db_path.clone(),
                reason: format!("failed to open file for flush: {e}"),
            })?;

        let mut writer = BufWriter::new(file);

        for (key, value) in map.iter() {
            writeln!(writer, "{key}\t{value}\t").map_err(|e| {
                ZhtError::PersistenceError {
                    path: self.db_path.clone(),
                    reason: format!("write failed during flush: {e}"),
                }
            })?;
        }

        writer.flush().map_err(|e| ZhtError::PersistenceError {
            path: self.db_path.clone(),
            reason: format!("flush failed: {e}"),
        })?;

        debug!(
            entries = map.len(),
            path = %self.db_path.display(),
            "flushed NoVoHT to file"
        );

        Ok(())
    }

    /// Load data from the database file into memory.
    ///
    /// Reads the file line by line. Each line is tab-separated:
    /// - `key\tvalue\t` -> insert/update the key.
    /// - `~key\t` -> tombstone, skip (already removed).
    ///
    /// This method does **not** clear existing in-memory data; it overlays
    /// the file contents onto whatever is already in the map. For a clean
    /// load, the map should be empty when this is called.
    ///
    /// C++ equivalent: `void NoVoHT::readFile()`
    fn read_file_from_reader<R: BufRead>(&self, reader: R) -> ZhtResult<()> {
        let mut map = self.map.write();

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    warn!(error = %e, "failed to read line from database file, stopping");
                    break;
                }
            };

            // Skip empty lines.
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Split on tabs.
            let parts: Vec<&str> = line.split('\t').collect();

            if parts.is_empty() {
                continue;
            }

            // Check for tombstone marker.
            let raw_key = parts[0];
            if raw_key.starts_with('~') {
                // Tombstone: remove the key if it exists.
                let key = &raw_key[1..];
                map.remove(key);
                continue;
            }

            if parts.len() < 2 {
                // Malformed line (no value), skip.
                warn!(line, "malformed line in database file, skipping");
                continue;
            }

            let key = raw_key.to_string();
            let value = parts[1].to_string();

            // If there are additional tab-separated parts (from append operations),
            // concatenate them with ":" separator, matching the C++ behavior
            // where append writes key\tappended_value\t.
            if parts.len() > 2 {
                let extra_values: Vec<&str> =
                    parts[2..].iter().filter(|s| !s.is_empty()).copied().collect();
                if !extra_values.is_empty() {
                    let full_value = format!("{}:{}", value, extra_values.join(":"));
                    map.insert(key, full_value);
                } else {
                    map.insert(key, value);
                }
            } else {
                map.insert(key, value);
            }
        }

        debug!(entries = map.len(), "loaded entries from file");
        Ok(())
    }

    /// Append a single entry to the database file.
    ///
    /// Used by `put`/`append` for append-only writes during normal operation.
    fn write_entry(&self, key: &str, value: &str) -> ZhtResult<()> {
        if !self.persistent {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.db_path)
            .map_err(|e| ZhtError::PersistenceError {
                path: self.db_path.clone(),
                reason: format!("failed to open file for append: {e}"),
            })?;

        writeln!(file, "{key}\t{value}\t").map_err(|e| ZhtError::PersistenceError {
            path: self.db_path.clone(),
            reason: format!("write failed: {e}"),
        })?;

        file.flush().map_err(|e| ZhtError::PersistenceError {
            path: self.db_path.clone(),
            reason: format!("flush failed: {e}"),
        })?;

        Ok(())
    }

    /// Write a tombstone entry for a removed key.
    ///
    /// Writes `~key\t` to the file to mark the key as deleted.
    /// The actual cleanup happens on the next `flush()`.
    fn write_removal(&self, key: &str) -> ZhtResult<()> {
        if !self.persistent {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.db_path)
            .map_err(|e| ZhtError::PersistenceError {
                path: self.db_path.clone(),
                reason: format!("failed to open file for removal append: {e}"),
            })?;

        writeln!(file, "~{key}\t").map_err(|e| ZhtError::PersistenceError {
            path: self.db_path.clone(),
            reason: format!("write removal failed: {e}"),
        })?;

        file.flush().map_err(|e| ZhtError::PersistenceError {
            path: self.db_path.clone(),
            reason: format!("flush removal failed: {e}"),
        })?;

        Ok(())
    }
}

impl Default for NoVoHT {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::NamedTempFile;

    #[test]
    fn test_basic_put_and_get() {
        let ht = NoVoHT::new();
        ht.put("name", "Alice").unwrap();
        assert_eq!(ht.get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_overwrite_existing_key() {
        let ht = NoVoHT::new();
        ht.put("key", "value1").unwrap();
        ht.put("key", "value2").unwrap();
        assert_eq!(ht.get("key").unwrap(), "value2");
    }

    #[test]
    fn test_get_nonexistent_key_returns_error() {
        let ht = NoVoHT::new();
        let result = ht.get("missing");
        assert!(result.is_err());
        match result.unwrap_err() {
            ZhtError::KeyNotFound(k) => assert_eq!(k, "missing"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_put_empty_key_returns_error() {
        let ht = NoVoHT::new();
        let result = ht.put("", "value");
        assert!(result.is_err());
        match result.unwrap_err() {
            ZhtError::EmptyKey => {}
            other => panic!("expected EmptyKey, got: {other}"),
        }
    }

    #[test]
    fn test_remove_existing_key() {
        let ht = NoVoHT::new();
        ht.put("key", "value").unwrap();
        assert!(ht.contains("key"));
        ht.remove("key").unwrap();
        assert!(!ht.contains("key"));
    }

    #[test]
    fn test_remove_nonexistent_key_returns_error() {
        let ht = NoVoHT::new();
        let result = ht.remove("missing");
        assert!(result.is_err());
        match result.unwrap_err() {
            ZhtError::KeyNotFound(k) => assert_eq!(k, "missing"),
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_append_to_existing_key() {
        let ht = NoVoHT::new();
        ht.put("list", "a").unwrap();
        ht.append("list", "b").unwrap();
        assert_eq!(ht.get("list").unwrap(), "a:b");
    }

    #[test]
    fn test_append_multiple_times() {
        let ht = NoVoHT::new();
        ht.put("list", "a").unwrap();
        ht.append("list", "b").unwrap();
        ht.append("list", "c").unwrap();
        assert_eq!(ht.get("list").unwrap(), "a:b:c");
    }

    #[test]
    fn test_append_to_nonexistent_key() {
        let ht = NoVoHT::new();
        ht.append("newkey", "value").unwrap();
        assert_eq!(ht.get("newkey").unwrap(), "value");
    }

    #[test]
    fn test_append_empty_key_returns_error() {
        let ht = NoVoHT::new();
        let result = ht.append("", "value");
        assert!(result.is_err());
        match result.unwrap_err() {
            ZhtError::EmptyKey => {}
            other => panic!("expected EmptyKey, got: {other}"),
        }
    }

    #[test]
    fn test_compare_swap_success() {
        let ht = NoVoHT::new();
        ht.put("key", "old").unwrap();
        let result = ht.compare_swap("key", "old", "new").unwrap();
        assert_eq!(result, "new");
        assert_eq!(ht.get("key").unwrap(), "new");
    }

    #[test]
    fn test_compare_swap_failure() {
        let ht = NoVoHT::new();
        ht.put("key", "actual").unwrap();
        let result = ht.compare_swap("key", "wrong", "new").unwrap();
        assert_eq!(result, "actual");
        assert_eq!(ht.get("key").unwrap(), "actual");
    }

    #[test]
    fn test_compare_swap_nonexistent_key() {
        let ht = NoVoHT::new();
        let result = ht.compare_swap("newkey", "expected", "inserted").unwrap();
        assert_eq!(result, "inserted");
        assert_eq!(ht.get("newkey").unwrap(), "inserted");
    }

    #[test]
    fn test_contains_key() {
        let ht = NoVoHT::new();
        assert!(!ht.contains("key"));
        ht.put("key", "value").unwrap();
        assert!(ht.contains("key"));
        ht.remove("key").unwrap();
        assert!(!ht.contains("key"));
    }

    #[test]
    fn test_len_and_is_empty() {
        let ht = NoVoHT::new();
        assert_eq!(ht.len(), 0);
        assert!(ht.is_empty());

        ht.put("a", "1").unwrap();
        assert_eq!(ht.len(), 1);
        assert!(!ht.is_empty());

        ht.put("b", "2").unwrap();
        assert_eq!(ht.len(), 2);

        ht.put("a", "3").unwrap();
        assert_eq!(ht.len(), 2);

        ht.remove("a").unwrap();
        assert_eq!(ht.len(), 1);

        ht.remove("b").unwrap();
        assert_eq!(ht.len(), 0);
        assert!(ht.is_empty());
    }

    #[test]
    fn test_keys() {
        let ht = NoVoHT::new();
        ht.put("a", "1").unwrap();
        ht.put("b", "2").unwrap();
        ht.put("c", "3").unwrap();

        let mut keys = ht.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_entries() {
        let ht = NoVoHT::new();
        ht.put("x", "10").unwrap();
        ht.put("y", "20").unwrap();

        let mut entries = ht.entries();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                ("x".to_string(), "10".to_string()),
                ("y".to_string(), "20".to_string())
            ]
        );
    }

    #[test]
    fn test_keys_empty() {
        let ht = NoVoHT::new();
        assert!(ht.keys().is_empty());
        assert!(ht.entries().is_empty());
    }

    #[test]
    fn test_default() {
        let ht = NoVoHT::default();
        assert!(ht.is_empty());
    }

    #[test]
    fn test_persistence_put_flush_recreate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("name", "Alice").unwrap();
            ht.put("age", "30").unwrap();
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.get("name").unwrap(), "Alice");
            assert_eq!(ht.get("age").unwrap(), "30");
            assert_eq!(ht.len(), 2);
        }
    }

    #[test]
    fn test_persistence_remove_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("a", "1").unwrap();
            ht.put("b", "2").unwrap();
            ht.remove("a").unwrap();
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.get("b").unwrap(), "2");
            assert!(ht.get("a").is_err());
            assert_eq!(ht.len(), 1);
        }
    }

    #[test]
    fn test_persistence_append_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("list", "a").unwrap();
            ht.append("list", "b").unwrap();
            ht.append("list", "c").unwrap();
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.get("list").unwrap(), "a:b:c");
        }
    }

    #[test]
    fn test_persistence_overwrite_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("key", "v1").unwrap();
            ht.put("key", "v2").unwrap();
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.get("key").unwrap(), "v2");
            assert_eq!(ht.len(), 1);
        }
    }

    #[test]
    fn test_persistence_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert!(ht.is_empty());
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert!(ht.is_empty());
        }
    }

    #[test]
    fn test_persistence_compare_swap_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("counter", "0").unwrap();
            ht.compare_swap("counter", "0", "1").unwrap();
            ht.flush().unwrap();
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.get("counter").unwrap(), "1");
        }
    }

    #[test]
    fn test_persistence_large_dataset() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            for i in 0..100 {
                ht.put(&format!("key_{i}"), &format!("value_{i}")).unwrap();
            }
            ht.flush().unwrap();
            assert_eq!(ht.len(), 100);
        }

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            assert_eq!(ht.len(), 100);
            for i in 0..100 {
                assert_eq!(ht.get(&format!("key_{i}")).unwrap(), format!("value_{i}"));
            }
        }
    }

    #[test]
    fn test_in_memory_no_persistence() {
        let ht = NoVoHT::new();
        ht.put("key", "value").unwrap();
        ht.flush().unwrap();
        assert_eq!(ht.get("key").unwrap(), "value");
        assert!(!ht.persistent);
    }

    #[test]
    fn test_in_memory_remove_and_reinsert() {
        let ht = NoVoHT::new();
        ht.put("key", "v1").unwrap();
        ht.remove("key").unwrap();
        assert!(!ht.contains("key"));
        ht.put("key", "v2").unwrap();
        assert_eq!(ht.get("key").unwrap(), "v2");
    }

    #[test]
    fn test_concurrent_puts() {
        let ht = Arc::new(NoVoHT::new());
        let mut handles = vec![];

        for i in 0..10 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{i}_{j}");
                    let value = format!("value_{i}_{j}");
                    ht.put(&key, &value).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(ht.len(), 1000);

        for i in 0..10 {
            for j in 0..100 {
                let key = format!("key_{i}_{j}");
                let value = format!("value_{i}_{j}");
                assert_eq!(ht.get(&key).unwrap(), value);
            }
        }
    }

    #[test]
    fn test_concurrent_reads() {
        let ht = Arc::new(NoVoHT::new());

        for i in 0..100 {
            ht.put(&format!("key_{i}"), &format!("value_{i}")).unwrap();
        }

        let mut handles = vec![];

        for _ in 0..10 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key_{i}");
                    let value = ht.get(&key).unwrap();
                    assert_eq!(value, format!("value_{i}"));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_read_write() {
        let ht = Arc::new(NoVoHT::new());

        for i in 0..50 {
            ht.put(&format!("key_{i}"), &format!("value_{i}")).unwrap();
        }

        let mut handles = vec![];

        // Writer threads.
        for i in 50..70 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for j in 0..50 {
                    let key = format!("key_{i}_{j}");
                    ht.put(&key, "written").unwrap();
                }
            }));
        }

        // Reader threads.
        for _ in 0..5 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let result = ht.get("key_0");
                    assert!(result.is_ok());
                }
            }));
        }

        // Append threads.
        for _ in 0..3 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for i in 0..10 {
                    ht.append("append_key", &format!("seg_{i}")).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(ht.contains("key_0"));
        assert_eq!(ht.get("key_0").unwrap(), "value_0");

        let append_val = ht.get("append_key").unwrap();
        let segments: Vec<&str> = append_val.split(':').collect();
        assert_eq!(segments.len(), 30);
    }

    #[test]
    fn test_concurrent_remove() {
        let ht = Arc::new(NoVoHT::new());

        for i in 0..100 {
            ht.put(&format!("key_{i}"), "value").unwrap();
        }

        let mut handles = vec![];

        for i in 0..10 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let key = format!("key_{}", i * 10 + j);
                    let _ = ht.remove(&key);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(ht.len(), 0);
        assert!(ht.is_empty());
    }

    #[test]
    fn test_concurrent_puts_with_persistence() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let ht = Arc::new(NoVoHT::with_persistence(&path).unwrap());

        let mut handles = vec![];

        for i in 0..5 {
            let ht = Arc::clone(&ht);
            handles.push(thread::spawn(move || {
                for j in 0..20 {
                    let key = format!("t{i}_key_{j}");
                    ht.put(&key, &format!("value_{j}")).unwrap();
                }
                if i == 0 {
                    ht.flush().unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let ht2 = NoVoHT::with_persistence(&path).unwrap();
        assert_eq!(ht2.len(), 100);
    }

    #[test]
    fn test_keys_with_special_characters() {
        let ht = NoVoHT::new();
        ht.put("key with spaces", "value").unwrap();
        ht.put("key\twith\ttabs", "value2").unwrap();
        ht.put("key\nwith\nnewlines", "value3").unwrap();

        assert_eq!(ht.get("key with spaces").unwrap(), "value");
        assert_eq!(ht.get("key\twith\ttabs").unwrap(), "value2");
        assert_eq!(ht.get("key\nwith\nnewlines").unwrap(), "value3");
        assert_eq!(ht.len(), 3);
    }

    #[test]
    fn test_empty_value() {
        let ht = NoVoHT::new();
        ht.put("key", "").unwrap();
        assert_eq!(ht.get("key").unwrap(), "");
        assert!(ht.contains("key"));
    }

    #[test]
    fn test_multiple_operations_same_key() {
        let ht = NoVoHT::new();

        ht.put("k", "v1").unwrap();
        assert_eq!(ht.get("k").unwrap(), "v1");

        ht.append("k", "v2").unwrap();
        assert_eq!(ht.get("k").unwrap(), "v1:v2");

        ht.put("k", "v3").unwrap();
        assert_eq!(ht.get("k").unwrap(), "v3");

        ht.append("k", "v4").unwrap();
        assert_eq!(ht.get("k").unwrap(), "v3:v4");

        ht.remove("k").unwrap();
        assert!(ht.get("k").is_err());

        ht.put("k", "v5").unwrap();
        assert_eq!(ht.get("k").unwrap(), "v5");
    }

    #[test]
    fn test_persistence_file_format() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let ht = NoVoHT::with_persistence(&path).unwrap();
            ht.put("hello", "world").unwrap();
            ht.flush().unwrap();
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "hello\tworld\t");
    }
}
