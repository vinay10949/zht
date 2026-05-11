//! ZHT Client Library — Public API for interacting with the ZHT distributed hash table.
//!
//! This is the Rust equivalent of the C++ `ZHTClient` class and the C binding
//! (`c_zhtclient.h`). It provides idiomatic Rust methods for all DHT operations:
//! insert, lookup, remove, append, compare_swap, and state_change_callback.
//!
//! # Example
//!
//! ```rust,no_run
//! use zht_client::ZHTClient;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut client = ZHTClient::new("zht.conf", "neighbor.conf")?;
//!     client.init().await?;
//!
//!     client.insert("my_key", "my_value").await?;
//!     let value = client.lookup("my_key").await?;
//!     println!("Found: {}", value);
//!
//!     client.teardown().await?;
//!     Ok(())
//! }
//! ```

use std::path::Path;

use anyhow::{Context, Result};
use prost::Message;
use tracing::{debug, info};
use zht_config::AppConfig;
use zht_common::constants::*;
use zht_common::error::ZhtError;
// Phase 2+: serialize and send via proxy
use zht_proto::ZPack;

/// The ZHT client for performing distributed hash table operations.
///
/// Each operation is routed to the appropriate server node using
/// zero-hop routing (client-side hash-based partitioning).
pub struct ZHTClient {
    /// Application configuration (ZHT params + neighbor list).
    config: AppConfig,

    /// Whether the client has been initialized.
    initialized: bool,
}

impl ZHTClient {
    /// Create a new ZHT client with the given configuration file paths.
    ///
    /// The client is not connected until `init()` is called.
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        zht_conf: P1,
        neighbor_conf: P2,
    ) -> Result<Self> {
        let config = AppConfig::load(zht_conf, neighbor_conf)
            .context("Failed to load ZHT configuration")?;

        info!(
            protocol = config.zht.protocol(),
            port = config.zht.port(),
            nodes = config.node_count(),
            "ZHTClient created"
        );

        Ok(Self {
            config,
            initialized: false,
        })
    }

    /// Initialize the client (load config, prepare connection pools).
    ///
    /// Must be called before any DHT operations.
    pub async fn init(&mut self) -> Result<()> {
        if self.initialized {
            debug!("Client already initialized, skipping");
            return Ok(());
        }

        // In Phase 2+, this will:
        // 1. Create the appropriate Proxy based on protocol (TCP/UDP/MPI)
        // 2. Initialize connection pool
        // 3. Warm up connections to neighbor nodes

        self.initialized = true;
        info!("ZHTClient initialized successfully");
        Ok(())
    }

    /// Insert a key-value pair into the DHT.
    ///
    /// Returns `Ok(())` on success.
    pub async fn insert(&self, key: &str, value: &str) -> Result<()> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, "Routing INSERT");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_INSERT.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        zpack.val = value.as_bytes().to_vec();

        // Phase 2+: serialize and send via proxy
        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "INSERT serialized");

        Ok(())
    }

    /// Look up a value by key.
    ///
    /// Returns the value associated with the key, or an error if not found.
    pub async fn lookup(&self, key: &str) -> Result<String> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, "Routing LOOKUP");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_LOOKUP.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();

        // Phase 2+: serialize, send via proxy, receive and parse response
        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "LOOKUP serialized");

        // Placeholder: return not-found error until transport is implemented
        Err(anyhow::anyhow!(ZhtError::KeyNotFound(key.to_string())))
    }

    /// Remove a key from the DHT.
    ///
    /// Returns `Ok(())` on success.
    pub async fn remove(&self, key: &str) -> Result<()> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, "Routing REMOVE");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_REMOVE.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();

        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "REMOVE serialized");

        Ok(())
    }

    /// Append data to an existing key's value.
    ///
    /// Returns `Ok(())` on success.
    pub async fn append(&self, key: &str, value: &str) -> Result<()> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, "Routing APPEND");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_APPEND.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        zpack.val = value.as_bytes().to_vec();

        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "APPEND serialized");

        Ok(())
    }

    /// Atomically compare-and-swap a value.
    ///
    /// If the current value for `key` matches `expected`, replaces it with `new_value`.
    /// Returns the actual current value (regardless of whether the swap succeeded).
    pub async fn compare_swap(
        &self,
        key: &str,
        expected: &str,
        new_value: &str,
    ) -> Result<String> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, "Routing COMPARE_SWAP");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_CMPSWP.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        zpack.val = expected.as_bytes().to_vec();
        zpack.newval = new_value.as_bytes().to_vec();

        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "COMPARE_SWAP serialized");

        // Placeholder
        Err(anyhow::anyhow!(ZhtError::KeyNotFound(key.to_string())))
    }

    /// State change callback — block until the key's value matches the expected value.
    ///
    /// Polls the server at the configured interval until:
    /// - The stored value matches `expected` → returns success
    /// - The lease expires (timeout) → returns error
    pub async fn state_change_callback(
        &self,
        key: &str,
        expected: &str,
        lease_ms: u64,
    ) -> Result<()> {
        self.ensure_initialized()?;
        self.check_key(key)?;

        let node = self.config.resolve_node(key)?;
        debug!(key, host = %node.host, port = node.port, lease_ms, "Routing STATE_CHANGE_CALLBACK");

        let mut zpack = ZPack::default();
        zpack.opcode = OPC_STCHGCB.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        zpack.val = expected.as_bytes().to_vec();
        zpack.lease = lease_ms.to_string().as_bytes().to_vec();

        let serialized = self.serialize_zpack(&zpack)?;
        debug!(key, bytes = serialized.len(), "STATE_CHANGE_CALLBACK serialized");

        Ok(())
    }

    /// Tear down the client and release resources.
    pub async fn teardown(&mut self) -> Result<()> {
        if !self.initialized {
            return Ok(());
        }

        // Phase 2+: close connection pools, release resources

        self.initialized = false;
        info!("ZHTClient torn down");
        Ok(())
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    // ── Private helpers ───────────────────────────────────────────────

    fn ensure_initialized(&self) -> Result<()> {
        if !self.initialized {
            anyhow::bail!("ZHTClient not initialized — call init() first");
        }
        Ok(())
    }

    fn check_key(&self, key: &str) -> Result<()> {
        if key.is_empty() {
            Err(anyhow::anyhow!(ZhtError::EmptyKey))
        } else {
            Ok(())
        }
    }

    fn serialize_zpack(&self, zpack: &ZPack) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        zpack.encode(&mut buf)
            .map_err(|e| ZhtError::ProtobufEncodeError(e.to_string()))?;
        Ok(buf)
    }
}

impl Drop for ZHTClient {
    fn drop(&mut self) {
        if self.initialized {
            debug!("ZHTClient dropped without explicit teardown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file
    }

    fn make_test_config() -> (NamedTempFile, NamedTempFile) {
        let zht = write_temp_file("PROTOCOL TCP\nPORT 50000\n");
        let neighbor = write_temp_file("localhost 50000\n");
        (zht, neighbor)
    }

    #[tokio::test]
    async fn test_client_creation() {
        let (zht, neighbor) = make_test_config();
        let client = ZHTClient::new(zht.path(), neighbor.path());
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_client_init_and_teardown() {
        let (zht, neighbor) = make_test_config();
        let mut client = ZHTClient::new(zht.path(), neighbor.path()).unwrap();
        client.init().await.unwrap();
        assert!(client.initialized);
        client.teardown().await.unwrap();
        assert!(!client.initialized);
    }

    #[tokio::test]
    async fn test_insert_empty_key_fails() {
        let (zht, neighbor) = make_test_config();
        let mut client = ZHTClient::new(zht.path(), neighbor.path()).unwrap();
        client.init().await.unwrap();

        let result = client.insert("", "value").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_operation_before_init_fails() {
        let (zht, neighbor) = make_test_config();
        let client = ZHTClient::new(zht.path(), neighbor.path()).unwrap();

        let result = client.insert("key", "value").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));
    }
}
