//! ZHT Configuration — Parsing and management of ZHT configuration files.
//!
//! Handles two configuration file formats:
//! - `zht.conf`: Key-value pairs for ZHT parameters (protocol, port, message size, etc.)
//! - `neighbor.conf`: Space-delimited host-port pairs defining cluster membership
//!
//! This module is the Rust equivalent of the C++ `ConfHandler` class.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};
use zht_common::conf_entry::ConfEntry;
use zht_common::constants::*;
use zht_common::host_entity::HostEntity;
use zht_common::{ZhtError, ZhtResult};

// ── ZHT Configuration ────────────────────────────────────────────────

/// Parsed ZHT configuration from `zht.conf`.
///
/// Contains all parameters needed to configure a ZHT server or client.
#[derive(Debug, Clone)]
pub struct ZhtConfig {
    /// Raw key-value map from the config file.
    parameters: HashMap<String, String>,

    /// Path to the zht.conf file.
    pub zht_conf_path: PathBuf,
}

impl ZhtConfig {
    /// Load configuration from a `zht.conf` file.
    pub fn load<P: AsRef<Path>>(path: P) -> ZhtResult<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(ZhtError::ConfigNotFound { path: path.to_path_buf() });
        }

        let content = std::fs::read_to_string(path).map_err(|e| ZhtError::ConfigParseError {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;

        let mut parameters = HashMap::new();
        for line in content.lines() {
            if let Some(entry) = ConfEntry::from_line(line) {
                parameters.insert(entry.name().to_string(), entry.value().to_string());
                debug!(param = %entry.name(), value = %entry.value(), "Loaded config entry");
            }
        }

        info!(
            path = %path.display(),
            params = parameters.len(),
            "Loaded ZHT configuration"
        );

        Ok(Self {
            parameters,
            zht_conf_path: path.to_path_buf(),
        })
    }

    /// Get a parameter value by name.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.parameters.get(key).map(|s| s.as_str())
    }

    /// Get a parameter value, or return the default.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    /// Get a parameter as a parsed type, or return default.
    pub fn get_parsed<T: std::str::FromStr>(&self, key: &str, default: T) -> T {
        self.get(key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// The transport protocol (TCP, UDP, or MPI).
    pub fn protocol(&self) -> &str {
        self.parameters
            .get(CONF_PROTOCOL)
            .map(|s| s.as_str())
            .unwrap_or("TCP")
    }

    /// The server listening port.
    pub fn port(&self) -> u16 {
        self.get_parsed(CONF_PORT, 50000)
    }

    /// Maximum message size in bytes.
    pub fn msg_maxsize(&self) -> usize {
        self.get_parsed(CONF_MSG_MAXSIZE, MSG_DEFAULT_SIZE)
    }

    /// State change callback poll interval in milliseconds.
    pub fn sccb_poll_interval(&self) -> u64 {
        self.get_parsed(CONF_SCCB_POLL_INTERVAL, SCCB_POLL_DEFAULT_INTERVAL)
    }

    /// Whether to instantly swap in-memory data to the NoVoHT DB file.
    pub fn instant_swap(&self) -> bool {
        self.get_parsed(CONF_INSTANT_SWAP, 0) == 1
    }

    /// Maximum number of ZHT instances.
    pub fn max_zht(&self) -> u32 {
        self.get_parsed(CONF_MAX_ZHT, 512)
    }

    /// Number of replicas.
    pub fn num_replicas(&self) -> u32 {
        self.get_parsed(CONF_NUM_REPLICAS, 0)
    }

    /// Replication type (1 = client-side).
    pub fn replication_type(&self) -> u32 {
        self.get_parsed(CONF_REPLICATION_TYPE, 0)
    }

    /// ZHT capacity.
    pub fn zht_capacity(&self) -> u32 {
        self.get_parsed(CONF_ZHT_CAPACITY, 4)
    }

    /// HTData path.
    pub fn htdata_path(&self) -> String {
        self.get_or(CONF_HTDATA_PATH, "data/")
    }

    /// Migration sleep time in microseconds.
    pub fn migslp_time(&self) -> u64 {
        self.get_parsed(CONF_MIGSLP_TIME, 1_000_000)
    }
}

// ── Neighbor Configuration ───────────────────────────────────────────

/// Parsed neighbor configuration from `neighbor.conf`.
///
/// Each line in `neighbor.conf` is a space-delimited `HOST PORT` pair
/// defining a node in the ZHT cluster.
#[derive(Debug, Clone)]
pub struct NeighborConfig {
    /// Ordered list of neighbor nodes.
    pub neighbors: Vec<HostEntity>,

    /// Path to the neighbor.conf file.
    pub neighbor_conf_path: PathBuf,
}

impl NeighborConfig {
    /// Load neighbor configuration from a `neighbor.conf` file.
    pub fn load<P: AsRef<Path>>(path: P) -> ZhtResult<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(ZhtError::ConfigNotFound { path: path.to_path_buf() });
        }

        let content = std::fs::read_to_string(path).map_err(|e| ZhtError::ConfigParseError {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;

        let mut neighbors = Vec::new();
        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let mut parts = trimmed.split_whitespace();
            let host = match parts.next() {
                Some(h) => h.to_string(),
                None => {
                    warn!(line = line_num, "Empty line in neighbor config, skipping");
                    continue;
                }
            };

            let port: u16 = match parts.next() {
                Some(p) => p.parse().map_err(|_| ZhtError::InvalidConfigValue {
                    param: "PORT".to_string(),
                    reason: format!(
                        "invalid port '{}' in {} line {}",
                        p,
                        path.display(),
                        line_num + 1
                    ),
                })?,
                None => {
                    return Err(ZhtError::InvalidConfigValue {
                        param: "PORT".to_string(),
                        reason: format!("missing port in {} line {}", path.display(), line_num + 1),
                    });
                }
            };

            debug!(host = %host, port, line = line_num, "Loading neighbor");
            neighbors.push(HostEntity::new(host, port));
        }

        if neighbors.is_empty() {
            warn!("Neighbor configuration is empty — no cluster nodes defined");
        }

        info!(
            path = %path.display(),
            nodes = neighbors.len(),
            "Loaded neighbor configuration"
        );

        Ok(Self {
            neighbors,
            neighbor_conf_path: path.to_path_buf(),
        })
    }

    /// Number of neighbor nodes.
    pub fn node_count(&self) -> usize {
        self.neighbors.len()
    }

    /// Get the node at a given index.
    pub fn get(&self, index: usize) -> Option<&HostEntity> {
        self.neighbors.get(index)
    }

    /// Find the node index for a given host:port string.
    pub fn find_index(&self, host: &str, port: u16) -> Option<usize> {
        self.neighbors.iter().position(|n| n.host == host && n.port == port)
    }
}

// ── Unified Configuration ────────────────────────────────────────────

/// Combined configuration holding both ZHT parameters and neighbor list.
///
/// This is the primary configuration object passed to server and client
/// initialization. It is the Rust equivalent of calling
/// `ConfHandler::initConf(zhtConf, neighborConf)` in C++.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// ZHT parameters (from zht.conf).
    pub zht: ZhtConfig,

    /// Neighbor nodes (from neighbor.conf).
    pub neighbors: NeighborConfig,
}

impl AppConfig {
    /// Load both configuration files.
    pub fn load<P1: AsRef<Path>, P2: AsRef<Path>>(
        zht_conf: P1,
        neighbor_conf: P2,
    ) -> ZhtResult<Self> {
        let zht = ZhtConfig::load(zht_conf)?;
        let neighbors = NeighborConfig::load(neighbor_conf)?;
        Ok(Self { zht, neighbors })
    }

    /// Get a ZHT parameter by name.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.zht.get(key)
    }

    /// Total number of neighbor nodes.
    pub fn node_count(&self) -> usize {
        self.neighbors.node_count()
    }

    /// Resolve the target host entity for a given key using zero-hop routing.
    ///
    /// Computes `hash(key) % num_nodes` to directly determine which
    /// server node should handle the request.
    pub fn resolve_node(&self, key: &str) -> ZhtResult<&HostEntity> {
        use zht_common::hash_util::HashUtil;

        let node_count = self.node_count();
        if node_count == 0 {
            return Err(ZhtError::NoDestinationNode(key.to_string()));
        }

        let hash_code = HashUtil::gen_hash_str(key);
        let index = (hash_code as usize) % node_count;
        self.neighbors
            .get(index)
            .ok_or_else(|| ZhtError::NoDestinationNode(key.to_string()))
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

    #[test]
    fn test_load_zht_config() {
        let content = r#"
# Comment line
PROTOCOL TCP
PORT 50000
MSG_MAXSIZE 1000000
SCCB_POLL_INTERVAL 100
INSTANT_SWAP 0
"#;
        let file = write_temp_file(content);
        let config = ZhtConfig::load(file.path()).unwrap();

        assert_eq!(config.protocol(), "TCP");
        assert_eq!(config.port(), 50000);
        assert_eq!(config.msg_maxsize(), 1_000_000);
        assert_eq!(config.sccb_poll_interval(), 100);
        assert!(!config.instant_swap());
    }

    #[test]
    fn test_load_zht_config_defaults() {
        let content = "PROTOCOL UDP\n";
        let file = write_temp_file(content);
        let config = ZhtConfig::load(file.path()).unwrap();

        assert_eq!(config.protocol(), "UDP");
        assert_eq!(config.port(), 50000); // default
        assert_eq!(config.sccb_poll_interval(), 100); // default
    }

    #[test]
    fn test_load_zht_config_missing_file() {
        let result = ZhtConfig::load("/nonexistent/path/zht.conf");
        assert!(matches!(result, Err(ZhtError::ConfigNotFound { .. })));
    }

    #[test]
    fn test_load_neighbor_config() {
        let content = "localhost 50000\n192.168.1.10 50001\nnode3.local 50002\n";
        let file = write_temp_file(content);
        let config = NeighborConfig::load(file.path()).unwrap();

        assert_eq!(config.node_count(), 3);
        assert_eq!(config.get(0).unwrap().host, "localhost");
        assert_eq!(config.get(0).unwrap().port, 50000);
        assert_eq!(config.get(1).unwrap().host, "192.168.1.10");
        assert_eq!(config.get(2).unwrap().port, 50002);
    }

    #[test]
    fn test_load_neighbor_config_with_comments() {
        let content = "# Cluster nodes\nlocalhost 50000\n# Another node\nnode2 50001\n\n";
        let file = write_temp_file(content);
        let config = NeighborConfig::load(file.path()).unwrap();

        assert_eq!(config.node_count(), 2);
    }

    #[test]
    fn test_load_neighbor_config_empty() {
        let content = "# Only comments\n";
        let file = write_temp_file(content);
        let config = NeighborConfig::load(file.path()).unwrap();

        assert_eq!(config.node_count(), 0);
    }

    #[test]
    fn test_neighbor_find_index() {
        let content = "localhost 50000\nnode2 50001\n";
        let file = write_temp_file(content);
        let config = NeighborConfig::load(file.path()).unwrap();

        assert_eq!(config.find_index("localhost", 50000), Some(0));
        assert_eq!(config.find_index("node2", 50001), Some(1));
        assert_eq!(config.find_index("nonexistent", 9999), None);
    }

    #[test]
    fn test_app_config_load_and_resolve() {
        let zht_content = "PROTOCOL TCP\nPORT 50000\n";
        let neighbor_content = "localhost 50000\nnode2 50001\nnode3 50002\n";

        let zht_file = write_temp_file(zht_content);
        let neighbor_file = write_temp_file(neighbor_content);

        let app = AppConfig::load(zht_file.path(), neighbor_file.path()).unwrap();

        assert_eq!(app.node_count(), 3);
        assert_eq!(app.zht.protocol(), "TCP");

        // Verify zero-hop routing determinism
        let node = app.resolve_node("test_key").unwrap();
        let node2 = app.resolve_node("test_key").unwrap();
        assert_eq!(node.host, node2.host);
        assert_eq!(node.port, node2.port);
    }

    #[test]
    fn test_app_config_empty_neighbors() {
        let zht_content = "PROTOCOL TCP\n";
        let neighbor_content = "# empty\n";

        let zht_file = write_temp_file(zht_content);
        let neighbor_file = write_temp_file(neighbor_content);

        let app = AppConfig::load(zht_file.path(), neighbor_file.path()).unwrap();
        let result = app.resolve_node("any_key");
        assert!(matches!(result, Err(ZhtError::NoDestinationNode(_))));
    }

    #[test]
    fn test_zht_config_get_raw() {
        let content = "CUSTOM_PARAM custom_value\n";
        let file = write_temp_file(content);
        let config = ZhtConfig::load(file.path()).unwrap();

        assert_eq!(config.get("CUSTOM_PARAM"), Some("custom_value"));
        assert_eq!(config.get("NONEXISTENT"), None);
        assert_eq!(config.get_or("NONEXISTENT", "default"), "default");
    }
}
