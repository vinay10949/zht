//! Host entity representation.
//!
//! Represents a remote ZHT server node with its host address and port.
//! This is the Rust equivalent of the C++ `HostEntity` struct.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

/// A remote ZHT server node.
///
/// Each node in the cluster is identified by its host and port.
/// The `sock` field from the original C++ (file descriptor) is not
/// needed in Rust since we use tokio channels and connection pooling.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostEntity {
    /// Host name or IP address (e.g., `"localhost"`, `"192.168.1.10"`)
    pub host: String,
    /// Port number
    pub port: u16,
}

impl HostEntity {
    /// Create a new host entity.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Returns the "host:port" string representation.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Parse a "host:port" string into a `HostEntity`.
    pub fn from_address(addr: &str) -> Option<Self> {
        let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }
        let port = parts[0].parse::<u16>().ok()?;
        let host = parts[1].to_string();
        Some(Self { host, port })
    }

    /// Try to resolve this host entity to a `SocketAddr`.
    pub fn to_socket_addr(&self) -> Option<SocketAddr> {
        let addr_str = format!("{}:{}", self.host, self.port);
        SocketAddr::from_str(&addr_str).ok()
    }
}

impl fmt::Display for HostEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl From<(String, u16)> for HostEntity {
    fn from((host, port): (String, u16)) -> Self {
        Self::new(host, port)
    }
}

impl From<(&str, u16)> for HostEntity {
    fn from((host, port): (&str, u16)) -> Self {
        Self::new(host, port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_entity_new() {
        let he = HostEntity::new("localhost", 50000);
        assert_eq!(he.host, "localhost");
        assert_eq!(he.port, 50000);
    }

    #[test]
    fn test_host_entity_address() {
        let he = HostEntity::new("192.168.1.10", 8080);
        assert_eq!(he.address(), "192.168.1.10:8080");
    }

    #[test]
    fn test_host_entity_from_address() {
        let he = HostEntity::from_address("192.168.1.10:8080").unwrap();
        assert_eq!(he.host, "192.168.1.10");
        assert_eq!(he.port, 8080);
    }

    #[test]
    fn test_host_entity_from_address_missing_port() {
        assert!(HostEntity::from_address("justahost").is_none());
    }

    #[test]
    fn test_host_entity_to_socket_addr() {
        let he = HostEntity::new("127.0.0.1", 50000);
        let addr = he.to_socket_addr().unwrap();
        assert_eq!(addr.port(), 50000);
    }

    #[test]
    fn test_host_entity_display() {
        let he = HostEntity::new("node1", 50001);
        assert_eq!(format!("{}", he), "node1:50001");
    }
}
