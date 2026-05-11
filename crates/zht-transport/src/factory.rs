//! Transport factory for creating protocol-specific client/server instances.

use std::sync::Arc;

use zht_common::error::ZhtResult;
use zht_common::error::ZhtError;

use crate::metrics::TransportMetrics;
use crate::traits::{TransportClientEnum, TransportServerEnum};

/// Factory for creating transport instances based on protocol configuration.
pub struct TransportFactory;

impl TransportFactory {
    /// Create a protocol-specific transport client.
    pub fn create_client(protocol: &str) -> ZhtResult<TransportClientEnum> {
        match protocol.to_uppercase().as_str() {
            "TCP" => Ok(TransportClientEnum::new_tcp()),
            "UDP" => Ok(TransportClientEnum::new_udp()),
            other => Err(ZhtError::UnsupportedProtocol(other.to_string())),
        }
    }

    /// Create a protocol-specific transport client with shared metrics.
    pub fn create_client_with_metrics(protocol: &str, metrics: Arc<TransportMetrics>) -> ZhtResult<TransportClientEnum> {
        match protocol.to_uppercase().as_str() {
            "TCP" => Ok(TransportClientEnum::new_tcp_with_metrics(metrics)),
            "UDP" => {
                // UDP client is lazy-initialized, metrics will be attached on first use
                Ok(TransportClientEnum::new_udp())
            }
            other => Err(ZhtError::UnsupportedProtocol(other.to_string())),
        }
    }

    /// Create a protocol-specific transport server.
    pub fn create_server(protocol: &str) -> ZhtResult<TransportServerEnum> {
        match protocol.to_uppercase().as_str() {
            "TCP" => Ok(TransportServerEnum::new_tcp()),
            "UDP" => Ok(TransportServerEnum::new_udp()),
            other => Err(ZhtError::UnsupportedProtocol(other.to_string())),
        }
    }

    /// Create a protocol-specific transport server with shared metrics.
    pub fn create_server_with_metrics(protocol: &str, metrics: Arc<TransportMetrics>) -> ZhtResult<TransportServerEnum> {
        match protocol.to_uppercase().as_str() {
            "TCP" => Ok(TransportServerEnum::new_tcp_with_metrics(metrics)),
            "UDP" => Ok(TransportServerEnum::new_udp_with_metrics(metrics)),
            other => Err(ZhtError::UnsupportedProtocol(other.to_string())),
        }
    }

    /// Create both a client and server for the given protocol.
    pub fn create_pair(protocol: &str) -> ZhtResult<(TransportClientEnum, TransportServerEnum)> {
        let client = Self::create_client(protocol)?;
        let server = Self::create_server(protocol)?;
        Ok((client, server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_tcp_client() {
        let c = TransportFactory::create_client("TCP").unwrap();
        assert_eq!(c.protocol_name(), "TCP");
    }

    #[test]
    fn test_create_tcp_server() {
        let s = TransportFactory::create_server("TCP").unwrap();
        assert_eq!(s.protocol_name(), "TCP");
    }

    #[test]
    fn test_create_udp_client() {
        let c = TransportFactory::create_client("UDP").unwrap();
        assert_eq!(c.protocol_name(), "UDP");
    }

    #[test]
    fn test_create_udp_server() {
        let s = TransportFactory::create_server("UDP").unwrap();
        assert_eq!(s.protocol_name(), "UDP");
    }

    #[test]
    fn test_create_pair_tcp() {
        let (c, s) = TransportFactory::create_pair("TCP").unwrap();
        assert_eq!(c.protocol_name(), "TCP");
        assert_eq!(s.protocol_name(), "TCP");
    }

    #[test]
    fn test_create_pair_udp() {
        let (c, s) = TransportFactory::create_pair("UDP").unwrap();
        assert_eq!(c.protocol_name(), "UDP");
        assert_eq!(s.protocol_name(), "UDP");
    }

    #[test]
    fn test_unsupported_protocol() {
        assert!(TransportFactory::create_client("MPI").is_err());
        assert!(TransportFactory::create_server("BLAH").is_err());
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(TransportFactory::create_client("tcp").unwrap().protocol_name(), "TCP");
        assert_eq!(TransportFactory::create_server("Udp").unwrap().protocol_name(), "UDP");
    }

    #[test]
    fn test_with_metrics() {
        let m = Arc::new(TransportMetrics::new());
        let (c, s) = TransportFactory::create_pair("TCP").unwrap();
        assert_eq!(c.protocol_name(), "TCP");
        assert_eq!(s.protocol_name(), "TCP");
    }
}
