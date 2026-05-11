//! Transport abstraction types for protocol-agnostic client/server communication.
//!
//! Uses an enum-based dispatch pattern for async compatibility.

use zht_common::error::ZhtResult;
use zht_proto::ZPack;

use crate::metrics::TransportMetrics;
use crate::tcp::{TcpProxy, TcpServer};
use crate::udp::UdpProxy;
use crate::udp::UdpServer;
use crate::HTWorker;

/// Callback trait for processing incoming ZHT requests.
pub trait RequestHandler: Send + Sync {
    fn handle(&self, request: &ZPack) -> ZPack;
}

/// Adapter that wraps an [`HTWorker`] as a [`RequestHandler`].
pub struct WorkerHandler {
    worker: std::sync::Arc<HTWorker>,
}

impl WorkerHandler {
    pub fn new(worker: std::sync::Arc<HTWorker>) -> Self { Self { worker } }
}

impl RequestHandler for WorkerHandler {
    fn handle(&self, request: &ZPack) -> ZPack { self.worker.process(request) }
}

// ── Enum-based Client Dispatch ────────────────────────────────────────

/// Protocol-agnostic client transport.
pub enum TransportClientEnum {
    Tcp(TcpProxy),
    Udp(std::sync::Mutex<Option<UdpProxy>>),
}

impl TransportClientEnum {
    pub fn new_tcp() -> Self { Self::Tcp(TcpProxy::new()) }
    pub fn new_tcp_with_metrics(metrics: std::sync::Arc<TransportMetrics>) -> Self {
        Self::Tcp(TcpProxy::with_metrics(metrics))
    }
    pub fn new_udp() -> Self { Self::Udp(std::sync::Mutex::new(None)) }

    pub async fn sendrecv(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<ZPack> {
        match self {
            Self::Tcp(p) => p.sendrecv(host, port, request).await,
            Self::Udp(opt) => {
                let mut g = opt.lock().unwrap();
                if g.is_none() { *g = Some(UdpProxy::new().await?); }
                g.as_ref().unwrap().sendrecv(host, port, request).await
            }
        }
    }

    pub async fn send_only(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<()> {
        match self {
            Self::Tcp(p) => p.send_only(host, port, request).await,
            Self::Udp(opt) => {
                let mut g = opt.lock().unwrap();
                if g.is_none() { *g = Some(UdpProxy::new().await?); }
                g.as_ref().unwrap().send_only(host, port, request).await
            }
        }
    }

    pub async fn close_connection(&self, host: &str, port: u16) {
        if let Self::Tcp(p) = self { p.close_connection(host, port).await; }
    }

    pub async fn close_all(&self) {
        if let Self::Tcp(p) = self { p.close_all().await; }
    }

    pub async fn connection_count(&self) -> usize {
        match self { Self::Tcp(p) => p.connection_count().await, Self::Udp(_) => 0 }
    }

    pub fn protocol_name(&self) -> &'static str {
        match self { Self::Tcp(_) => "TCP", Self::Udp(_) => "UDP" }
    }
}

// ── Enum-based Server Dispatch ────────────────────────────────────────

/// Protocol-agnostic server transport.
pub enum TransportServerEnum {
    Tcp(TcpServer),
    Udp(UdpServer),
}

impl TransportServerEnum {
    pub fn new_tcp() -> Self { Self::Tcp(TcpServer::new()) }
    pub fn new_tcp_with_metrics(metrics: std::sync::Arc<TransportMetrics>) -> Self {
        Self::Tcp(TcpServer::with_metrics(metrics))
    }
    pub fn new_udp() -> Self { Self::Udp(UdpServer::new()) }
    pub fn new_udp_with_metrics(metrics: std::sync::Arc<TransportMetrics>) -> Self {
        Self::Udp(UdpServer::with_metrics(metrics))
    }

    pub fn protocol_name(&self) -> &'static str {
        match self { Self::Tcp(_) => "TCP", Self::Udp(_) => "UDP" }
    }

    pub fn metrics(&self) -> Option<&std::sync::Arc<TransportMetrics>> {
        match self { Self::Tcp(s) => Some(s.metrics()), Self::Udp(s) => Some(s.metrics()) }
    }
}
