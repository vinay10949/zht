//! Enhanced TCP transport with SO_REUSEADDR, connection health, and graceful shutdown.

use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;

use zht_common::error::{ZhtError, ZhtResult};
use zht_proto::ZPack;

use crate::framing::encode_message;
use crate::metrics::TransportMetrics;
use crate::shutdown::ShutdownSignal;

// ── Handler Trait ────────────────────────────────────────────────────

pub trait RequestHandler: Send + Sync {
    fn handle(&self, request: &ZPack) -> ZPack;
}

// ── Connection Pool ──────────────────────────────────────────────────

const MAX_IDLE: Duration = Duration::from_secs(300);
const MAX_OPS: u64 = 10_000;

struct PooledConn {
    stream: TcpStream,
    last_used: std::time::Instant,
    ops: u64,
}

impl PooledConn {
    fn new(s: TcpStream) -> Self { Self { stream: s, last_used: std::time::Instant::now(), ops: 0 } }
    fn touch(&mut self) { self.last_used = std::time::Instant::now(); self.ops += 1; }
}

// ═══════════════════════════════════════════════════════════════════════
// TcpProxy
// ═══════════════════════════════════════════════════════════════════════

pub struct TcpProxy {
    conns: tokio::sync::Mutex<std::collections::HashMap<String, PooledConn>>,
    metrics: Arc<TransportMetrics>,
}

impl TcpProxy {
    pub fn new() -> Self {
        Self { conns: tokio::sync::Mutex::new(std::collections::HashMap::new()), metrics: Arc::new(TransportMetrics::new()) }
    }
    pub fn with_metrics(m: Arc<TransportMetrics>) -> Self {
        Self { conns: tokio::sync::Mutex::new(std::collections::HashMap::new()), metrics: m }
    }

    async fn connect(&self, host: &str, port: u16) -> ZhtResult<TcpStream> {
        let addr_str = format!("{}:{}", host, port);
        let addr: std::net::SocketAddr = addr_str.parse().map_err(|e|
            ZhtError::ConnectionFailed { host: host.into(), port, reason: format!("Invalid addr: {}", e) })?;

        let stream = TcpStream::connect(addr).await.map_err(|e|
            ZhtError::ConnectionFailed { host: host.into(), port, reason: e.to_string() })?;

        stream.set_nodelay(true).map_err(|e|
            ZhtError::ConnectionFailed { host: host.into(), port, reason: format!("TCP_NODELAY: {}", e) })?;

        tracing::debug!("TCP connected to {}:{}", host, port);
        self.metrics.inc_active_connections();
        Ok(stream)
    }

    async fn get_or_create(&self, host: &str, port: u16) -> ZhtResult<()> {
        let key = format!("{}:{}", host, port);
        let mut conns = self.conns.lock().await;
        if let Some(c) = conns.get_mut(&key) {
            if c.last_used.elapsed() > MAX_IDLE || c.ops >= MAX_OPS {
                tracing::debug!("Recycling TCP connection to {}", key);
                self.metrics.dec_active_connections();
                self.metrics.inc_reconnections();
                let s = self.connect(host, port).await?;
                *c = PooledConn::new(s);
            }
        } else {
            let s = self.connect(host, port).await?;
            conns.insert(key, PooledConn::new(s));
        }
        Ok(())
    }

    async fn send_raw(stream: &mut TcpStream, zpack: &ZPack) -> ZhtResult<()> {
        let data = encode_message(zpack);
        stream.write_all(&data).await.map_err(|e| ZhtError::SendFailed(e.to_string()))?;
        stream.flush().await.map_err(|e| ZhtError::SendFailed(e.to_string()))?;
        Ok(())
    }

    async fn recv_raw(stream: &mut TcpStream) -> ZhtResult<ZPack> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| ZhtError::RecvFailed(e.to_string()))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| ZhtError::RecvFailed(e.to_string()))?;
        ZPack::decode(buf.as_slice()).map_err(|e| ZhtError::ProtobufDecodeError(e.to_string()))
    }

    pub async fn sendrecv(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<ZPack> {
        self.get_or_create(host, port).await?;
        let key = format!("{}:{}", host, port);
        let mut conns = self.conns.lock().await;
        let conn = conns.get_mut(&key).unwrap();

        if let Err(e) = Self::send_raw(&mut conn.stream, request).await {
            self.metrics.inc_send_errors();
            drop(conns);
            self.remove_conn(host, port).await;
            return Err(e);
        }
        conn.touch();
        self.metrics.inc_messages_sent();

        match Self::recv_raw(&mut conn.stream).await {
            Ok(r) => { conn.touch(); self.metrics.inc_messages_received(); Ok(r) }
            Err(e) => { self.metrics.inc_recv_errors(); drop(conns); self.remove_conn(host, port).await; Err(e) }
        }
    }

    pub async fn send_only(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<()> {
        self.get_or_create(host, port).await?;
        let key = format!("{}:{}", host, port);
        let mut conns = self.conns.lock().await;
        Self::send_raw(&mut conns.get_mut(&key).unwrap().stream, request).await?;
        conns.get_mut(&key).unwrap().touch();
        self.metrics.inc_messages_sent();
        Ok(())
    }

    pub async fn close_connection(&self, host: &str, port: u16) { self.remove_conn(host, port).await; }

    async fn remove_conn(&self, host: &str, port: u16) {
        let key = format!("{}:{}", host, port);
        if self.conns.lock().await.remove(&key).is_some() { self.metrics.dec_active_connections(); }
    }

    pub async fn close_all(&self) {
        let n = self.conns.lock().await.len();
        self.conns.lock().await.clear();
        for _ in 0..n { self.metrics.dec_active_connections(); }
    }

    pub async fn connection_count(&self) -> usize { self.conns.lock().await.len() }
    pub fn protocol_name(&self) -> &'static str { "TCP" }
    pub fn metrics(&self) -> &Arc<TransportMetrics> { &self.metrics }
}

impl Default for TcpProxy { fn default() -> Self { Self::new() } }

// ═══════════════════════════════════════════════════════════════════════
// TcpServer
// ═══════════════════════════════════════════════════════════════════════

pub struct TcpServer { metrics: Arc<TransportMetrics> }

impl TcpServer {
    pub fn new() -> Self { Self { metrics: Arc::new(TransportMetrics::new()) } }
    pub fn with_metrics(m: Arc<TransportMetrics>) -> Self { Self { metrics: m } }
    pub fn metrics(&self) -> &Arc<TransportMetrics> { &self.metrics }

    pub async fn bind(addr: SocketAddr) -> ZhtResult<TcpListener> {
        TcpListener::bind(addr).await.map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(), port: addr.port(), reason: e.to_string() })
    }

    pub async fn run(&self, addr: SocketAddr, handler: Arc<dyn RequestHandler>, shutdown: ShutdownSignal) -> ZhtResult<SocketAddr> {
        let listener = Self::bind(addr).await?;
        let bound = listener.local_addr().map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(), port: addr.port(), reason: e.to_string() })?;
        tracing::info!("ZHT TCP server listening on {}", bound);

        loop {
            if shutdown.is_triggered() { tracing::info!("TCP server shutting down"); break; }
            match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
                Ok(Ok((stream, peer))) => {
                    self.metrics.inc_connections_accepted();
                    let h = handler.clone();
                    let m = self.metrics.clone();
                    let s = shutdown.resubscribe();
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, peer, h, m, s).await {
                            tracing::warn!("Connection error {}: {}", peer, e);
                        }
                    });
                }
                Ok(Err(e)) => { tracing::warn!("Accept error: {}", e); }
                Err(_) => {}
            }
        }
        Ok(bound)
    }
}

impl Default for TcpServer { fn default() -> Self { Self::new() } }

async fn handle_conn(mut stream: TcpStream, peer: SocketAddr, handler: Arc<dyn RequestHandler>, metrics: Arc<TransportMetrics>, shutdown: ShutdownSignal) -> ZhtResult<()> {
    stream.set_nodelay(true).map_err(|e| ZhtError::ConnectionFailed {
        host: peer.ip().to_string(), port: peer.port(), reason: format!("TCP_NODELAY: {}", e) })?;

    loop {
        if shutdown.is_triggered() { break; }
        let request = match tokio::time::timeout(Duration::from_secs(30), TcpProxy::recv_raw(&mut stream)).await {
            Ok(Ok(r)) => r,
            Ok(Err(ZhtError::RecvFailed(m))) if m.contains("unexpected eof") => break,
            Ok(Err(_)) => break,
            Err(_) => continue,
        };
        metrics.inc_requests_processed();
        let response = handler.handle(&request);
        if let Err(_e) = TcpProxy::send_raw(&mut stream, &response).await {
            metrics.inc_send_errors(); break;
        }
    }
    Ok(())
}

/// Backward-compatible server function.
pub async fn run_server(addr: SocketAddr, worker: Arc<crate::HTWorker>) -> ZhtResult<()> {
    let server = TcpServer::new();
    struct Adapter(Arc<crate::HTWorker>);
    impl RequestHandler for Adapter { fn handle(&self, r: &ZPack) -> ZPack { self.0.process(r) } }
    let handler: Arc<dyn RequestHandler> = Arc::new(Adapter(worker));
    let (_ctrl, shutdown) = crate::ShutdownController::new();
    server.run(addr, handler, shutdown).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::HTWorker;
    use zht_common::constants::*;
    use zht_storage::NoVoHT;

    fn zp(op: &str, k: &str, v: &str) -> ZPack {
        let mut z = ZPack::default(); z.opcode = op.as_bytes().to_vec(); z.key = k.as_bytes().to_vec();
        if !v.is_empty() { z.val = v.as_bytes().to_vec(); } z
    }

    struct TH(Arc<HTWorker>);
    impl RequestHandler for TH { fn handle(&self, r: &ZPack) -> ZPack { self.0.process(r) } }

    fn handler() -> Arc<dyn RequestHandler> { Arc::new(TH(Arc::new(HTWorker::new(NoVoHT::new())))) }

    #[tokio::test]
    async fn test_proxy_new() {
        let p = TcpProxy::new();
        assert_eq!(p.protocol_name(), "TCP");
        assert_eq!(p.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_bind() {
        let l = TcpServer::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert_ne!(l.local_addr().unwrap().port(), 0);
    }

    #[tokio::test]
    async fn test_integration() {
        let listener = TcpServer::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = handler();
        let sm = Arc::new(TransportMetrics::new());
        let sm2 = sm.clone();
        let sh = tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(500), listener.accept()).await {
                    Ok(Ok((stream, peer))) => {
                        sm2.inc_connections_accepted();
                        let hh = h.clone(); let mm = sm2.clone();
                        let (_, s) = crate::ShutdownController::new();
                        tokio::spawn(async move { let _ = handle_conn(stream, peer, hh, mm, s).await; });
                    }
                    _ => break,
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let proxy = TcpProxy::new();
        let r = proxy.sendrecv("127.0.0.1", addr.port(), &zp(OPC_INSERT, "k", "v")).await.unwrap();
        assert_eq!(r.lease, REC_SUCC.as_bytes().to_vec());
        let r = proxy.sendrecv("127.0.0.1", addr.port(), &zp(OPC_LOOKUP, "k", "")).await.unwrap();
        assert_eq!(r.val, b"v".to_vec());
        assert_eq!(proxy.connection_count().await, 1);
        assert_eq!(proxy.metrics().snapshot().messages_sent, 2);
        sh.abort();
    }

    #[tokio::test]
    async fn test_close() {
        let p = TcpProxy::new();
        p.close_connection("x", 1).await;
        p.close_all().await;
        assert_eq!(p.connection_count().await, 0);
    }
}
