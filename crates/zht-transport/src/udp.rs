//! UDP transport for ZHT client-server communication.

use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::time::{timeout, Duration};

use prost::Message;

use zht_common::error::{ZhtError, ZhtResult};
use zht_proto::ZPack;

use crate::framing::{decode_message, encode_message};
use crate::chunking::{chunk_message, ChunkReceiver, CHUNK_PAYLOAD_SIZE, DEFAULT_CHUNK_THRESHOLD};
use crate::metrics::TransportMetrics;
use crate::shutdown::ShutdownSignal;

const UDP_MAX_DATAGRAM: usize = 65_507;
const UDP_RECV_TIMEOUT: Duration = Duration::from_secs(5);

// ── Handler Trait ────────────────────────────────────────────────────

/// Callback trait for processing incoming requests (UDP server).
pub trait RequestHandler: Send + Sync {
    fn handle(&self, request: &ZPack) -> ZPack;
}

// ═══════════════════════════════════════════════════════════════════════
// UdpProxy
// ═══════════════════════════════════════════════════════════════════════

/// UDP client proxy for sending requests to ZHT server nodes.
pub struct UdpProxy {
    socket: tokio::sync::Mutex<Arc<UdpSocket>>,
    metrics: Arc<TransportMetrics>,
}

impl UdpProxy {
    pub async fn new() -> ZhtResult<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|e| ZhtError::ConnectionFailed {
            host: "0.0.0.0".to_string(), port: 0, reason: e.to_string(),
        })?;
        Ok(Self { socket: tokio::sync::Mutex::new(Arc::new(socket)), metrics: Arc::new(TransportMetrics::new()) })
    }

    pub async fn with_metrics(metrics: Arc<TransportMetrics>) -> ZhtResult<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await.map_err(|e| ZhtError::ConnectionFailed {
            host: "0.0.0.0".to_string(), port: 0, reason: e.to_string(),
        })?;
        Ok(Self { socket: tokio::sync::Mutex::new(Arc::new(socket)), metrics })
    }

    pub async fn sendrecv(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<ZPack> {
        let server_addr: std::net::SocketAddr = format!("{}:{}", host, port).parse().map_err(|e| {
            ZhtError::ConnectionFailed { host: host.to_string(), port, reason: format!("Invalid address: {}", e) }
        })?;

        if request.encoded_len() > DEFAULT_CHUNK_THRESHOLD {
            self.sendrecv_chunked(server_addr, request).await
        } else {
            self.sendrecv_framed(server_addr, request).await
        }
    }

    async fn sendrecv_framed(&self, server_addr: std::net::SocketAddr, request: &ZPack) -> ZhtResult<ZPack> {
        let frame = encode_message(request);
        let sock = self.socket.lock().await;
        sock.send_to(&frame, server_addr).await.map_err(|e| ZhtError::SendFailed(e.to_string()))?;
        self.metrics.add_bytes_sent(frame.len() as u64);
        self.metrics.inc_messages_sent();

        let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
        match timeout(UDP_RECV_TIMEOUT, sock.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => {
                self.metrics.add_bytes_received(len as u64);
                self.metrics.inc_messages_received();
                let (zpack, _) = decode_message(&buf[..len])?;
                Ok(zpack)
            }
            Ok(Err(e)) => { self.metrics.inc_recv_errors(); Err(ZhtError::RecvFailed(e.to_string())) }
            Err(_) => { self.metrics.inc_recv_errors(); Err(ZhtError::RecvFailed("UDP timeout".into())) }
        }
    }

    async fn sendrecv_chunked(&self, server_addr: std::net::SocketAddr, request: &ZPack) -> ZhtResult<ZPack> {
        let data = request.encode_to_vec();
        let (_, chunks) = chunk_message(&data, CHUNK_PAYLOAD_SIZE);
        let sock = self.socket.lock().await;

        for chunk in &chunks {
            sock.send_to(&chunk.data, server_addr).await.map_err(|e| ZhtError::SendFailed(e.to_string()))?;
            self.metrics.add_bytes_sent(chunk.data.len() as u64);
            self.metrics.inc_chunks_sent();
        }
        self.metrics.inc_messages_sent();

        let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
        match timeout(UDP_RECV_TIMEOUT, sock.recv_from(&mut buf)).await {
            Ok(Ok((len, _))) => {
                self.metrics.add_bytes_received(len as u64);
                self.metrics.inc_messages_received();
                let (zpack, _) = decode_message(&buf[..len])?;
                Ok(zpack)
            }
            Ok(Err(e)) => { self.metrics.inc_recv_errors(); Err(ZhtError::RecvFailed(e.to_string())) }
            Err(_) => { self.metrics.inc_recv_errors(); Err(ZhtError::RecvFailed("UDP timeout (chunked)".into())) }
        }
    }

    pub async fn send_only(&self, host: &str, port: u16, request: &ZPack) -> ZhtResult<()> {
        let server_addr: std::net::SocketAddr = format!("{}:{}", host, port).parse().map_err(|e| {
            ZhtError::ConnectionFailed { host: host.to_string(), port, reason: format!("Invalid address: {}", e) }
        })?;
        let frame = encode_message(request);
        let sock = self.socket.lock().await;
        sock.send_to(&frame, server_addr).await.map_err(|e| ZhtError::SendFailed(e.to_string()))?;
        self.metrics.add_bytes_sent(frame.len() as u64);
        self.metrics.inc_messages_sent();
        Ok(())
    }

    pub async fn close_connection(&self, _host: &str, _port: u16) {}
    pub async fn close_all(&self) {}
    pub async fn connection_count(&self) -> usize { 0 }
    pub fn protocol_name(&self) -> &'static str { "UDP" }
    pub fn metrics(&self) -> &Arc<TransportMetrics> { &self.metrics }
}

// ═══════════════════════════════════════════════════════════════════════
// UdpServer
// ═══════════════════════════════════════════════════════════════════════

/// UDP server that receives datagrams and dispatches them to a handler.
pub struct UdpServer {
    metrics: Arc<TransportMetrics>,
}

impl UdpServer {
    pub fn new() -> Self { Self { metrics: Arc::new(TransportMetrics::new()) } }
    pub fn with_metrics(metrics: Arc<TransportMetrics>) -> Self { Self { metrics } }
    pub fn metrics(&self) -> &Arc<TransportMetrics> { &self.metrics }

    pub async fn run(
        &self, addr: std::net::SocketAddr,
        handler: Box<dyn RequestHandler>,
        shutdown: ShutdownSignal,
    ) -> ZhtResult<std::net::SocketAddr> {
        let socket = UdpSocket::bind(addr).await.map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(), port: addr.port(), reason: e.to_string(),
        })?;
        let bound_addr = socket.local_addr().map_err(|e| ZhtError::ConnectionFailed {
            host: addr.ip().to_string(), port: addr.port(), reason: e.to_string(),
        })?;
        tracing::info!("ZHT UDP server listening on {}", bound_addr);

        let handler = Arc::from(handler) as Arc<dyn RequestHandler>;
        let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
        let mut chunk_receiver = ChunkReceiver::new();

        loop {
            if shutdown.is_triggered() { break; }

            match tokio::time::timeout(Duration::from_millis(100), socket.recv_from(&mut buf)).await {
                Ok(Ok((len, src_addr))) => {
                    self.metrics.add_bytes_received(len as u64);

                    match decode_message(&buf[..len]) {
                        Ok((request, _)) => {
                            self.metrics.inc_requests_processed();
                            let response = handler.handle(&request);
                            let frame = encode_message(&response);
                            if let Err(_e) = socket.send_to(&frame, src_addr).await {
                                self.metrics.inc_send_errors();
                            } else {
                                self.metrics.add_bytes_sent(frame.len() as u64);
                            }
                        }
                        Err(_) => {
                            match chunk_receiver.receive(&buf[..len]) {
                                Ok(Some(assembled)) => {
                                    if let Ok((request, _)) = decode_message(&assembled) {
                                        self.metrics.inc_requests_processed();
                                        let response = handler.handle(&request);
                                        let frame = encode_message(&response);
                                        if let Err(_e) = socket.send_to(&frame, src_addr).await {
                                            self.metrics.inc_send_errors();
                                        } else {
                                            self.metrics.add_bytes_sent(frame.len() as u64);
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => { tracing::warn!("Chunk parse error: {}", e); }
                            }
                        }
                    }
                }
                Ok(Err(e)) => { tracing::warn!("UDP recv error: {}", e); self.metrics.inc_recv_errors(); }
                Err(_) => {}
            }
        }
        Ok(bound_addr)
    }

    pub fn protocol_name(&self) -> &'static str { "UDP" }
}

impl Default for UdpServer {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::HTWorker;
    use zht_common::constants::*;
    use zht_storage::NoVoHT;

    fn make_zpack(opcode: &str, key: &str, val: &str) -> ZPack {
        let mut z = ZPack::default();
        z.opcode = opcode.as_bytes().to_vec();
        z.key = key.as_bytes().to_vec();
        if !val.is_empty() { z.val = val.as_bytes().to_vec(); }
        z
    }

    struct TestHandler(Arc<HTWorker>);
    impl RequestHandler for TestHandler {
        fn handle(&self, req: &ZPack) -> ZPack { self.0.process(req) }
    }

    #[tokio::test]
    async fn test_udp_proxy_creation() {
        let p = UdpProxy::new().await.unwrap();
        assert_eq!(p.protocol_name(), "UDP");
        assert_eq!(p.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_udp_roundtrip() {
        let server_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_socket.local_addr().unwrap();
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let ss = server_socket.clone();

        let sh = tokio::spawn(async move {
            let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
            loop {
                match tokio::time::timeout(Duration::from_millis(500), ss.recv_from(&mut buf)).await {
                    Ok(Ok((len, src))) => {
                        if let Ok((req, _)) = decode_message(&buf[..len]) {
                            let resp = worker.process(&req);
                            let frame = encode_message(&resp);
                            let _ = ss.send_to(&frame, src).await;
                        }
                    }
                    _ => break,
                }
            }
        });

        let proxy = UdpProxy::new().await.unwrap();
        let resp = proxy.sendrecv("127.0.0.1", server_addr.port(), &make_zpack(OPC_INSERT, "uk", "uv")).await.unwrap();
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        let resp = proxy.sendrecv("127.0.0.1", server_addr.port(), &make_zpack(OPC_LOOKUP, "uk", "")).await.unwrap();
        assert_eq!(resp.val, b"uv".to_vec());

        let snap = proxy.metrics().snapshot();
        assert_eq!(snap.messages_sent, 2);

        sh.abort();
    }

    #[tokio::test]
    async fn test_udp_multiple_clients() {
        let server_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_socket.local_addr().unwrap();
        let store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(store));
        let ss = server_socket.clone();

        let sh = tokio::spawn(async move {
            let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
            loop {
                match tokio::time::timeout(Duration::from_millis(500), ss.recv_from(&mut buf)).await {
                    Ok(Ok((len, src))) => {
                        if let Ok((req, _)) = decode_message(&buf[..len]) {
                            let resp = worker.process(&req);
                            let frame = encode_message(&resp);
                            let _ = ss.send_to(&frame, src).await;
                        }
                    }
                    _ => break,
                }
            }
        });

        let mut handles = Vec::new();
        for i in 0..5 {
            handles.push(tokio::spawn(async move {
                let proxy = UdpProxy::new().await.unwrap();
                let req = make_zpack(OPC_INSERT, &format!("mu_{}", i), &format!("mv_{}", i));
                let resp = proxy.sendrecv("127.0.0.1", server_addr.port(), &req).await.unwrap();
                assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());
            }));
        }
        for h in handles { h.await.unwrap(); }
        sh.abort();
    }

    #[tokio::test]
    async fn test_udp_fire_and_forget() {
        let server_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_socket.local_addr().unwrap();
        let ss = server_socket.clone();
        let sh = tokio::spawn(async move {
            let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
            loop {
                match tokio::time::timeout(Duration::from_millis(300), ss.recv_from(&mut buf)).await {
                    Ok(Ok(_)) => {}
                    _ => break,
                }
            }
        });

        let proxy = UdpProxy::new().await.unwrap();
        proxy.send_only("127.0.0.1", server_addr.port(), &make_zpack(OPC_INSERT, "ff", "v")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = proxy.metrics().snapshot();
        assert_eq!(snap.messages_sent, 1);
        assert_eq!(snap.messages_received, 0);

        sh.abort();
    }

    #[tokio::test]
    async fn test_udp_close_noop() {
        let proxy = UdpProxy::new().await.unwrap();
        proxy.close_connection("localhost", 9999).await;
        proxy.close_all().await;
        assert_eq!(proxy.connection_count().await, 0);
    }
}
