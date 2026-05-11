//! Graceful shutdown support for transport servers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast;

/// A signal that server tasks can listen on to initiate graceful shutdown.
///
/// Uses a shared atomic flag for non-blocking checks and a broadcast
/// channel for async waiting.
pub struct ShutdownSignal {
    triggered: Arc<AtomicBool>,
    rx: broadcast::Receiver<()>,
}

impl std::fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("triggered", &self.triggered.load(Ordering::Relaxed))
            .finish()
    }
}

impl ShutdownSignal {
    /// Check whether the shutdown signal has been triggered (non-blocking).
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Relaxed)
    }

    /// Wait until the shutdown signal is triggered.
    pub async fn recv(&self) {
        if self.is_triggered() { return; }
        let _ = self.rx.resubscribe().recv().await;
    }

    /// Create a new, independent receiver from the underlying channel.
    pub fn resubscribe(&self) -> Self {
        Self {
            triggered: self.triggered.clone(),
            rx: self.rx.resubscribe(),
        }
    }
}

/// Controller side of the graceful shutdown mechanism.
#[derive(Debug, Clone)]
pub struct ShutdownController {
    triggered: Arc<AtomicBool>,
    tx: broadcast::Sender<()>,
}

impl ShutdownController {
    /// Create a new controller and an initial shutdown signal.
    pub fn new() -> (Self, ShutdownSignal) {
        let (tx, rx) = broadcast::channel(1);
        let triggered = Arc::new(AtomicBool::new(false));
        let signal = ShutdownSignal { triggered: triggered.clone(), rx };
        (Self { triggered, tx }, signal)
    }

    /// Trigger the shutdown signal, notifying all listeners.
    pub fn shutdown(&self) -> usize {
        self.triggered.store(true, Ordering::Relaxed);
        self.tx.send(()).unwrap_or(0)
    }
}

impl Default for ShutdownController {
    fn default() -> Self { Self::new().0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_not_triggered_initially() {
        let (_ctrl, sig) = ShutdownController::new();
        assert!(!sig.is_triggered());
    }

    #[tokio::test]
    async fn test_trigger() {
        let (ctrl, sig) = ShutdownController::new();
        ctrl.shutdown();
        assert!(sig.is_triggered());
    }

    #[tokio::test]
    async fn test_recv_waits() {
        let (ctrl, sig) = ShutdownController::new();
        let h = tokio::spawn(async move { sig.recv().await; true });
        tokio::time::sleep(Duration::from_millis(10)).await;
        ctrl.shutdown();
        assert!(h.await.unwrap());
    }

    #[tokio::test]
    async fn test_resubscribe() {
        let (ctrl, sig) = ShutdownController::new();
        let sig2 = sig.resubscribe();
        ctrl.shutdown();
        assert!(sig.is_triggered());
        assert!(sig2.is_triggered());
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let (ctrl, sig) = ShutdownController::new();
        let s2 = sig.resubscribe();
        let s3 = sig.resubscribe();
        let h1 = tokio::spawn(async move { sig.recv().await; });
        let h2 = tokio::spawn(async move { s2.recv().await; });
        let h3 = tokio::spawn(async move { s3.recv().await; });
        tokio::time::sleep(Duration::from_millis(10)).await;
        ctrl.shutdown();
        h1.await.unwrap(); h2.await.unwrap(); h3.await.unwrap();
    }

    #[tokio::test]
    async fn test_idempotent() {
        let (ctrl, sig) = ShutdownController::new();
        assert!(ctrl.shutdown() >= 1);
        ctrl.shutdown();
        assert!(sig.is_triggered());
    }

    use tokio::time::Duration;
}
