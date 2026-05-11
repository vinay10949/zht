//! ZHT Server — Zero-Hop Distributed Hash Table server binary.
//!
//! This is the main entry point for the ZHT server binary.
//! It loads configuration, initializes all server components
//! (membership, replication, migration, metrics), and starts
//! the TCP server with graceful shutdown support.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use zht_common::constants::*;
use zht_common::HostEntity;
use zht_config::AppConfig;
use zht_storage::NoVoHT;
use zht_server::ZHTServer;

/// ZHT Server — Zero-Hop Distributed Hash Table
#[derive(Parser, Debug)]
#[command(
    name = "zhtserver",
    version,
    about = "ZHT (Zero-Hop Distributed Hash Table) server",
    long_about = None
)]
struct Args {
    /// Path to the ZHT configuration file (zht.conf).
    #[arg(short, long, default_value = "configs/zht.conf")]
    zht_conf: PathBuf,

    /// Path to the neighbor configuration file (neighbor.conf).
    #[arg(short, long, default_value = "configs/neighbor.conf")]
    neighbor_conf: PathBuf,

    /// Server port (overrides zht.conf).
    #[arg(short, long)]
    port: Option<u16>,

    /// Path to the NoVoHT persistence file (optional).
    #[arg(short, long)]
    db_file: Option<String>,

    /// Enable verbose (debug-level) logging.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let level = if args.verbose { Level::DEBUG } else { Level::INFO };
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!("ZHT Server starting...");
    info!("ZHT config: {}", args.zht_conf.display());
    info!("Neighbor config: {}", args.neighbor_conf.display());

    // Load configuration
    let config = AppConfig::load(&args.zht_conf, &args.neighbor_conf)
        .context("Failed to load configuration")?;

    // Print configuration summary
    info!("Protocol: {}", config.zht.protocol());
    info!(
        "Port: {}{}",
        config.zht.port(),
        args.port
            .map(|p| format!(" (overridden to {})", p))
            .unwrap_or_default()
    );
    info!("Max message size: {} bytes", config.zht.msg_maxsize());
    info!("SCCB poll interval: {} ms", config.zht.sccb_poll_interval());
    info!("Instant swap: {}", config.zht.instant_swap());
    info!("Cluster nodes: {}", config.node_count());
    info!(
        "Replication: replicas={}, type={}",
        config.zht.num_replicas(),
        config.zht.replication_type()
    );

    for (i, node) in config.neighbors.neighbors.iter().enumerate() {
        info!("  Node {}: {}:{}", i, node.host, node.port);
    }

    // Validate configuration
    let protocol = config.zht.protocol();
    if protocol != PROTO_VAL_TCP
        && protocol != PROTO_VAL_UDP
        && protocol != PROTO_VAL_MPI
    {
        anyhow::bail!(
            "Unsupported protocol '{}'. Must be one of: TCP, UDP, MPI",
            protocol
        );
    }

    if config.node_count() == 0 {
        warn!("No cluster nodes configured — server will start but may not be reachable");
    }

    let effective_port = args.port.unwrap_or_else(|| config.zht.port());

    // Initialize storage engine
    let store = if let Some(ref db_path) = args.db_file {
        info!("Loading persistent storage from: {}", db_path);
        NoVoHT::with_persistence(db_path)?
    } else {
        info!("Using in-memory storage (no persistence)");
        NoVoHT::new()
    };
    info!("Storage engine initialized");

    // Create the self host entity
    let self_host = HostEntity::new("0.0.0.0", effective_port);
    let initial_members: Vec<HostEntity> = config
        .neighbors
        .neighbors
        .iter()
        .filter(|n| **n != self_host)
        .cloned()
        .collect();

    // Create the ZHTServer with all components
    let server = Arc::new(ZHTServer::new_with_store(
        store,
        self_host,
        initial_members,
        config.zht.num_replicas(),
        config.zht.replication_type(),
        true, // migration enabled
    ));

    // Spawn signal handler (ctrl+c → cancel)
    let cancel_token = server.cancel_token.clone();
    let signal_handle = tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install ctrl+c handler");
        info!("Ctrl+C received, initiating graceful shutdown...");
        cancel_token.cancel();
    });

    // Spawn periodic membership health checker (every 30 seconds)
    let health_handle = server.spawn_health_checker(
        Duration::from_secs(30),
        Duration::from_secs(60), // heartbeat timeout
    );

    // Bind and start TCP server
    let addr = format!("0.0.0.0:{}", effective_port)
        .parse::<std::net::SocketAddr>()
        .context("Invalid bind address")?;

    info!("Starting TCP server on {}", addr);
    info!("Ready to accept connections — press Ctrl+C to stop");

    // Start server (blocks until cancel token is triggered)
    match server.start(&addr.to_string()).await {
        Ok(()) => info!("Server accept loop exited cleanly"),
        Err(e) => warn!("Server error: {}", e),
    }

    // Abort background tasks
    signal_handle.abort();
    health_handle.abort();

    // Log final metrics
    let final_status = server.status();
    info!("=== Server Shutdown Summary ===");
    info!("Uptime: {:.1}s", final_status.uptime_secs);
    info!(
        "Connections: total={}, accepted={}, rejected={}",
        final_status.connections_total,
        final_status.connections_accepted,
        final_status.connections_rejected
    );
    info!(
        "Bytes: received={}, sent={}",
        final_status.bytes_received, final_status.bytes_sent
    );
    info!(
        "Storage: {} entries",
        final_status.storage_entries
    );
    info!(
        "Operations: total={}, errors={}",
        final_status.worker_metrics.total_operations,
        final_status.worker_metrics.total_errors
    );
    info!(
        "Membership: {}/{} alive",
        final_status.alive_members, final_status.member_count
    );

    // Flush storage if persistent
    if args.db_file.is_some() {
        info!("Flushing storage to disk...");
        if let Err(e) = server.store().flush() {
            warn!("Failed to flush storage: {}", e);
        } else {
            info!("Storage flushed successfully");
        }
    }

    info!("Server shutdown complete");
    Ok(())
}
