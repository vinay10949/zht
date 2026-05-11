//! ZHT Server — Zero-Hop Distributed Hash Table server.
//!
//! This is the main entry point for the ZHT server binary.
//! In Phase 1, it loads configuration and validates the setup.
//! In later phases, it will start the async event loop and handle requests.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use zht_config::AppConfig;

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

    /// Path to the NoVoHT persistence file.
    #[arg(short, long)]
    db_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
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
        tracing::warn!("No cluster nodes configured — server will start but may not be reachable");
    }

    let effective_port = args.port.unwrap_or_else(|| config.zht.port());
    info!("Server will listen on port {}", effective_port);

    // Phase 2+: Start the async event loop (tokio-based server)
    // - Bind to the configured port
    // - Accept incoming connections
    // - Deserialize ZPack messages
    // - Dispatch to HTWorker for DHT operations
    // - Send responses back to clients

    info!("Phase 1 complete — configuration validated successfully");
    info!("Phase 2+ will add the async event loop and DHT operations");

    Ok(())
}

// Import constants for validation
use zht_common::constants::*;
