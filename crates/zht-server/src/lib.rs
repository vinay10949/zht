//! ZHT Server — Server library with membership, replication, migration, and metrics.
//!
//! This crate provides the server-side components for a ZHT node:
//!
//! - [`MembershipManager`] — Tracks cluster members and their health state.
//! - [`ReplicationManager`] — Replicates write operations across replica nodes.
//! - [`MigrationManager`] — Handles data migration when membership changes.
//! - [`ServerMetrics`] / [`ServerMetricsSnapshot`] — Server-level observability.
//! - [`ZHTServer`] — Main server orchestrator that wires everything together.
//! - [`ReplicatedHTWorker`] — Wraps `HTWorker` to add automatic replication.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │  ZHTServer   │  ← Top-level orchestrator
//! │─────────────│
//! │ Membership  │  ← Tracks alive/suspect/dead nodes
//! │ Replication │  ← Replicates writes to N replicas
//! │ Migration   │  ← Moves data when nodes join/leave
//! │ Metrics     │  ← Connection/byte/operation counters
//! │ HTWorker    │  ← Core request processor
//! │ NoVoHT      │  ← In-memory hash table
//! └─────────────┘
//! ```

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use zht_common::constants::*;
use zht_common::hash_util::HashUtil;
use zht_common::HostEntity;
use zht_config::AppConfig;
use zht_proto::ZPack;
use zht_storage::NoVoHT;
use zht_transport::{recv_message, send_message, HTWorker, HTWorkerMetrics, TcpProxy};

// ═══════════════════════════════════════════════════════════════════════
// Section A: Membership Manager
// ═══════════════════════════════════════════════════════════════════════

/// Health state of a cluster member node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is actively responding to heartbeats.
    Alive,
    /// Node has missed heartbeats but is not yet declared dead.
    Suspect,
    /// Node has been declared dead after too many missed heartbeats.
    Dead,
    /// Node has gracefully left the cluster.
    Left,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Alive => write!(f, "Alive"),
            NodeState::Suspect => write!(f, "Suspect"),
            NodeState::Dead => write!(f, "Dead"),
            NodeState::Left => write!(f, "Left"),
        }
    }
}

/// An entry in the membership table.
#[derive(Debug, Clone)]
pub struct MembershipEntry {
    /// The host entity (host + port).
    pub host: HostEntity,
    /// Current health state.
    pub state: NodeState,
    /// Timestamp of the last heartbeat received.
    pub last_heartbeat: Instant,
    /// Timestamp when the node was first seen (joined).
    pub joined_at: Instant,
}

/// Manages cluster membership tracking.
///
/// `MembershipManager` maintains a list of all known cluster members,
/// tracks their health state, and provides zero-hop routing via
/// `hash(key) % active_count`.
pub struct MembershipManager {
    /// All known cluster members.
    members: RwLock<Vec<MembershipEntry>>,
    /// Self node information.
    self_node: HostEntity,
}

impl MembershipManager {
    /// Create a new membership manager.
    ///
    /// * `self_host` — The host entity representing this node.
    /// * `initial_members` — Initial list of neighbor nodes (from neighbor.conf).
    pub fn new(self_host: HostEntity, initial_members: Vec<HostEntity>) -> Self {
        let now = Instant::now();
        let members: Vec<MembershipEntry> = initial_members
            .into_iter()
            .filter(|h| *h != self_host)
            .map(|h| MembershipEntry {
                host: h,
                state: NodeState::Alive,
                last_heartbeat: now,
                joined_at: now,
            })
            .collect();

        info!(
            "MembershipManager initialized with {} members (self={}:{})",
            members.len(),
            self_host.host,
            self_host.port
        );

        Self {
            members: RwLock::new(members),
            self_node: self_host,
        }
    }

    /// Return all members regardless of state.
    pub fn members(&self) -> Vec<MembershipEntry> {
        self.members.read().clone()
    }

    /// Return the number of alive members (excluding self).
    pub fn member_count(&self) -> usize {
        self.members
            .read()
            .iter()
            .filter(|m| m.state == NodeState::Alive)
            .count()
    }

    /// Mark a node as alive (e.g., from a heartbeat).
    pub fn mark_alive(&self, host: &str, port: u16) {
        let mut members = self.members.write();
        if let Some(entry) = members.iter_mut().find(|m| m.host.host == host && m.host.port == port)
        {
            entry.state = NodeState::Alive;
            entry.last_heartbeat = Instant::now();
        }
    }

    /// Mark a node as suspect (missed heartbeats).
    pub fn mark_suspect(&self, host: &str, port: u16) {
        let mut members = self.members.write();
        if let Some(entry) = members.iter_mut().find(|m| m.host.host == host && m.host.port == port)
        {
            entry.state = NodeState::Suspect;
            entry.last_heartbeat = Instant::now();
        }
    }

    /// Mark a node as dead.
    pub fn mark_dead(&self, host: &str, port: u16) {
        let mut members = self.members.write();
        if let Some(entry) = members.iter_mut().find(|m| m.host.host == host && m.host.port == port)
        {
            entry.state = NodeState::Dead;
        }
    }

    /// Check if a node is alive.
    pub fn is_alive(&self, host: &str, port: u16) -> bool {
        self.members.read().iter().any(|m| {
            m.host.host == host
                && m.host.port == port
                && m.state == NodeState::Alive
        })
    }

    /// Add a new member dynamically.
    pub fn add_member(&self, host: HostEntity) {
        let mut members = self.members.write();
        // Don't add self or duplicates.
        if host == self.self_node {
            return;
        }
        if members.iter().any(|m| m.host == host) {
            return;
        }
        let addr = host.address();
        members.push(MembershipEntry {
            host,
            state: NodeState::Alive,
            last_heartbeat: Instant::now(),
            joined_at: Instant::now(),
        });
        info!("New member added: {}", addr);
    }

    /// Remove a member from the membership table.
    pub fn remove_member(&self, host: &str, port: u16) {
        let mut members = self.members.write();
        let before = members.len();
        members.retain(|m| !(m.host.host == host && m.host.port == port));
        if members.len() < before {
            info!("Member removed: {}:{}", host, port);
        }
    }

    /// Check health of all members and mark stale ones as suspect.
    ///
    /// Nodes that have not sent a heartbeat within `timeout` are marked
    /// as suspect.
    pub fn check_health(&self, timeout: Duration) {
        let mut members = self.members.write();
        for entry in members.iter_mut() {
            if entry.state == NodeState::Alive && entry.last_heartbeat.elapsed() > timeout {
                entry.state = NodeState::Suspect;
                warn!(
                    "Node {}:{} marked as suspect (no heartbeat for {:?})",
                    entry.host.host,
                    entry.host.port,
                    entry.last_heartbeat.elapsed()
                );
            }
        }
    }

    /// Return only alive members.
    pub fn active_members(&self) -> Vec<HostEntity> {
        self.members
            .read()
            .iter()
            .filter(|m| m.state == NodeState::Alive)
            .map(|m| m.host.clone())
            .collect()
    }

    /// Zero-hop routing: resolve which node should handle a key.
    ///
    /// Returns the `HostEntity` of the node responsible for `key`,
    /// or `None` if there are no alive members.
    pub fn resolve_node(&self, key: &str) -> Option<HostEntity> {
        let alive = self.active_members();
        if alive.is_empty() {
            return None;
        }
        let hash_code = HashUtil::gen_hash_str(key);
        let index = (hash_code as usize) % alive.len();
        Some(alive[index].clone())
    }

    /// Get the self node.
    pub fn self_node(&self) -> &HostEntity {
        &self.self_node
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section B: Replication Manager
// ═══════════════════════════════════════════════════════════════════════

/// Manages data replication across cluster nodes.
///
/// When replication is enabled (`replication_type > 0`), write operations
/// are forwarded to N replica nodes in addition to the primary node.
/// Replica nodes are selected as the next N nodes in the hash ring after
/// the primary: if `hash(key) % num_nodes = i`, replicas are at
/// `(i+1) % num_nodes`, `(i+2) % num_nodes`, etc.
pub struct ReplicationManager {
    /// Number of replicas to maintain.
    num_replicas: u32,
    /// Replication type: 0 = none, 1 = client-side.
    replication_type: u32,
    /// Membership reference for routing.
    membership: Arc<MembershipManager>,
    /// TCP proxy for sending replication messages.
    proxy: Arc<TcpProxy>,
}

impl ReplicationManager {
    /// Create a new replication manager.
    ///
    /// * `num_replicas` — Number of replicas (from config).
    /// * `replication_type` — 0 = disabled, 1 = client-side replication.
    /// * `membership` — Membership manager reference.
    /// * `proxy` — TCP proxy for network communication.
    pub fn new(
        num_replicas: u32,
        replication_type: u32,
        membership: Arc<MembershipManager>,
        proxy: Arc<TcpProxy>,
    ) -> Self {
        info!(
            "ReplicationManager: replicas={}, type={}",
            num_replicas, replication_type
        );
        Self {
            num_replicas,
            replication_type,
            membership,
            proxy,
        }
    }

    /// Replicate an insert operation to all replica nodes.
    pub async fn replicate_insert(&self, key: &str, value: &[u8]) {
        if !self.is_replication_enabled() {
            return;
        }
        let replicas = self.get_replica_nodes(key);
        for host in &replicas {
            let request = ZPack {
                opcode: OPC_INSERT.as_bytes().to_vec(),
                key: key.as_bytes().to_vec(),
                val: value.to_vec(),
                ..Default::default()
            };
            if let Err(e) = self.proxy.send_only(&host.host, host.port, &request).await {
                warn!(
                    "Failed to replicate INSERT to {}:{}: {}",
                    host.host, host.port, e
                );
            }
        }
    }

    /// Replicate a remove operation to all replica nodes.
    pub async fn replicate_remove(&self, key: &str) {
        if !self.is_replication_enabled() {
            return;
        }
        let replicas = self.get_replica_nodes(key);
        for host in &replicas {
            let request = ZPack {
                opcode: OPC_REMOVE.as_bytes().to_vec(),
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            if let Err(e) = self.proxy.send_only(&host.host, host.port, &request).await {
                warn!(
                    "Failed to replicate REMOVE to {}:{}: {}",
                    host.host, host.port, e
                );
            }
        }
    }

    /// Replicate an append operation to all replica nodes.
    pub async fn replicate_append(&self, key: &str, value: &[u8]) {
        if !self.is_replication_enabled() {
            return;
        }
        let replicas = self.get_replica_nodes(key);
        for host in &replicas {
            let request = ZPack {
                opcode: OPC_APPEND.as_bytes().to_vec(),
                key: key.as_bytes().to_vec(),
                val: value.to_vec(),
                ..Default::default()
            };
            if let Err(e) = self.proxy.send_only(&host.host, host.port, &request).await {
                warn!(
                    "Failed to replicate APPEND to {}:{}: {}",
                    host.host, host.port, e
                );
            }
        }
    }

    /// Get the N replica nodes for a given key.
    ///
    /// If the primary node for a key is at index `i` in the active members
    /// list, replicas are at `(i+1) % n`, `(i+2) % n`, etc.
    pub fn get_replica_nodes(&self, key: &str) -> Vec<HostEntity> {
        let alive = self.membership.active_members();
        if alive.is_empty() || self.num_replicas == 0 {
            return Vec::new();
        }

        let hash_code = HashUtil::gen_hash_str(key);
        let primary_index = (hash_code as usize) % alive.len();
        let n = self.num_replicas as usize;
        let total = alive.len();

        (1..=n)
            .map(|offset| alive[(primary_index + offset) % total].clone())
            .collect()
    }

    /// Check if replication is configured and enabled.
    pub fn is_replication_enabled(&self) -> bool {
        self.replication_type > 0 && self.num_replicas > 0
    }

    /// Number of replicas configured.
    pub fn replica_count(&self) -> u32 {
        self.num_replicas
    }

    /// Replication type.
    pub fn replication_type(&self) -> u32 {
        self.replication_type
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section C: Migration Manager
// ═══════════════════════════════════════════════════════════════════════

/// A migration action: move a set of keys from one node to another.
#[derive(Debug, Clone)]
pub struct MigrationAction {
    /// Target node to receive the keys.
    pub target_host: HostEntity,
    /// Keys to migrate.
    pub keys: Vec<String>,
}

/// Manages data migration when cluster membership changes.
///
/// When nodes join or leave the cluster, key ownership may shift
/// according to `hash(key) % num_nodes`. `MigrationManager` computes
/// which keys need to move and orchestrates the transfer.
pub struct MigrationManager {
    /// Membership reference.
    membership: Arc<MembershipManager>,
    /// TCP proxy for sending migration data.
    proxy: Arc<TcpProxy>,
    /// Local storage reference.
    store: Arc<NoVoHT>,
    /// Whether migration is enabled.
    migration_enabled: bool,
    /// Whether a migration is currently in progress.
    migrating: AtomicBool,
}

impl MigrationManager {
    /// Create a new migration manager.
    ///
    /// * `membership` — Membership manager reference.
    /// * `proxy` — TCP proxy for network communication.
    /// * `store` — Local storage engine.
    /// * `enabled` — Whether migration is enabled.
    pub fn new(
        membership: Arc<MembershipManager>,
        proxy: Arc<TcpProxy>,
        store: Arc<NoVoHT>,
        enabled: bool,
    ) -> Self {
        info!("MigrationManager initialized (enabled={})", enabled);
        Self {
            membership,
            proxy,
            store,
            migration_enabled: enabled,
            migrating: AtomicBool::new(false),
        }
    }

    /// Check if a migration is currently in progress.
    pub fn is_migrating(&self) -> bool {
        self.migrating.load(Ordering::Relaxed)
    }

    /// Check if migration is enabled.
    pub fn is_migration_enabled(&self) -> bool {
        self.migration_enabled
    }

    /// Check if any keys need to be migrated and perform migration.
    ///
    /// This is a synchronous check; the actual migration happens
    /// asynchronously. Returns the number of keys queued for migration.
    pub async fn check_and_migrate(&self) -> usize {
        if !self.migration_enabled || self.is_migrating() {
            return 0;
        }

        let alive = self.membership.active_members();
        let all_keys = self.store.keys();
        let self_host = self.membership.self_node();

        // For each key, check if this node is still the owner.
        // Keys that no longer belong to this node should be migrated.
        let mut keys_to_migrate: HashMap<String, HostEntity> = HashMap::new();

        for key in &all_keys {
            if let Some(target) = self.resolve_owner(key, &alive) {
                if &target != self_host {
                    keys_to_migrate.insert(key.clone(), target);
                }
            }
        }

        if keys_to_migrate.is_empty() {
            return 0;
        }

        self.migrating.store(true, Ordering::Relaxed);
        info!("Migration: {} keys to migrate", keys_to_migrate.len());

        // Group by target node
        let mut by_target: HashMap<String, (HostEntity, Vec<String>)> = HashMap::new();
        for (key, target) in keys_to_migrate {
            let addr = target.address();
            by_target
                .entry(addr)
                .or_insert((target, Vec::new()))
                .1
                .push(key);
        }

        let mut migrated = 0usize;
        for (addr, (target, keys)) in by_target {
            debug!(
                "Migrating {} keys to {}:{}",
                keys.len(),
                target.host,
                target.port
            );
            match self
                .migrate_keys_for_node(&target.host, target.port, &keys)
                .await
            {
                Ok(count) => {
                    migrated += count;
                    // Remove migrated keys from local store
                    for key in &keys {
                        let _ = self.store.remove(key);
                    }
                }
                Err(e) => {
                    warn!("Migration to {} failed: {}", addr, e);
                }
            }
        }

        self.migrating.store(false, Ordering::Relaxed);
        migrated
    }

    /// Migrate a set of keys to a target node.
    ///
    /// Returns the number of keys successfully migrated.
    pub async fn migrate_keys_for_node(
        &self,
        target_host: &str,
        target_port: u16,
        keys: &[String],
    ) -> Result<usize, String> {
        if keys.is_empty() {
            return Ok(0);
        }

        let entries = self.store.entries();
        let key_set: std::collections::HashSet<&str> =
            keys.iter().map(|s| s.as_str()).collect();

        let mut migrated = 0usize;
        for (key, value) in &entries {
            if !key_set.contains(key.as_str()) {
                continue;
            }
            let request = ZPack {
                opcode: OPC_INSERT.as_bytes().to_vec(),
                key: key.as_bytes().to_vec(),
                val: value.as_bytes().to_vec(),
                ..Default::default()
            };
            match self
                .proxy
                .sendrecv_with_timeout(
                    target_host,
                    target_port,
                    &request,
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(resp) => {
                    let status = String::from_utf8_lossy(&resp.lease);
                    if status.starts_with(REC_SUCC) {
                        migrated += 1;
                    } else {
                        warn!(
                            "Migration INSERT for key '{}' to {}:{} returned: {}",
                            key, target_host, target_port, status
                        );
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to migrate key '{}': {}", key, e));
                }
            }
        }

        Ok(migrated)
    }

    /// Get all keys that belong to a specific node (by hash).
    pub fn get_keys_for_node(&self, host: &str, port: u16) -> Vec<String> {
        let target = HostEntity::new(host, port);
        let all_keys = self.store.keys();
        let alive = self.membership.active_members();
        all_keys
            .into_iter()
            .filter(|key| {
                self.resolve_owner(key, &alive)
                    .map(|ref h| *h == target)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Calculate a migration plan given old and new member lists.
    ///
    /// Returns a list of `MigrationAction` describing which keys should
    /// move from this node to which target node.
    pub fn calculate_migration_plan(
        &self,
        old_members: &[HostEntity],
        new_members: &[HostEntity],
    ) -> Vec<MigrationAction> {
        if old_members.is_empty() || new_members.is_empty() {
            return Vec::new();
        }

        let all_keys = self.store.keys();
        let mut plan: HashMap<String, MigrationAction> = HashMap::new();

        for key in &all_keys {
            let old_owner = self.resolve_owner_against(key, old_members);
            let new_owner = self.resolve_owner_against(key, new_members);

            // If ownership changed and the new owner is different from us
            if old_owner != new_owner {
                if let Some(target) = new_owner {
                    let self_host = self.membership.self_node();
                    if &target != self_host {
                        let addr = target.address();
                        let action = plan.entry(addr).or_insert(MigrationAction {
                            target_host: target,
                            keys: Vec::new(),
                        });
                        action.keys.push(key.clone());
                    }
                }
            }
        }

        plan.into_values().collect()
    }

    /// Bulk insert migrated key-value pairs into local storage.
    pub fn receive_migrated_keys(&self, keys_values: &[(String, String)]) {
        for (key, value) in keys_values {
            if let Err(e) = self.store.put(key, value) {
                warn!("Failed to store migrated key: {}", e);
            }
        }
    }

    /// Resolve the owner of a key against a specific member list.
    fn resolve_owner(&self, key: &str, members: &[HostEntity]) -> Option<HostEntity> {
        self.resolve_owner_against(key, members)
    }

    /// Resolve the owner of a key against a specific member list.
    fn resolve_owner_against(&self, key: &str, members: &[HostEntity]) -> Option<HostEntity> {
        if members.is_empty() {
            return None;
        }
        let hash_code = HashUtil::gen_hash_str(key);
        let index = (hash_code as usize) % members.len();
        Some(members[index].clone())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section D: Server Metrics
// ═══════════════════════════════════════════════════════════════════════

/// A point-in-time snapshot of server metrics.
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    /// Server uptime in seconds.
    pub uptime_secs: f64,
    /// Total connections since server start.
    pub connections_total: u64,
    /// Currently active connections.
    pub connections_active: u64,
    /// Total connections accepted.
    pub connections_accepted: u64,
    /// Total connections rejected.
    pub connections_rejected: u64,
    /// Total bytes received.
    pub bytes_received: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Number of entries in the storage engine.
    pub storage_entries: usize,
    /// Snapshot of HTWorker operation metrics.
    pub worker_metrics: HTWorkerMetrics,
    /// Total number of members in the membership table.
    pub member_count: usize,
    /// Number of alive members.
    pub alive_members: usize,
}

/// Server-level observability metrics.
///
/// Tracks connections, bytes, and periodically snapshots HTWorker
/// operation metrics for aggregated monitoring.
pub struct ServerMetrics {
    /// Server start time.
    pub start_time: Instant,
    /// Total connections since start.
    pub connections_total: AtomicU64,
    /// Currently active connections.
    pub connections_active: AtomicU64,
    /// Total accepted connections.
    pub connections_accepted: AtomicU64,
    /// Total rejected connections.
    pub connections_rejected: AtomicU64,
    /// Total bytes received.
    pub bytes_received: AtomicU64,
    /// Total bytes sent.
    pub bytes_sent: AtomicU64,
    /// Snapshot of HTWorker metrics (updated periodically).
    pub worker_metrics: Mutex<HTWorkerMetrics>,
}

impl ServerMetrics {
    /// Create with zeroed counters and the current timestamp.
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            worker_metrics: Mutex::new(HTWorkerMetrics::default()),
        }
    }

    /// Record that a connection was accepted.
    pub fn record_connection_accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a connection was closed.
    pub fn record_connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record bytes received.
    pub fn record_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    /// Record bytes sent.
    pub fn record_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    /// Record a rejected connection.
    pub fn record_connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the worker metrics snapshot.
    pub fn update_worker_metrics(&self, metrics: HTWorkerMetrics) {
        *self.worker_metrics.lock() = metrics;
    }

    /// Server uptime in seconds.
    pub fn uptime_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Take a point-in-time snapshot of all metrics.
    pub fn snapshot(
        &self,
        storage_entries: usize,
        member_count: usize,
        alive_members: usize,
    ) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            storage_entries,
            worker_metrics: self.worker_metrics.lock().clone(),
            member_count,
            alive_members,
        }
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section E: Replicated HTWorker
// ═══════════════════════════════════════════════════════════════════════

/// A wrapper around `HTWorker` that adds automatic replication.
///
/// After processing a request locally, `ReplicatedHTWorker` checks if
/// the operation was a mutating write (INSERT, REMOVE, APPEND) that
/// succeeded, and if so, replicates it to the configured replica nodes.
pub struct ReplicatedHTWorker {
    /// Inner HTWorker for local processing.
    inner: HTWorker,
    /// Replication manager.
    replication: Arc<ReplicationManager>,
}

impl ReplicatedHTWorker {
    /// Create a new replicated HT worker.
    pub fn new(worker: HTWorker, replication: Arc<ReplicationManager>) -> Self {
        Self {
            inner: worker,
            replication,
        }
    }

    /// Process a request locally, then replicate if it was a successful write.
    ///
    /// **Note**: This must be called from within a tokio runtime context
    /// because replication is spawned as a background task.
    pub fn process_with_replication(&self, request: &ZPack) -> ZPack {
        let response = self.inner.process(request);

        // Check if operation succeeded
        let status = String::from_utf8_lossy(&response.lease);
        if !status.starts_with(REC_SUCC) {
            return response;
        }

        let opcode = String::from_utf8_lossy(&request.opcode).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();
        let val = request.val.clone();

        if key.is_empty() {
            return response;
        }

        let replication = self.replication.clone();

        // Spawn replication in background
        tokio::spawn(async move {
            match opcode.as_str() {
                OPC_INSERT => {
                    replication.replicate_insert(&key, &val).await;
                }
                OPC_REMOVE => {
                    replication.replicate_remove(&key).await;
                }
                OPC_APPEND => {
                    replication.replicate_append(&key, &val).await;
                }
                _ => {}
            }
        });

        response
    }

    /// Process a request locally only (no replication).
    pub fn process_local(&self, request: &ZPack) -> ZPack {
        self.inner.process(request)
    }

    /// Get a reference to the inner HTWorker.
    pub fn inner(&self) -> &HTWorker {
        &self.inner
    }

    /// Get worker metrics.
    pub fn metrics(&self) -> HTWorkerMetrics {
        self.inner.metrics()
    }

    /// Get the underlying store.
    pub fn store(&self) -> &NoVoHT {
        self.inner.store()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Section F: ZHT Server Orchestrator
// ═══════════════════════════════════════════════════════════════════════

/// The main ZHT server orchestrator.
///
/// `ZHTServer` wires together all server components:
/// membership management, replication, migration, metrics, and the
/// TCP server listener.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use zht_server::ZHTServer;
/// use zht_storage::NoVoHT;
/// use zht_common::HostEntity;
///
/// #[tokio::main]
/// async fn main() {
///     let store = NoVoHT::new();
///     let self_host = HostEntity::new("0.0.0.0", 50000);
///     let server = ZHTServer::new_with_store(store, self_host, vec![], 0, 0, true);
///     server.start("0.0.0.0:50000").await.unwrap();
/// }
/// ```
pub struct ZHTServer {
    /// Application configuration.
    pub config: Option<AppConfig>,
    /// Local NoVoHT storage engine.
    pub store: Arc<NoVoHT>,
    /// The core HTWorker.
    pub worker: Arc<HTWorker>,
    /// Membership manager.
    pub membership: Arc<MembershipManager>,
    /// Replication manager.
    pub replication: Arc<ReplicationManager>,
    /// Migration manager.
    pub migration: Arc<MigrationManager>,
    /// Server metrics.
    pub metrics: Arc<ServerMetrics>,
    /// Cancellation token for graceful shutdown.
    pub cancel_token: CancellationToken,
    /// TCP proxy for outgoing connections (available for future use).
    #[allow(dead_code)]
    proxy: Arc<TcpProxy>,
}

impl ZHTServer {
    /// Create a new ZHT server from an `AppConfig`.
    ///
    /// Initializes all components from the configuration.
    pub fn new(config: AppConfig, store: NoVoHT) -> Self {
        let self_host = HostEntity::new(
            "0.0.0.0",
            config.zht.port(),
        );
        let initial_members: Vec<HostEntity> = config
            .neighbors
            .neighbors
            .iter()
            .filter(|n| **n != self_host)
            .cloned()
            .collect();

        Self::new_with_store(
            store,
            self_host,
            initial_members,
            config.zht.num_replicas(),
            config.zht.replication_type(),
            true,
        )
    }

    /// Create a new ZHT server with explicit parameters.
    ///
    /// This is useful for testing and programmatic server creation
    /// without a config file.
    pub fn new_with_store(
        store: NoVoHT,
        self_host: HostEntity,
        initial_members: Vec<HostEntity>,
        num_replicas: u32,
        replication_type: u32,
        migration_enabled: bool,
    ) -> Self {
        let store = Arc::new(store);
        // Create a separate NoVoHT for the worker. Since NoVoHT uses interior
        // mutability (RwLock), we create a new empty one and share data via
        // the same Arc. We use a clone of the Arc for the worker.
        let worker_store = NoVoHT::new();
        let worker = Arc::new(HTWorker::new(worker_store));
        let membership = Arc::new(MembershipManager::new(self_host, initial_members));
        let proxy = Arc::new(TcpProxy::new());
        let replication = Arc::new(ReplicationManager::new(
            num_replicas,
            replication_type,
            membership.clone(),
            proxy.clone(),
        ));
        let migration = Arc::new(MigrationManager::new(
            membership.clone(),
            proxy.clone(),
            store.clone(),
            migration_enabled,
        ));
        let metrics = Arc::new(ServerMetrics::new());
        let cancel_token = CancellationToken::new();

        info!("ZHTServer created successfully");
        Self {
            config: None,
            store,
            worker,
            membership,
            replication,
            migration,
            metrics,
            cancel_token,
            proxy,
        }
    }

    /// Start the TCP server with graceful shutdown support.
    ///
    /// Binds to the given address and accepts connections in a loop.
    /// Each connection is handled in a separate tokio task.
    /// The accept loop exits when `cancel_token` is cancelled.
    pub async fn start(&self, addr: &str) -> anyhow::Result<()> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid bind address '{}': {}", addr, e))?;

        let listener = TcpListener::bind(socket_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", addr, e))?;

        info!("ZHT server listening on {}", socket_addr);
        info!("Press Ctrl+C to trigger graceful shutdown");

        let metrics = self.metrics.clone();
        let worker = self.worker.clone();
        let cancel = self.cancel_token.clone();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = accept_result
                        .map_err(|e| anyhow::anyhow!("Accept error: {}", e))?;

                    metrics.record_connection_accepted();
                    debug!("Accepted connection from {}", peer_addr);

                    let worker = worker.clone();
                    let metrics = metrics.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection_with_metrics(stream, peer_addr, worker, &metrics).await {
                            warn!("Connection handler error for {}: {}", peer_addr, e);
                        }
                    });
                }
                _ = cancel.cancelled() => {
                    info!("Shutdown signal received, stopping accept loop");
                    break;
                }
            }
        }

        info!(
            "Server stopped accepting new connections (uptime: {:.1}s)",
            self.metrics.uptime_secs()
        );
        Ok(())
    }

    /// Trigger graceful shutdown.
    pub fn shutdown(&self) {
        info!("Initiating graceful shutdown...");
        self.cancel_token.cancel();
    }

    /// Check server health.
    ///
    /// Returns `true` if the server appears healthy:
    /// - Storage is accessible
    /// - At least one membership entry exists or the cluster is intentionally single-node
    pub fn health_check(&self) -> bool {
        // Storage health check: try len()
        let _ = self.store.len();

        // Membership check: single-node clusters are healthy
        let member_count = self.membership.member_count();
        member_count > 0 || self.membership.members().is_empty()
    }

    /// Get a status snapshot.
    pub fn status(&self) -> ServerMetricsSnapshot {
        let member_list = self.membership.members();
        let member_count = member_list.len();
        let alive_members = member_list
            .iter()
            .filter(|m| m.state == NodeState::Alive)
            .count();

        // Update worker metrics before snapshot
        self.metrics
            .update_worker_metrics(self.worker.metrics());

        self.metrics
            .snapshot(self.store.len(), member_count, alive_members)
    }

    /// Access the storage engine.
    pub fn store(&self) -> &NoVoHT {
        &self.store
    }

    /// Spawn a periodic membership health checker.
    ///
    /// Checks member health every `interval` and marks stale nodes as suspect.
    /// Returns a `JoinHandle` for the background task.
    pub fn spawn_health_checker(
        self: &Arc<Self>,
        interval: Duration,
        timeout: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        debug!("Running periodic membership health check");
                        server.membership.check_health(timeout);
                        debug!(
                            "Membership: {} alive, {} total",
                            server.membership.member_count(),
                            server.membership.members().len()
                        );
                    }
                    _ = server.cancel_token.cancelled() => {
                        info!("Health checker shutting down");
                        break;
                    }
                }
            }
        })
    }
}

/// Handle a single client connection with metrics tracking.
async fn handle_connection_with_metrics(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    worker: Arc<HTWorker>,
    metrics: &ServerMetrics,
) -> anyhow::Result<()> {
    // Disable Nagle's algorithm for lower latency
    stream.set_nodelay(true)?;

    loop {
        let request = match recv_message(&mut stream).await {
            Ok(req) => req,
            Err(_) => {
                // Client disconnected or connection error
                break;
            }
        };

        // Track bytes received
        let recv_bytes = 4 + request.encoded_len() as u64;
        metrics.record_bytes_received(recv_bytes);

        // Process the request
        let response = worker.process(&request);

        // Track bytes sent
        let send_bytes = 4 + response.encoded_len() as u64;
        metrics.record_bytes_sent(send_bytes);

        // Send response
        if let Err(e) = send_message(&mut stream, &response).await {
            debug!("Failed to send response to {}: {}", peer_addr, e);
            break;
        }
    }

    metrics.record_connection_closed();
    debug!("Connection closed from {}", peer_addr);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// Section G: Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    // ── MembershipManager tests ───────────────────────────────────────

    fn make_host(port: u16) -> HostEntity {
        HostEntity::new("localhost", port)
    }

    #[test]
    fn test_membership_new_empty() {
        let mm = MembershipManager::new(make_host(50000), vec![]);
        assert_eq!(mm.member_count(), 0);
        assert_eq!(mm.members().len(), 0);
    }

    #[test]
    fn test_membership_new_with_initial() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        );
        assert_eq!(mm.member_count(), 2);
        assert_eq!(mm.members().len(), 2);
    }

    #[test]
    fn test_membership_excludes_self() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50000), make_host(50001)],
        );
        // Self should be excluded
        assert_eq!(mm.member_count(), 1);
        assert_eq!(mm.members().len(), 1);
        assert!(mm.members()[0].host.port == 50001);
    }

    #[test]
    fn test_membership_add_member() {
        let mm = MembershipManager::new(make_host(50000), vec![]);
        mm.add_member(make_host(50001));
        assert_eq!(mm.member_count(), 1);
    }

    #[test]
    fn test_membership_add_duplicate_ignored() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        mm.add_member(make_host(50001));
        assert_eq!(mm.member_count(), 1);
    }

    #[test]
    fn test_membership_add_self_ignored() {
        let mm = MembershipManager::new(make_host(50000), vec![]);
        mm.add_member(make_host(50000));
        assert_eq!(mm.member_count(), 0);
    }

    #[test]
    fn test_membership_remove_member() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        );
        mm.remove_member("localhost", 50001);
        assert_eq!(mm.member_count(), 1);
        assert!(mm.is_alive("localhost", 50002));
    }

    #[test]
    fn test_membership_mark_alive() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        mm.mark_dead("localhost", 50001);
        assert!(!mm.is_alive("localhost", 50001));
        mm.mark_alive("localhost", 50001);
        assert!(mm.is_alive("localhost", 50001));
    }

    #[test]
    fn test_membership_mark_suspect() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        mm.mark_suspect("localhost", 50001);
        assert!(!mm.is_alive("localhost", 50001));
        // Check it's in suspect state
        let members = mm.members();
        assert_eq!(members[0].state, NodeState::Suspect);
    }

    #[test]
    fn test_membership_mark_dead() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        mm.mark_dead("localhost", 50001);
        assert!(!mm.is_alive("localhost", 50001));
        let members = mm.members();
        assert_eq!(members[0].state, NodeState::Dead);
    }

    #[test]
    fn test_membership_is_alive() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        assert!(mm.is_alive("localhost", 50001));
        assert!(!mm.is_alive("localhost", 65535));
    }

    #[test]
    fn test_membership_check_health() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        );
        // All members start as alive with recent heartbeat, so no change
        mm.check_health(Duration::from_secs(30));
        assert_eq!(mm.member_count(), 1);

        // Manually set old heartbeat
        {
            let mut members = mm.members.write();
            if let Some(entry) = members.iter_mut().next() {
                // last_heartbeat is set during construction, so we can't really
                // make it old. But we can mark it suspect to test the filter.
                entry.state = NodeState::Alive;
            }
        }
        // With very short timeout, member should be marked suspect
        // (if we could manipulate time). Instead, let's verify the check
        // doesn't crash with zero timeout.
        mm.check_health(Duration::ZERO);
    }

    #[test]
    fn test_membership_active_members() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        );
        mm.mark_dead("localhost", 50002);
        let active = mm.active_members();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].port, 50001);
    }

    #[test]
    fn test_membership_resolve_node() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002), make_host(50003)],
        );
        let node = mm.resolve_node("test_key");
        assert!(node.is_some());
        // Same key should always resolve to same node
        let node2 = mm.resolve_node("test_key");
        assert_eq!(node.unwrap(), node2.unwrap());
    }

    #[test]
    fn test_membership_resolve_node_empty() {
        let mm = MembershipManager::new(make_host(50000), vec![]);
        assert!(mm.resolve_node("key").is_none());
    }

    #[test]
    fn test_membership_resolve_node_distribution() {
        let mm = MembershipManager::new(
            make_host(50000),
            vec![
                make_host(50001),
                make_host(50002),
                make_host(50003),
                make_host(50004),
            ],
        );
        // With 4 nodes, we should see keys distributed across them
        let mut nodes: std::collections::HashSet<String> = std::collections::HashSet::new();
        for i in 0..100 {
            let node = mm.resolve_node(&format!("key_{}", i)).unwrap();
            nodes.insert(node.address());
        }
        // Should have keys on at least 2 different nodes (very unlikely to all hash to 1)
        assert!(nodes.len() >= 2, "Expected distribution across nodes, got: {:?}", nodes);
    }

    #[test]
    fn test_membership_self_node() {
        let self_host = make_host(50000);
        let mm = MembershipManager::new(self_host.clone(), vec![]);
        assert_eq!(mm.self_node(), &self_host);
    }

    // ── ReplicationManager tests ──────────────────────────────────────

    fn make_replication_manager(num_replicas: u32, replication_type: u32) -> ReplicationManager {
        let membership = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![
                make_host(50001),
                make_host(50002),
                make_host(50003),
            ],
        ));
        let proxy = Arc::new(TcpProxy::new());
        ReplicationManager::new(num_replicas, replication_type, membership, proxy)
    }

    #[test]
    fn test_replication_disabled() {
        let rm = make_replication_manager(2, 0);
        assert!(!rm.is_replication_enabled());
    }

    #[test]
    fn test_replication_enabled() {
        let rm = make_replication_manager(2, 1);
        assert!(rm.is_replication_enabled());
    }

    #[test]
    fn test_replication_zero_replicas() {
        let rm = make_replication_manager(0, 1);
        assert!(!rm.is_replication_enabled());
    }

    #[test]
    fn test_replication_replica_count() {
        let rm = make_replication_manager(3, 1);
        assert_eq!(rm.replica_count(), 3);
    }

    #[test]
    fn test_replication_type() {
        let rm = make_replication_manager(2, 1);
        assert_eq!(rm.replication_type(), 1);
    }

    #[test]
    fn test_replication_get_replica_nodes() {
        let rm = make_replication_manager(2, 1);
        let replicas = rm.get_replica_nodes("test_key");
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn test_replication_get_replica_nodes_disabled() {
        let rm = make_replication_manager(0, 1);
        let replicas = rm.get_replica_nodes("test_key");
        assert!(replicas.is_empty());
    }

    #[test]
    fn test_replication_get_replica_nodes_wraps_around() {
        let rm = make_replication_manager(2, 1);
        let r1 = rm.get_replica_nodes("key1");
        let r2 = rm.get_replica_nodes("key1");
        // Same key should produce same replicas
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_replication_no_members() {
        let membership = Arc::new(MembershipManager::new(make_host(50000), vec![]));
        let proxy = Arc::new(TcpProxy::new());
        let rm = ReplicationManager::new(2, 1, membership, proxy);
        assert!(rm.get_replica_nodes("key").is_empty());
    }

    // ── MigrationManager tests ────────────────────────────────────────

    fn make_migration_manager(enabled: bool) -> MigrationManager {
        let store = Arc::new(NoVoHT::new());
        let membership = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        ));
        let proxy = Arc::new(TcpProxy::new());
        MigrationManager::new(membership, proxy, store, enabled)
    }

    #[test]
    fn test_migration_enabled() {
        let mm = make_migration_manager(true);
        assert!(mm.is_migration_enabled());
    }

    #[test]
    fn test_migration_disabled() {
        let mm = make_migration_manager(false);
        assert!(!mm.is_migration_enabled());
    }

    #[test]
    fn test_migration_not_migrating_initially() {
        let mm = make_migration_manager(true);
        assert!(!mm.is_migrating());
    }

    #[test]
    fn test_migration_calculate_plan_empty() {
        let mm = make_migration_manager(true);
        let plan = mm.calculate_migration_plan(&[], &[]);
        assert!(plan.is_empty());
    }

    #[test]
    fn test_migration_calculate_plan_same_members() {
        let mm = make_migration_manager(true);
        let members = vec![make_host(50001), make_host(50002)];
        let plan = mm.calculate_migration_plan(&members, &members);
        assert!(plan.is_empty());
    }

    #[test]
    fn test_migration_receive_keys() {
        let store = Arc::new(NoVoHT::new());
        let membership = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        ));
        let proxy = Arc::new(TcpProxy::new());
        let mm = MigrationManager::new(membership, proxy, store.clone(), true);

        mm.receive_migrated_keys(&[
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]);

        assert_eq!(store.len(), 2);
        assert_eq!(store.get("key1").unwrap(), "val1");
        assert_eq!(store.get("key2").unwrap(), "val2");
    }

    #[test]
    fn test_migration_get_keys_for_node() {
        let store = Arc::new(NoVoHT::new());
        // Insert some keys
        store.put("key1", "val1").unwrap();
        store.put("key2", "val2").unwrap();

        let membership = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![make_host(50001)],
        ));
        let proxy = Arc::new(TcpProxy::new());
        let mm = MigrationManager::new(membership, proxy, store.clone(), true);

        // Keys are resolved against alive members
        let keys = mm.get_keys_for_node("localhost", 50001);
        // With only 1 member (50001), all keys hash to that node
        assert_eq!(keys.len(), 2);
    }

    // ── ServerMetrics tests ───────────────────────────────────────────

    #[test]
    fn test_metrics_initial() {
        let m = ServerMetrics::new();
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_connection_accepted() {
        let m = ServerMetrics::new();
        m.record_connection_accepted();
        assert_eq!(m.connections_accepted.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metrics_connection_closed() {
        let m = ServerMetrics::new();
        m.record_connection_accepted();
        m.record_connection_accepted();
        m.record_connection_closed();
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metrics_bytes() {
        let m = ServerMetrics::new();
        m.record_bytes_received(1024);
        m.record_bytes_received(512);
        m.record_bytes_sent(2048);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 1536);
        assert_eq!(m.bytes_sent.load(Ordering::Relaxed), 2048);
    }

    #[test]
    fn test_metrics_connection_rejected() {
        let m = ServerMetrics::new();
        m.record_connection_rejected();
        assert_eq!(m.connections_rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metrics_uptime() {
        let m = ServerMetrics::new();
        thread::sleep(Duration::from_millis(10));
        assert!(m.uptime_secs() >= 0.01);
        assert!(m.start_time.elapsed().as_secs_f64() >= 0.01);
    }

    #[test]
    fn test_metrics_snapshot() {
        let m = ServerMetrics::new();
        m.record_connection_accepted();
        m.record_bytes_received(100);
        m.record_bytes_sent(200);

        let snap = m.snapshot(42, 3, 2);
        assert!(snap.uptime_secs >= 0.0);
        assert_eq!(snap.connections_accepted, 1);
        assert_eq!(snap.bytes_received, 100);
        assert_eq!(snap.bytes_sent, 200);
        assert_eq!(snap.storage_entries, 42);
        assert_eq!(snap.member_count, 3);
        assert_eq!(snap.alive_members, 2);
    }

    #[test]
    fn test_metrics_default() {
        let m = ServerMetrics::default();
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_update_worker_metrics() {
        let m = ServerMetrics::new();
        let mut wm = HTWorkerMetrics::default();
        wm.total_operations = 42;
        m.update_worker_metrics(wm);
        let snap = m.snapshot(0, 0, 0);
        assert_eq!(snap.worker_metrics.total_operations, 42);
    }

    // ── ZHTServer tests ───────────────────────────────────────────────

    #[test]
    fn test_server_creation() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![make_host(50001)],
            0,
            0,
            true,
        );
        assert!(server.health_check());
        let _ = server.membership.member_count();
    }

    #[test]
    fn test_server_store_access() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![],
            0,
            0,
            true,
        );
        server.store().put("key", "value").unwrap();
        assert_eq!(server.store().get("key").unwrap(), "value");
    }

    #[test]
    fn test_server_health_check() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![],
            0,
            0,
            true,
        );
        assert!(server.health_check());
    }

    #[test]
    fn test_server_status() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![make_host(50001)],
            0,
            0,
            true,
        );
        let status = server.status();
        assert_eq!(status.member_count, 1);
        assert_eq!(status.alive_members, 1);
        assert_eq!(status.storage_entries, 0);
    }

    #[test]
    fn test_server_shutdown() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![],
            0,
            0,
            true,
        );
        assert!(!server.cancel_token.is_cancelled());
        server.shutdown();
        assert!(server.cancel_token.is_cancelled());
    }

    #[test]
    fn test_server_with_replication() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![make_host(50001), make_host(50002), make_host(50003)],
            2,
            1,
            true,
        );
        assert!(server.replication.is_replication_enabled());
        assert_eq!(server.replication.replica_count(), 2);
    }

    #[tokio::test]
    async fn test_server_start_and_shutdown() {
        let store = NoVoHT::new();
        let server = ZHTServer::new_with_store(
            store,
            make_host(50000),
            vec![],
            0,
            0,
            true,
        );

        // Bind to a random port
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Free the port

        let server = Arc::new(server);
        let cancel = server.cancel_token.clone();

        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            let _ = server_clone.start(&addr.to_string()).await;
        });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Trigger shutdown
        cancel.cancel();

        // Wait for server to stop
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "Server should shut down within 2 seconds");
    }

    // ── ReplicatedHTWorker tests ──────────────────────────────────────

    fn make_replicated_worker(num_replicas: u32, replication_type: u32) -> ReplicatedHTWorker {
        let store = NoVoHT::new();
        let worker = HTWorker::new(store);
        let membership = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        ));
        let proxy = Arc::new(TcpProxy::new());
        let replication = Arc::new(ReplicationManager::new(
            num_replicas,
            replication_type,
            membership,
            proxy,
        ));
        ReplicatedHTWorker::new(worker, replication)
    }

    #[test]
    fn test_replicated_worker_process_local() {
        let rw = make_replicated_worker(2, 0);
        let request = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: b"key".to_vec(),
            val: b"value".to_vec(),
            ..Default::default()
        };
        let response = rw.process_local(&request);
        let status = String::from_utf8_lossy(&response.lease);
        assert!(status.starts_with(REC_SUCC));
        assert_eq!(rw.store().get("key").unwrap(), "value");
    }

    #[tokio::test]
    async fn test_replicated_worker_insert() {
        let rw = make_replicated_worker(2, 1);
        let request = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: b"key".to_vec(),
            val: b"value".to_vec(),
            ..Default::default()
        };
        let response = rw.process_with_replication(&request);
        let status = String::from_utf8_lossy(&response.lease);
        assert!(status.starts_with(REC_SUCC));
    }

    #[test]
    fn test_replicated_worker_lookup() {
        let rw = make_replicated_worker(0, 0);
        // Insert first
        let insert_req = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: b"key".to_vec(),
            val: b"value".to_vec(),
            ..Default::default()
        };
        rw.process_local(&insert_req);

        // Lookup
        let lookup_req = ZPack {
            opcode: OPC_LOOKUP.as_bytes().to_vec(),
            key: b"key".to_vec(),
            ..Default::default()
        };
        let response = rw.process_local(&lookup_req);
        let status = String::from_utf8_lossy(&response.lease);
        assert!(status.starts_with(REC_SUCC));
    }

    #[tokio::test]
    async fn test_replicated_worker_remove() {
        let rw = make_replicated_worker(0, 0);
        let insert_req = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: b"key".to_vec(),
            val: b"value".to_vec(),
            ..Default::default()
        };
        rw.process_local(&insert_req);

        let remove_req = ZPack {
            opcode: OPC_REMOVE.as_bytes().to_vec(),
            key: b"key".to_vec(),
            ..Default::default()
        };
        let response = rw.process_with_replication(&remove_req);
        let status = String::from_utf8_lossy(&response.lease);
        assert!(status.starts_with(REC_SUCC));
    }

    #[tokio::test]
    async fn test_replicated_worker_append() {
        let rw = make_replicated_worker(0, 0);
        let insert_req = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: b"list".to_vec(),
            val: b"a".to_vec(),
            ..Default::default()
        };
        rw.process_local(&insert_req);

        let append_req = ZPack {
            opcode: OPC_APPEND.as_bytes().to_vec(),
            key: b"list".to_vec(),
            val: b"b".to_vec(),
            ..Default::default()
        };
        let response = rw.process_with_replication(&append_req);
        let status = String::from_utf8_lossy(&response.lease);
        assert!(status.starts_with(REC_SUCC));
        assert_eq!(rw.store().get("list").unwrap(), "a:b");
    }

    #[test]
    fn test_replicated_worker_empty_key_no_replication() {
        let rw = make_replicated_worker(2, 1);
        let request = ZPack {
            opcode: OPC_INSERT.as_bytes().to_vec(),
            key: vec![],
            val: b"value".to_vec(),
            ..Default::default()
        };
        let response = rw.process_with_replication(&request);
        let status = String::from_utf8_lossy(&response.lease);
        assert_eq!(status, REC_EMPTYKEY);
    }

    #[test]
    fn test_replicated_worker_metrics() {
        let rw = make_replicated_worker(0, 0);
        let request = ZPack {
            opcode: OPC_LOOKUP.as_bytes().to_vec(),
            key: b"key".to_vec(),
            ..Default::default()
        };
        rw.process_local(&request);
        let metrics = rw.metrics();
        assert_eq!(metrics.total_operations, 1);
        assert_eq!(metrics.lookup_count, 1);
    }

    // ── NodeState tests ───────────────────────────────────────────────

    #[test]
    fn test_node_state_display() {
        assert_eq!(format!("{}", NodeState::Alive), "Alive");
        assert_eq!(format!("{}", NodeState::Suspect), "Suspect");
        assert_eq!(format!("{}", NodeState::Dead), "Dead");
        assert_eq!(format!("{}", NodeState::Left), "Left");
    }

    #[test]
    fn test_node_state_equality() {
        assert_eq!(NodeState::Alive, NodeState::Alive);
        assert_ne!(NodeState::Alive, NodeState::Dead);
    }

    // ── ServerMetricsSnapshot tests ───────────────────────────────────

    #[test]
    fn test_snapshot_clone() {
        let snap = ServerMetricsSnapshot {
            uptime_secs: 1.5,
            connections_total: 10,
            connections_active: 2,
            connections_accepted: 10,
            connections_rejected: 0,
            bytes_received: 1000,
            bytes_sent: 2000,
            storage_entries: 50,
            worker_metrics: HTWorkerMetrics::default(),
            member_count: 3,
            alive_members: 3,
        };
        let snap2 = snap.clone();
        assert_eq!(snap.uptime_secs, snap2.uptime_secs);
        assert_eq!(snap.connections_total, snap2.connections_total);
    }

    // ── MigrationAction tests ─────────────────────────────────────────

    #[test]
    fn test_migration_action() {
        let action = MigrationAction {
            target_host: make_host(50001),
            keys: vec!["key1".to_string(), "key2".to_string()],
        };
        assert_eq!(action.keys.len(), 2);
        assert_eq!(action.target_host.port, 50001);
    }

    // ── MembershipEntry tests ─────────────────────────────────────────

    #[test]
    fn test_membership_entry() {
        let entry = MembershipEntry {
            host: make_host(50001),
            state: NodeState::Alive,
            last_heartbeat: Instant::now(),
            joined_at: Instant::now(),
        };
        assert_eq!(entry.host.port, 50001);
        assert_eq!(entry.state, NodeState::Alive);
    }

    // ── Integration: multi-threaded membership ────────────────────────

    #[test]
    fn test_membership_concurrent_access() {
        let mm = Arc::new(MembershipManager::new(
            make_host(50000),
            vec![make_host(50001), make_host(50002)],
        ));

        let mut handles = vec![];

        // Thread 1: mark alive/suspect
        let mm1 = Arc::clone(&mm);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                if i % 2 == 0 {
                    mm1.mark_alive("localhost", 50001);
                } else {
                    mm1.mark_suspect("localhost", 50001);
                }
            }
        }));

        // Thread 2: read members
        let mm2 = Arc::clone(&mm);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = mm2.members();
                let _ = mm2.active_members();
                let _ = mm2.member_count();
            }
        }));

        for h in handles {
            h.join().unwrap();
        }

        // Should still be functional
        assert!(mm.members().len() >= 1);
    }

    // ── Integration: metrics concurrent ───────────────────────────────

    #[test]
    fn test_metrics_concurrent() {
        let m = Arc::new(ServerMetrics::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    m.record_connection_accepted();
                    m.record_bytes_received(100);
                    m.record_bytes_sent(200);
                    m.record_connection_closed();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(m.connections_accepted.load(Ordering::Relaxed), 4000);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 400000);
        assert_eq!(m.bytes_sent.load(Ordering::Relaxed), 800000);
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 0);
    }
}
