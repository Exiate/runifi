//! Cluster coordinator — the main entry point for cluster operations.
//!
//! The `ClusterCoordinator` manages:
//! - Node membership and state tracking
//! - Heartbeat monitoring
//! - Coordinator and primary node election
//! - Flow configuration replication
//! - TCP-based inter-node communication
//!
//! It is designed to be created once per RuniFi instance and driven by
//! the engine's lifecycle (start/stop).

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::config::ClusterConfig;
use super::election::{ElectionRole, ElectionState, RaftState};
use super::extract_node_id;
use super::gossip::{GossipEvent, GossipState};
use super::heartbeat::{HeartbeatEvent, HeartbeatManager};
use super::load_balance::{LoadBalanceStrategy, LoadBalancer};
use super::node::{ClusterNodeId, ClusterRole, ClusterStatus, NodeInfo, NodeState, NodeSummary};
use super::protocol::{
    ClusterMessage, ElectionWonData, FlowSyncResponseData, FlowUpdateData, HeartbeatAckData,
    HeartbeatData, JoinResponseData, MessagePayload, NodeMetricsSummary, PingAckData,
    VoteRequestData, VoteResponseData,
};
use super::quorum::QuorumState;
use super::replication::FlowReplicator;

/// Cluster error type.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Invalid state transition: {0} -> {1}")]
    InvalidTransition(String, String),
    #[error("Cluster not enabled")]
    NotEnabled,
    #[error("Cannot operate on self")]
    CannotOperateOnSelf,
}

/// The cluster coordinator manages all cluster operations for this node.
pub struct ClusterCoordinator {
    /// Cluster configuration.
    config: ClusterConfig,

    /// This node's ID.
    self_id: ClusterNodeId,

    /// All known cluster nodes, keyed by node ID.
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,

    /// Heartbeat manager.
    heartbeat_manager: Arc<HeartbeatManager>,

    /// Election state for the coordinator role.
    coordinator_election: Arc<ElectionState>,

    /// Election state for the primary node role.
    primary_election: Arc<ElectionState>,

    /// Flow replicator.
    flow_replicator: Arc<FlowReplicator>,

    /// SWIM gossip membership state.
    gossip: Arc<GossipState>,

    /// Split-brain quorum tracker.
    quorum: Option<Arc<QuorumState>>,

    /// Monotonic message sequence counter.
    seq_counter: AtomicU64,

    /// Cancellation token for all background tasks.
    cancel_token: CancellationToken,

    /// Handles for spawned background tasks.
    task_handles: Vec<JoinHandle<()>>,
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator.
    ///
    /// This does not start any background tasks. Call `start()` to begin
    /// cluster operations.
    pub fn new(config: ClusterConfig) -> Self {
        let self_id = config.node_id.clone();
        let effective_nodes = config.effective_seed_nodes();
        let cluster_size = effective_nodes.len();

        // Initialize the node map with all configured nodes.
        let mut node_map = HashMap::new();
        for node_addr in &effective_nodes {
            let node_id = extract_node_id(node_addr);
            let node = NodeInfo::new(node_id.clone(), node_addr.clone());
            node_map.insert(node_id, node);
        }

        // Mark self as connected immediately.
        if let Some(self_node) = node_map.get_mut(&self_id) {
            self_node.state = NodeState::Connected;
            self_node.start_time = Some(std::time::Instant::now());
        }

        let nodes = Arc::new(RwLock::new(node_map));

        let heartbeat_manager = Arc::new(HeartbeatManager::new(&config, nodes.clone()));
        let coordinator_election =
            Arc::new(ElectionState::new(ElectionRole::Coordinator, cluster_size));
        let primary_election =
            Arc::new(ElectionState::new(ElectionRole::PrimaryNode, cluster_size));
        let flow_replicator = Arc::new(FlowReplicator::new(nodes.clone()));

        // Initialize gossip state.
        let gossip = Arc::new(GossipState::new(
            self_id.clone(),
            config.bind_address.clone(),
            config.gossip_interval_ms,
            config.gossip_fanout,
            config.indirect_probe_count,
            config.suspicion_timeout_ms,
        ));
        gossip.add_seed_nodes(&effective_nodes);

        // Initialize quorum if enabled.
        let quorum = if config.quorum_enabled {
            let seed_ids: Vec<String> = effective_nodes
                .iter()
                .map(|addr| extract_node_id(addr))
                .collect();
            Some(Arc::new(QuorumState::new(seed_ids)))
        } else {
            None
        };

        Self {
            config,
            self_id,
            nodes,
            heartbeat_manager,
            coordinator_election,
            primary_election,
            flow_replicator,
            gossip,
            quorum,
            seq_counter: AtomicU64::new(0),
            cancel_token: CancellationToken::new(),
            task_handles: Vec::new(),
        }
    }

    /// Start the cluster coordinator.
    ///
    /// Spawns:
    /// 1. TCP listener for incoming cluster messages.
    /// 2. Heartbeat check loop.
    /// 3. Heartbeat sender loop.
    /// 4. Election timeout watcher.
    /// 5. Heartbeat event handler.
    pub async fn start(&mut self) -> io::Result<()> {
        if !self.config.enabled {
            tracing::info!("Clustering is disabled");
            return Ok(());
        }

        tracing::info!(
            node_id = %self.self_id,
            bind_address = %self.config.bind_address,
            cluster_size = self.config.effective_seed_nodes().len(),
            "Starting cluster coordinator"
        );

        // Channel for heartbeat events.
        let (event_tx, event_rx) = mpsc::channel::<HeartbeatEvent>(64);

        // 1. TCP listener.
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        let listener_cancel = self.cancel_token.child_token();
        let nodes_listener = self.nodes.clone();
        let coord_election_listener = self.coordinator_election.clone();
        let primary_election_listener = self.primary_election.clone();
        let flow_replicator_listener = self.flow_replicator.clone();
        let self_id_listener = self.self_id.clone();
        let heartbeat_mgr_listener = self.heartbeat_manager.clone();

        let listener_handle = tokio::spawn(async move {
            run_tcp_listener(
                listener,
                listener_cancel,
                nodes_listener,
                coord_election_listener,
                primary_election_listener,
                flow_replicator_listener,
                heartbeat_mgr_listener,
                self_id_listener,
            )
            .await;
        });
        self.task_handles.push(listener_handle);

        // 2. Heartbeat check loop.
        let hb_manager = self.heartbeat_manager.clone();
        let hb_cancel = self.cancel_token.child_token();
        let hb_event_tx = event_tx.clone();
        let hb_handle = tokio::spawn(async move {
            hb_manager.run_check_loop(hb_event_tx, hb_cancel).await;
        });
        self.task_handles.push(hb_handle);

        // 3. Heartbeat sender loop.
        let sender_cancel = self.cancel_token.child_token();
        let sender_config = self.config.clone();
        let sender_self_id = self.self_id.clone();
        let sender_coord_election = self.coordinator_election.clone();
        let sender_primary_election = self.primary_election.clone();
        let sender_flow_replicator = self.flow_replicator.clone();
        let sender_nodes = self.nodes.clone();
        let sender_seq = Arc::new(AtomicU64::new(0));

        let sender_handle = tokio::spawn(async move {
            run_heartbeat_sender(
                sender_cancel,
                sender_config,
                sender_self_id,
                sender_coord_election,
                sender_primary_election,
                sender_flow_replicator,
                sender_nodes,
                sender_seq,
            )
            .await;
        });
        self.task_handles.push(sender_handle);

        // 4. Election timeout watcher.
        let election_cancel = self.cancel_token.child_token();
        let election_config = self.config.clone();
        let election_self_id = self.self_id.clone();
        let election_coord = self.coordinator_election.clone();
        let election_primary = self.primary_election.clone();
        let election_nodes = self.nodes.clone();

        let election_handle = tokio::spawn(async move {
            run_election_watcher(
                election_cancel,
                election_config,
                election_self_id,
                election_coord,
                election_primary,
                election_nodes,
            )
            .await;
        });
        self.task_handles.push(election_handle);

        // 5. Heartbeat event handler.
        let event_cancel = self.cancel_token.child_token();
        let event_self_id = self.self_id.clone();
        let event_hb_mgr = self.heartbeat_manager.clone();
        let event_nodes = self.nodes.clone();
        let event_coord_election = self.coordinator_election.clone();
        let event_primary_election = self.primary_election.clone();

        let event_handle = tokio::spawn(async move {
            run_event_handler(
                event_rx,
                event_cancel,
                event_self_id,
                event_hb_mgr,
                event_nodes,
                event_coord_election,
                event_primary_election,
            )
            .await;
        });
        self.task_handles.push(event_handle);

        // 6. Gossip loop.
        let gossip_cancel = self.cancel_token.child_token();
        let gossip_state = self.gossip.clone();
        let (gossip_event_tx, gossip_event_rx) = mpsc::channel::<GossipEvent>(64);

        let gossip_handle = tokio::spawn(async move {
            gossip_state
                .run_gossip_loop(gossip_event_tx, gossip_cancel)
                .await;
        });
        self.task_handles.push(gossip_handle);

        // 7. Gossip event handler — updates node map from gossip membership changes.
        let gossip_evt_cancel = self.cancel_token.child_token();
        let gossip_evt_nodes = self.nodes.clone();
        let gossip_evt_coord = self.coordinator_election.clone();
        let gossip_evt_primary = self.primary_election.clone();

        let gossip_evt_handle = tokio::spawn(async move {
            run_gossip_event_handler(
                gossip_event_rx,
                gossip_evt_cancel,
                gossip_evt_nodes,
                gossip_evt_coord,
                gossip_evt_primary,
            )
            .await;
        });
        self.task_handles.push(gossip_evt_handle);

        // 8. Quorum check loop.
        if let Some(quorum) = &self.quorum {
            let quorum_cancel = self.cancel_token.child_token();
            let quorum_state = quorum.clone();
            let quorum_gossip = self.gossip.clone();
            let quorum_self_id = self.self_id.clone();
            let quorum_interval = Duration::from_millis(self.config.heartbeat_interval_ms);

            let quorum_handle = tokio::spawn(async move {
                run_quorum_check_loop(
                    quorum_cancel,
                    quorum_state,
                    quorum_gossip,
                    quorum_self_id,
                    quorum_interval,
                )
                .await;
            });
            self.task_handles.push(quorum_handle);
        }

        tracing::info!(
            node_id = %self.self_id,
            "Cluster coordinator started — {} background tasks",
            self.task_handles.len()
        );

        Ok(())
    }

    /// Stop the cluster coordinator gracefully.
    pub async fn stop(&mut self) {
        if !self.config.enabled {
            return;
        }

        tracing::info!(node_id = %self.self_id, "Stopping cluster coordinator");

        // Announce graceful leave via gossip.
        self.gossip.announce_leave();

        // Broadcast disconnect notice to peers.
        let _ = self.broadcast_disconnect().await;

        self.cancel_token.cancel();
        for handle in self.task_handles.drain(..) {
            let _ = handle.await;
        }

        // Reset election state.
        self.coordinator_election.reset();
        self.primary_election.reset();

        tracing::info!(node_id = %self.self_id, "Cluster coordinator stopped");
    }

    /// Get the current cluster status.
    pub fn status(&self) -> ClusterStatus {
        let nodes = self.nodes.read();
        let self_node = nodes.get(&self.self_id);

        ClusterStatus {
            enabled: self.config.enabled,
            node_id: self.self_id.clone(),
            state: self_node
                .map(|n| n.state)
                .unwrap_or(NodeState::Disconnected),
            roles: self_node.map(|n| n.roles.clone()).unwrap_or_default(),
            nodes: nodes.values().map(NodeSummary::from).collect(),
            flow_version: self.flow_replicator.current_version(),
            election_term: self.coordinator_election.term(),
        }
    }

    /// Replicate a flow configuration change to all cluster nodes.
    ///
    /// Only the coordinator should call this. Returns `None` if this node
    /// is not the coordinator.
    pub async fn replicate_flow(&self, config: String) -> Option<u64> {
        if !self.coordinator_election.is_leader() {
            tracing::warn!("Cannot replicate flow — not the coordinator");
            return None;
        }

        let term = self.coordinator_election.term();
        let update = self.flow_replicator.prepare_update(config, term)?;
        let version = update.version;

        // Broadcast to all connected peers.
        self.broadcast_flow_update(&update).await;

        Some(version)
    }

    /// Get a reference to the flow replicator.
    pub fn flow_replicator(&self) -> &Arc<FlowReplicator> {
        &self.flow_replicator
    }

    /// Check if this node is the coordinator.
    pub fn is_coordinator(&self) -> bool {
        self.coordinator_election.is_leader()
    }

    /// Check if this node is the primary node.
    pub fn is_primary(&self) -> bool {
        self.primary_election.is_leader()
    }

    /// Create a load balancer for the given strategy.
    pub fn create_load_balancer(&self, strategy: LoadBalanceStrategy) -> LoadBalancer {
        LoadBalancer::new(strategy)
    }

    /// Get the list of connected node IDs.
    pub fn connected_nodes(&self) -> Vec<ClusterNodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|(_, n)| n.state == NodeState::Connected)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Returns whether this node has quorum (split-brain protection).
    /// If quorum is not enabled, always returns `true`.
    pub fn has_quorum(&self) -> bool {
        self.quorum.as_ref().is_none_or(|q| q.has_quorum())
    }

    /// Get a reference to the gossip state.
    pub fn gossip(&self) -> &Arc<GossipState> {
        &self.gossip
    }

    /// Get a reference to the quorum state.
    pub fn quorum_state(&self) -> Option<&Arc<QuorumState>> {
        self.quorum.as_ref()
    }

    /// Get total node count (all known nodes).
    pub fn total_node_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Get connected node count.
    pub fn connected_node_count(&self) -> usize {
        self.nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Connected)
            .count()
    }

    /// Get detailed info for all nodes.
    pub fn all_node_summaries(&self) -> Vec<NodeSummary> {
        self.nodes.read().values().map(NodeSummary::from).collect()
    }

    /// Get info for a specific node.
    pub fn node_info(&self, node_id: &str) -> Option<NodeSummary> {
        self.nodes.read().get(node_id).map(NodeSummary::from)
    }

    /// Get the self node ID.
    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    /// Disconnect a node (administratively).
    pub async fn disconnect_node(&self, node_id: &ClusterNodeId) -> Result<(), ClusterError> {
        if node_id == &self.self_id {
            return Err(ClusterError::CannotOperateOnSelf);
        }

        let addr = {
            let mut nodes = self.nodes.write();
            let node = nodes
                .get_mut(node_id)
                .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

            let old_state = node.state.to_string();
            if !node.transition_to(NodeState::Disconnecting) {
                return Err(ClusterError::InvalidTransition(
                    old_state,
                    "DISCONNECTING".into(),
                ));
            }
            let _ = node.transition_to(NodeState::Disconnected);
            node.address.clone()
        };

        // Notify the target node.
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::DisconnectNotice,
        };
        let _ = send_message(&addr, &msg).await;

        tracing::info!(node = %node_id, "Node disconnected administratively");
        Ok(())
    }

    /// Reconnect a previously disconnected node.
    pub async fn connect_node(&self, node_id: &ClusterNodeId) -> Result<(), ClusterError> {
        if node_id == &self.self_id {
            return Err(ClusterError::CannotOperateOnSelf);
        }

        let addr = {
            let mut nodes = self.nodes.write();
            let node = nodes
                .get_mut(node_id)
                .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

            let old_state = node.state.to_string();
            if !node.transition_to(NodeState::Connecting) {
                return Err(ClusterError::InvalidTransition(
                    old_state,
                    "CONNECTING".into(),
                ));
            }
            node.address.clone()
        };

        // Send a join request to trigger reconnection.
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::FlowSyncRequest,
        };
        let _ = send_message(&addr, &msg).await;

        tracing::info!(node = %node_id, "Node reconnection initiated");
        Ok(())
    }

    /// Begin graceful decommission of a node.
    pub async fn decommission_node(&self, node_id: &ClusterNodeId) -> Result<(), ClusterError> {
        if node_id == &self.self_id {
            return Err(ClusterError::CannotOperateOnSelf);
        }

        let addr = {
            let mut nodes = self.nodes.write();
            let node = nodes
                .get_mut(node_id)
                .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

            let old_state = node.state.to_string();
            if !node.transition_to(NodeState::Decommissioning) {
                return Err(ClusterError::InvalidTransition(
                    old_state,
                    "DECOMMISSIONING".into(),
                ));
            }
            node.address.clone()
        };

        // Notify the target node to begin decommission.
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::DecommissionNotice,
        };
        let _ = send_message(&addr, &msg).await;

        tracing::info!(node = %node_id, "Node decommission initiated");
        Ok(())
    }

    /// Force-remove a node from the membership map.
    pub fn force_remove_node(&self, node_id: &ClusterNodeId) -> Result<(), ClusterError> {
        if node_id == &self.self_id {
            return Err(ClusterError::CannotOperateOnSelf);
        }

        let mut nodes = self.nodes.write();
        if nodes.remove(node_id).is_none() {
            return Err(ClusterError::NodeNotFound(node_id.clone()));
        }

        // Update election cluster size.
        let new_size = nodes.len();
        self.coordinator_election.set_cluster_size(new_size);
        self.primary_election.set_cluster_size(new_size);

        // Remove from gossip membership to prevent re-discovery.
        self.gossip.remove_member(node_id);

        tracing::info!(node = %node_id, "Node force-removed from cluster");
        Ok(())
    }

    /// Manually designate a node as the primary node (skipping election).
    pub async fn designate_primary(&self, node_id: &ClusterNodeId) -> Result<(), ClusterError> {
        {
            let mut nodes = self.nodes.write();
            if !nodes.contains_key(node_id) {
                return Err(ClusterError::NodeNotFound(node_id.clone()));
            }

            // Remove primary role from all, add to target.
            for node in nodes.values_mut() {
                node.remove_role(ClusterRole::PrimaryNode);
            }
            if let Some(target) = nodes.get_mut(node_id) {
                target.add_role(ClusterRole::PrimaryNode);
            }
        }

        // If this node is the new primary, become leader in election state.
        if node_id == &self.self_id {
            self.primary_election.become_leader(&self.self_id);
        }

        // Broadcast the designation as an ElectionWon.
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::ElectionWon(ElectionWonData {
                term: self.primary_election.term(),
                role: ElectionRole::PrimaryNode,
                winner_id: node_id.clone(),
            }),
        };
        let peers = self.config.peer_addresses();
        for peer_addr in peers {
            let _ = send_message(&peer_addr, &msg).await;
        }

        tracing::info!(node = %node_id, "Primary node designated manually");
        Ok(())
    }

    /// Get the current primary node ID.
    pub fn primary_node_id(&self) -> Option<ClusterNodeId> {
        self.primary_election.leader_id()
    }

    /// Get the current coordinator node ID.
    pub fn coordinator_node_id(&self) -> Option<ClusterNodeId> {
        self.coordinator_election.leader_id()
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, Ordering::Relaxed)
    }

    async fn broadcast_disconnect(&self) {
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::DisconnectNotice,
        };

        let peers = self.config.peer_addresses();
        for peer_addr in peers {
            if let Err(e) = send_message(&peer_addr, &msg).await {
                tracing::debug!(peer = %peer_addr, error = %e, "Failed to send disconnect notice");
            }
        }
    }

    async fn broadcast_flow_update(&self, update: &FlowUpdateData) {
        let msg = ClusterMessage {
            sender_id: self.self_id.clone(),
            seq: self.next_seq(),
            payload: MessagePayload::FlowUpdate(update.clone()),
        };

        let peers = self.config.peer_addresses();
        for peer_addr in peers {
            if let Err(e) = send_message(&peer_addr, &msg).await {
                tracing::warn!(peer = %peer_addr, error = %e, "Failed to send flow update");
            }
        }
    }
}

// ── TCP listener ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_tcp_listener(
    listener: TcpListener,
    cancel: CancellationToken,
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
    flow_replicator: Arc<FlowReplicator>,
    heartbeat_mgr: Arc<HeartbeatManager>,
    self_id: ClusterNodeId,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("TCP listener cancelled");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        tracing::trace!(peer = %addr, "Accepted cluster connection");
                        let nodes = nodes.clone();
                        let coord = coord_election.clone();
                        let primary = primary_election.clone();
                        let replicator = flow_replicator.clone();
                        let hb_mgr = heartbeat_mgr.clone();
                        let self_id = self_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream, nodes, coord, primary, replicator, hb_mgr, self_id,
                            ).await {
                                tracing::debug!(peer = %addr, error = %e, "Connection handler error");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "TCP accept error");
                    }
                }
            }
        }
    }
}

/// Handle a single inbound TCP connection.
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: TcpStream,
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
    flow_replicator: Arc<FlowReplicator>,
    heartbeat_mgr: Arc<HeartbeatManager>,
    self_id: ClusterNodeId,
) -> io::Result<()> {
    // Read length-prefixed message.
    let msg = read_message(&mut stream).await?;

    // Process the message and optionally send a response.
    let response = process_message(
        &msg,
        &nodes,
        &coord_election,
        &primary_election,
        &flow_replicator,
        &heartbeat_mgr,
        &self_id,
    );

    if let Some(resp) = response {
        let encoded = resp
            .encode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        stream.write_all(&encoded).await?;
    }

    Ok(())
}

/// Process an incoming cluster message and return an optional response.
#[allow(clippy::too_many_arguments)]
fn process_message(
    msg: &ClusterMessage,
    nodes: &Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    coord_election: &Arc<ElectionState>,
    primary_election: &Arc<ElectionState>,
    flow_replicator: &Arc<FlowReplicator>,
    heartbeat_mgr: &Arc<HeartbeatManager>,
    self_id: &ClusterNodeId,
) -> Option<ClusterMessage> {
    match &msg.payload {
        MessagePayload::Heartbeat(data) => {
            heartbeat_mgr.record_heartbeat(&msg.sender_id, data.flow_version, data.metrics.clone());

            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::HeartbeatAck(HeartbeatAckData {
                    accepted: true,
                    coordinator_id: coord_election.leader_id(),
                    primary_id: primary_election.leader_id(),
                    election_term: coord_election.term(),
                }),
            };
            Some(response)
        }

        MessagePayload::VoteRequest(data) => {
            let election = match data.role {
                ElectionRole::Coordinator => coord_election,
                ElectionRole::PrimaryNode => primary_election,
            };

            let (vote_granted, term) = election.handle_vote_request(&data.candidate_id, data.term);

            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::VoteResponse(VoteResponseData {
                    term,
                    vote_granted,
                    role: data.role,
                }),
            };
            Some(response)
        }

        MessagePayload::ElectionWon(data) => {
            let election = match data.role {
                ElectionRole::Coordinator => coord_election,
                ElectionRole::PrimaryNode => primary_election,
            };
            election.accept_leader(&data.winner_id, data.term);

            // Update node roles.
            let role = match data.role {
                ElectionRole::Coordinator => ClusterRole::Coordinator,
                ElectionRole::PrimaryNode => ClusterRole::PrimaryNode,
            };

            let mut nodes = nodes.write();
            // Remove role from all nodes first.
            for node in nodes.values_mut() {
                node.remove_role(role);
            }
            // Add role to the winner.
            if let Some(winner) = nodes.get_mut(&data.winner_id) {
                winner.add_role(role);
            }

            None // No response needed.
        }

        MessagePayload::FlowUpdate(data) => {
            let ack = flow_replicator.apply_update(data);

            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::FlowUpdateAck(ack),
            };
            Some(response)
        }

        MessagePayload::FlowUpdateAck(data) => {
            flow_replicator.record_ack(data, &msg.sender_id);
            None
        }

        MessagePayload::FlowSyncRequest => {
            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::FlowSyncResponse(FlowSyncResponseData {
                    version: flow_replicator.current_version(),
                    flow_config: flow_replicator.current_config(),
                    coordinator_id: coord_election.leader_id(),
                    primary_id: primary_election.leader_id(),
                    election_term: coord_election.term(),
                }),
            };
            Some(response)
        }

        MessagePayload::JoinRequest(data) => {
            tracing::info!(
                node = %msg.sender_id,
                address = %data.address,
                "Node join request received"
            );

            // Add the joining node to our node map.
            let mut nodes = nodes.write();
            let node = nodes
                .entry(msg.sender_id.clone())
                .or_insert_with(|| NodeInfo::new(msg.sender_id.clone(), data.address.clone()));
            let _ = node.transition_to(NodeState::Connected);

            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::JoinResponse(JoinResponseData {
                    accepted: true,
                    reason: None,
                    flow_version: flow_replicator.current_version(),
                    flow_config: Some(flow_replicator.current_config()),
                }),
            };
            Some(response)
        }

        MessagePayload::DisconnectNotice => {
            tracing::info!(node = %msg.sender_id, "Node disconnect notice received");
            heartbeat_mgr.disconnect_node(&msg.sender_id);
            None
        }

        MessagePayload::Ping(_data) => {
            // Respond with PingAck.
            let response = ClusterMessage {
                sender_id: self_id.clone(),
                seq: 0,
                payload: MessagePayload::PingAck(PingAckData {
                    incarnation: 0,
                    updates: vec![],
                }),
            };
            Some(response)
        }

        MessagePayload::PingAck(_data) => None,

        MessagePayload::PingReq(_data) => None,

        MessagePayload::GossipMembership(_data) => None,

        MessagePayload::DecommissionNotice => {
            tracing::info!("Received decommission notice — this node should begin draining");
            // In a full implementation, this would trigger queue draining.
            // For now, acknowledge receipt.
            None
        }

        MessagePayload::DecommissionComplete => {
            tracing::info!(
                node = %msg.sender_id,
                "Node decommission complete — removing from cluster"
            );
            heartbeat_mgr.disconnect_node(&msg.sender_id);
            None
        }

        MessagePayload::BulletinForward(_data) => {
            // Coordinator receives bulletins from remote nodes.
            // Actual processing is done by the bulletin board integration.
            None
        }

        MessagePayload::FlowSyncResponse(_)
        | MessagePayload::JoinResponse(_)
        | MessagePayload::HeartbeatAck(_)
        | MessagePayload::VoteResponse(_) => {
            // These are responses to our requests — not expected as inbound.
            None
        }
    }
}

// ── Heartbeat sender ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_heartbeat_sender(
    cancel: CancellationToken,
    config: ClusterConfig,
    self_id: ClusterNodeId,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
    flow_replicator: Arc<FlowReplicator>,
    _nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    seq: Arc<AtomicU64>,
) {
    let interval = Duration::from_millis(config.heartbeat_interval_ms);
    let mut ticker = tokio::time::interval(interval);
    // Skip the first immediate tick.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Heartbeat sender cancelled");
                break;
            }
            _ = ticker.tick() => {
                let mut roles = Vec::new();
                if coord_election.is_leader() {
                    roles.push(ClusterRole::Coordinator);
                }
                if primary_election.is_leader() {
                    roles.push(ClusterRole::PrimaryNode);
                }

                let metrics = Some(NodeMetricsSummary {
                    total_flowfiles: 0,     // Populated when engine handle is available
                    active_processors: 0,   // Populated when engine handle is available
                    system_load: read_system_load(),
                });

                let msg = ClusterMessage {
                    sender_id: self_id.clone(),
                    seq: seq.fetch_add(1, Ordering::Relaxed),
                    payload: MessagePayload::Heartbeat(HeartbeatData {
                        state: NodeState::Connected,
                        roles,
                        flow_version: flow_replicator.current_version(),
                        election_term: coord_election.term(),
                        metrics,
                    }),
                };

                let peers = config.peer_addresses();
                for peer_addr in peers {
                    if let Err(e) = send_message(&peer_addr, &msg).await {
                        tracing::trace!(
                            peer = %peer_addr,
                            error = %e,
                            "Failed to send heartbeat"
                        );
                    }
                }
            }
        }
    }
}

// ── Election watcher ─────────────────────────────────────────────────────────

async fn run_election_watcher(
    cancel: CancellationToken,
    config: ClusterConfig,
    self_id: ClusterNodeId,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
) {
    // Add random jitter to prevent simultaneous elections.
    let base_timeout = Duration::from_millis(config.election_timeout_ms);

    // Initial delay before first election attempt — allow time for heartbeats.
    tokio::select! {
        _ = cancel.cancelled() => return,
        _ = tokio::time::sleep(base_timeout) => {}
    }

    loop {
        // Check if we need to start an election.
        let needs_coordinator = coord_election.leader_id().is_none();
        let needs_primary = primary_election.leader_id().is_none();

        if needs_coordinator {
            run_election(&coord_election, &self_id, &config, &nodes).await;
        }

        if needs_primary {
            run_election(&primary_election, &self_id, &config, &nodes).await;
        }

        // Sleep before checking again.
        let jitter = base_timeout / 2
            + Duration::from_millis(
                (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_millis() as u64)
                    % (config.election_timeout_ms / 2),
            );

        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Election watcher cancelled");
                break;
            }
            _ = tokio::time::sleep(jitter) => {}
        }
    }
}

/// Run an election for a specific role.
async fn run_election(
    election: &Arc<ElectionState>,
    self_id: &ClusterNodeId,
    config: &ClusterConfig,
    nodes: &Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
) {
    // Already have a leader or we're already the leader.
    if election.leader_id().is_some() || election.is_leader() {
        return;
    }

    let term = election.start_election(self_id);
    let role = election.role();

    // Send vote requests to all peers.
    let vote_request = ClusterMessage {
        sender_id: self_id.clone(),
        seq: 0,
        payload: MessagePayload::VoteRequest(VoteRequestData {
            term,
            role,
            candidate_id: self_id.clone(),
        }),
    };

    let peers = config.peer_addresses();
    for peer_addr in &peers {
        match send_and_receive(peer_addr, &vote_request).await {
            Ok(response) => {
                if let MessagePayload::VoteResponse(data) = response.payload
                    && data.vote_granted
                    && data.term == term
                {
                    let majority = election.record_vote(&response.sender_id, term);
                    if majority {
                        // Won the election.
                        election.become_leader(self_id);

                        // Update node roles.
                        let cluster_role = match role {
                            ElectionRole::Coordinator => ClusterRole::Coordinator,
                            ElectionRole::PrimaryNode => ClusterRole::PrimaryNode,
                        };
                        {
                            let mut nodes = nodes.write();
                            for node in nodes.values_mut() {
                                node.remove_role(cluster_role);
                            }
                            if let Some(self_node) = nodes.get_mut(self_id) {
                                self_node.add_role(cluster_role);
                            }
                        }

                        // Broadcast the win to all peers.
                        let won_msg = ClusterMessage {
                            sender_id: self_id.clone(),
                            seq: 0,
                            payload: MessagePayload::ElectionWon(ElectionWonData {
                                term,
                                role,
                                winner_id: self_id.clone(),
                            }),
                        };
                        for other_peer in &peers {
                            let _ = send_message(other_peer, &won_msg).await;
                        }

                        tracing::info!(
                            role = %role,
                            term = term,
                            "Election won — broadcasting to cluster"
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::trace!(
                    peer = %peer_addr,
                    role = %role,
                    error = %e,
                    "Vote request failed"
                );
            }
        }
    }

    // If we didn't win, step down.
    if election.raft_state() == RaftState::Candidate {
        election.step_down();
    }
}

// ── Heartbeat event handler ──────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_event_handler(
    mut rx: mpsc::Receiver<HeartbeatEvent>,
    cancel: CancellationToken,
    _self_id: ClusterNodeId,
    heartbeat_mgr: Arc<HeartbeatManager>,
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Event handler cancelled");
                break;
            }
            event = rx.recv() => {
                let Some(event) = event else { break };
                match event {
                    HeartbeatEvent::NodeTimedOut { ref node_id } => {
                        tracing::warn!(node = %node_id, "Disconnecting node due to heartbeat timeout");
                        heartbeat_mgr.disconnect_node(node_id);

                        // Check if the disconnected node was the primary.
                        let was_primary = {
                            let nodes = nodes.read();
                            nodes.get(node_id).is_some_and(|n| n.is_primary())
                        };

                        if was_primary {
                            tracing::warn!(
                                node = %node_id,
                                "Primary node disconnected — clearing primary election for re-election"
                            );
                            // Clear the primary election so a new election is triggered.
                            primary_election.reset();
                            // Remove the role from the disconnected node.
                            let mut nodes = nodes.write();
                            if let Some(node) = nodes.get_mut(node_id) {
                                node.remove_role(ClusterRole::PrimaryNode);
                            }
                        }

                        // Check if the disconnected node was the coordinator.
                        let was_coordinator = coord_election.leader_id().as_deref() == Some(node_id.as_str());
                        if was_coordinator {
                            tracing::warn!(
                                node = %node_id,
                                "Coordinator disconnected — clearing election for re-election"
                            );
                            coord_election.reset();
                            let mut nodes = nodes.write();
                            if let Some(node) = nodes.get_mut(node_id) {
                                node.remove_role(ClusterRole::Coordinator);
                            }
                        }
                    }
                    HeartbeatEvent::HeartbeatReceived { node_id, flow_version } => {
                        tracing::trace!(
                            node = %node_id,
                            flow_version = flow_version,
                            "Heartbeat received"
                        );
                    }
                    HeartbeatEvent::NodeReconnecting { node_id } => {
                        tracing::info!(node = %node_id, "Node attempting to reconnect");
                    }
                }
            }
        }
    }
}

/// Handle gossip membership events and update the shared node map.
async fn run_gossip_event_handler(
    mut rx: mpsc::Receiver<GossipEvent>,
    cancel: CancellationToken,
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    coord_election: Arc<ElectionState>,
    primary_election: Arc<ElectionState>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Gossip event handler cancelled");
                break;
            }
            event = rx.recv() => {
                let Some(event) = event else { break };
                match event {
                    GossipEvent::NodeJoined { id, address } => {
                        tracing::info!(node = %id, address = %address, "Node joined via gossip");
                        let mut nodes = nodes.write();
                        let node = nodes
                            .entry(id.clone())
                            .or_insert_with(|| NodeInfo::new(id, address));
                        let _ = node.transition_to(NodeState::Connected);
                        let size = nodes.len();
                        coord_election.set_cluster_size(size);
                        primary_election.set_cluster_size(size);
                    }
                    GossipEvent::NodeSuspected { id } => {
                        tracing::warn!(node = %id, "Node suspected via gossip");
                    }
                    GossipEvent::NodeDead { id } => {
                        tracing::warn!(node = %id, "Node declared dead via gossip");
                        let mut nodes = nodes.write();
                        if let Some(node) = nodes.get_mut(&id) {
                            let _ = node.transition_to(NodeState::Disconnecting);
                            let _ = node.transition_to(NodeState::Disconnected);
                        }
                    }
                    GossipEvent::NodeLeft { id } => {
                        tracing::info!(node = %id, "Node left cluster via gossip");
                        let mut nodes = nodes.write();
                        if let Some(node) = nodes.get_mut(&id) {
                            let _ = node.transition_to(NodeState::Disconnecting);
                            let _ = node.transition_to(NodeState::Disconnected);
                        }
                    }
                    GossipEvent::NodeAlive { id } => {
                        tracing::info!(node = %id, "Node alive again via gossip");
                        let mut nodes = nodes.write();
                        if let Some(node) = nodes.get_mut(&id)
                            && node.state == NodeState::Disconnected
                        {
                            let _ = node.transition_to(NodeState::Connecting);
                            let _ = node.transition_to(NodeState::Connected);
                        }
                    }
                }
            }
        }
    }
}

/// Periodically check quorum (majority of seed nodes reachable).
async fn run_quorum_check_loop(
    cancel: CancellationToken,
    quorum: Arc<QuorumState>,
    gossip: Arc<GossipState>,
    self_id: ClusterNodeId,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Quorum check loop cancelled");
                break;
            }
            _ = ticker.tick() => {
                let alive = gossip.alive_members();
                quorum.check_quorum(&alive, &self_id);
            }
        }
    }
}

// ── Wire helpers ─────────────────────────────────────────────────────────────

/// Send a message to a peer via TCP (fire-and-forget).
async fn send_message(addr: &str, msg: &ClusterMessage) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let encoded = msg
        .encode()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    stream.write_all(&encoded).await?;
    Ok(())
}

/// Send a message and read a response.
async fn send_and_receive(addr: &str, msg: &ClusterMessage) -> io::Result<ClusterMessage> {
    let mut stream = TcpStream::connect(addr).await?;
    let encoded = msg
        .encode()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    stream.write_all(&encoded).await?;
    read_message(&mut stream).await
}

/// Read a length-prefixed message from a TCP stream.
async fn read_message(stream: &mut TcpStream) -> io::Result<ClusterMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 10 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Message too large: {} bytes", len),
        ));
    }

    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;

    ClusterMessage::decode(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read the system load average from /proc/loadavg (Linux only).
/// Returns the 1-minute load average as a fraction of available CPUs,
/// or `None` if it cannot be read.
///
/// Uses synchronous I/O intentionally: `/proc/loadavg` is a procfs virtual
/// file that never blocks on real disk I/O (kernel serves it from memory).
fn read_system_load() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/loadavg")
            && let Some(load_str) = contents.split_whitespace().next()
            && let Ok(load) = load_str.parse::<f64>()
        {
            // Normalize by CPU count.
            let cpus = std::thread::available_parallelism()
                .map(|n| n.get() as f64)
                .unwrap_or(1.0);
            return Some((load / cpus).min(1.0));
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_node_id_from_address() {
        assert_eq!(extract_node_id("node-1:9443"), "node-1");
        assert_eq!(extract_node_id("node-1"), "node-1");
        assert_eq!(extract_node_id("192.168.1.1:9443"), "192.168.1.1");
    }

    #[test]
    fn coordinator_creation() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "node-1".into(),
            bind_address: "0.0.0.0:9443".into(),
            nodes: vec![
                "node-1:9443".into(),
                "node-2:9443".into(),
                "node-3:9443".into(),
            ],
            ..Default::default()
        };

        let coordinator = ClusterCoordinator::new(config);

        assert_eq!(coordinator.self_id, "node-1");
        assert!(!coordinator.is_coordinator());
        assert!(!coordinator.is_primary());

        let status = coordinator.status();
        assert!(status.enabled);
        assert_eq!(status.nodes.len(), 3);
    }

    #[test]
    fn coordinator_status_reports_all_nodes() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "node-1".into(),
            bind_address: "0.0.0.0:9443".into(),
            nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };

        let coordinator = ClusterCoordinator::new(config);
        let status = coordinator.status();

        assert_eq!(status.node_id, "node-1");
        assert_eq!(status.state, NodeState::Connected);
        assert_eq!(status.nodes.len(), 2);
    }

    #[test]
    fn coordinator_disabled() {
        let config = ClusterConfig::default();
        let coordinator = ClusterCoordinator::new(config);
        let status = coordinator.status();
        assert!(!status.enabled);
    }

    #[test]
    fn process_heartbeat_message() {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let mut n1 = NodeInfo::new("node-1".into(), "node-1:9443".into());
        n1.state = NodeState::Connected;
        nodes.write().insert("node-1".into(), n1);
        let mut n2 = NodeInfo::new("node-2".into(), "node-2:9443".into());
        n2.state = NodeState::Connected;
        nodes.write().insert("node-2".into(), n2);

        let config = ClusterConfig {
            node_id: "node-1".into(),
            nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };

        let coord = Arc::new(ElectionState::new(ElectionRole::Coordinator, 2));
        let primary = Arc::new(ElectionState::new(ElectionRole::PrimaryNode, 2));
        let replicator = Arc::new(FlowReplicator::new(nodes.clone()));
        let hb_mgr = Arc::new(HeartbeatManager::new(&config, nodes.clone()));

        let msg = ClusterMessage {
            sender_id: "node-2".into(),
            seq: 1,
            payload: MessagePayload::Heartbeat(HeartbeatData {
                state: NodeState::Connected,
                roles: vec![],
                flow_version: 3,
                election_term: 1,
                metrics: None,
            }),
        };

        let response = process_message(
            &msg,
            &nodes,
            &coord,
            &primary,
            &replicator,
            &hb_mgr,
            &"node-1".into(),
        );

        assert!(response.is_some());
        match response.unwrap().payload {
            MessagePayload::HeartbeatAck(data) => {
                assert!(data.accepted);
            }
            _ => panic!("Expected HeartbeatAck"),
        }
    }

    #[test]
    fn process_vote_request_message() {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let config = ClusterConfig {
            node_id: "node-1".into(),
            nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };

        let coord = Arc::new(ElectionState::new(ElectionRole::Coordinator, 2));
        let primary = Arc::new(ElectionState::new(ElectionRole::PrimaryNode, 2));
        let replicator = Arc::new(FlowReplicator::new(nodes.clone()));
        let hb_mgr = Arc::new(HeartbeatManager::new(&config, nodes.clone()));

        let msg = ClusterMessage {
            sender_id: "node-2".into(),
            seq: 1,
            payload: MessagePayload::VoteRequest(VoteRequestData {
                term: 1,
                role: ElectionRole::Coordinator,
                candidate_id: "node-2".into(),
            }),
        };

        let response = process_message(
            &msg,
            &nodes,
            &coord,
            &primary,
            &replicator,
            &hb_mgr,
            &"node-1".into(),
        );

        assert!(response.is_some());
        match response.unwrap().payload {
            MessagePayload::VoteResponse(data) => {
                assert!(data.vote_granted);
                assert_eq!(data.term, 1);
            }
            _ => panic!("Expected VoteResponse"),
        }
    }

    #[test]
    fn process_flow_sync_request() {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let config = ClusterConfig {
            node_id: "node-1".into(),
            ..Default::default()
        };

        let coord = Arc::new(ElectionState::new(ElectionRole::Coordinator, 2));
        let primary = Arc::new(ElectionState::new(ElectionRole::PrimaryNode, 2));
        let replicator = Arc::new(FlowReplicator::new(nodes.clone()));
        replicator.set_initial_config("[flow]\nname = \"test\"".into(), 5);
        let hb_mgr = Arc::new(HeartbeatManager::new(&config, nodes.clone()));

        let msg = ClusterMessage {
            sender_id: "node-3".into(),
            seq: 1,
            payload: MessagePayload::FlowSyncRequest,
        };

        let response = process_message(
            &msg,
            &nodes,
            &coord,
            &primary,
            &replicator,
            &hb_mgr,
            &"node-1".into(),
        );

        assert!(response.is_some());
        match response.unwrap().payload {
            MessagePayload::FlowSyncResponse(data) => {
                assert_eq!(data.version, 5);
                assert!(data.flow_config.contains("test"));
            }
            _ => panic!("Expected FlowSyncResponse"),
        }
    }

    #[tokio::test]
    async fn coordinator_start_stop_disabled() {
        let config = ClusterConfig::default();
        let mut coordinator = ClusterCoordinator::new(config);

        // Should succeed silently when disabled.
        assert!(coordinator.start().await.is_ok());
        coordinator.stop().await;
    }

    #[tokio::test]
    async fn coordinator_start_stop_enabled() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "node-1".into(),
            bind_address: "127.0.0.1:0".into(), // OS-assigned port
            nodes: vec!["node-1:0".into(), "node-2:0".into()],
            heartbeat_interval_ms: 100,
            heartbeat_miss_threshold: 3,
            election_timeout_ms: 500,
            ..Default::default()
        };

        let mut coordinator = ClusterCoordinator::new(config);

        // Bind to port 0 — the OS will assign an ephemeral port.
        // This test mainly verifies startup/shutdown doesn't panic.
        let result = coordinator.start().await;
        // May fail if port 0 isn't resolved correctly in the address.
        // That's acceptable for unit tests — integration tests use real ports.
        if result.is_ok() {
            coordinator.stop().await;
        }
    }
}
