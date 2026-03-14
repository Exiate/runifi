//! Heartbeat mechanism for cluster health monitoring.
//!
//! Each node periodically sends heartbeats to all peers. The coordinator
//! monitors heartbeats and disconnects nodes that miss the configured
//! threshold (default: 3 consecutive misses at 5 s intervals = 15 s timeout).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::ClusterConfig;
use super::node::{ClusterNodeId, NodeInfo, NodeState};

/// Events emitted by the heartbeat manager for the coordinator to handle.
#[derive(Debug)]
pub enum HeartbeatEvent {
    /// A node has exceeded the missed heartbeat threshold and should be
    /// disconnected.
    NodeTimedOut { node_id: ClusterNodeId },

    /// A heartbeat was received from a node.
    HeartbeatReceived {
        node_id: ClusterNodeId,
        flow_version: u64,
    },

    /// A previously disconnected node has sent a heartbeat (possible rejoin).
    NodeReconnecting { node_id: ClusterNodeId },
}

/// Manages heartbeat tracking for all known cluster nodes.
///
/// The heartbeat manager runs a periodic check loop that:
/// 1. Increments the missed heartbeat count for all nodes that have not sent
///    a heartbeat since the last check.
/// 2. Emits `HeartbeatEvent::NodeTimedOut` for nodes exceeding the threshold.
///
/// Incoming heartbeats are recorded via `record_heartbeat()`.
pub struct HeartbeatManager {
    /// Heartbeat interval.
    interval: Duration,
    /// Number of missed heartbeats before disconnect.
    miss_threshold: u32,
    /// Shared node state map.
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    /// This node's ID.
    self_id: ClusterNodeId,
}

impl HeartbeatManager {
    /// Create a new heartbeat manager.
    pub fn new(
        config: &ClusterConfig,
        nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,
    ) -> Self {
        Self {
            interval: Duration::from_millis(config.heartbeat_interval_ms),
            miss_threshold: config.heartbeat_miss_threshold,
            nodes,
            self_id: config.node_id.clone(),
        }
    }

    /// Get the heartbeat interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Run the heartbeat check loop.
    ///
    /// This should be spawned as a tokio task. It periodically checks all
    /// tracked nodes and emits events for timed-out nodes.
    pub async fn run_check_loop(
        &self,
        event_tx: mpsc::Sender<HeartbeatEvent>,
        cancel: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(self.interval);
        // Skip the first immediate tick.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("Heartbeat check loop cancelled");
                    break;
                }
                _ = interval.tick() => {
                    self.check_heartbeats(&event_tx).await;
                }
            }
        }
    }

    /// Check all nodes for missed heartbeats.
    ///
    /// For each non-self connected node, we check whether the elapsed time
    /// since the last heartbeat exceeds `interval * miss_threshold`.  If a
    /// node has never sent a heartbeat we increment its miss counter each
    /// check cycle and disconnect once the threshold is reached.
    async fn check_heartbeats(&self, event_tx: &mpsc::Sender<HeartbeatEvent>) {
        let now = Instant::now();
        let timeout_duration = self.interval * self.miss_threshold;
        let mut timed_out = Vec::new();

        {
            let mut nodes = self.nodes.write();
            for (id, node) in nodes.iter_mut() {
                // Skip self.
                if *id == self.self_id {
                    continue;
                }

                // Only check connected/connecting nodes.
                if !matches!(node.state, NodeState::Connected | NodeState::Connecting) {
                    continue;
                }

                let exceeded = match node.last_heartbeat {
                    Some(last) => {
                        // Check if elapsed time exceeds the threshold.
                        if now.duration_since(last) > timeout_duration {
                            node.missed_heartbeats = self.miss_threshold;
                            true
                        } else {
                            false
                        }
                    }
                    None => {
                        // Never received a heartbeat — increment miss
                        // counter and check against threshold.
                        node.record_missed_heartbeat(self.miss_threshold)
                    }
                };

                if exceeded {
                    timed_out.push(id.clone());
                }
            }
        }

        for node_id in timed_out {
            tracing::warn!(
                node = %node_id,
                threshold = self.miss_threshold,
                "Node exceeded heartbeat miss threshold"
            );
            let _ = event_tx
                .send(HeartbeatEvent::NodeTimedOut {
                    node_id: node_id.clone(),
                })
                .await;
        }
    }

    /// Record a heartbeat received from a peer node.
    ///
    /// Returns `true` if this is a new/reconnecting node.
    pub fn record_heartbeat(
        &self,
        node_id: &ClusterNodeId,
        flow_version: u64,
        metrics: Option<super::protocol::NodeMetricsSummary>,
    ) -> bool {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            let was_disconnected = matches!(node.state, NodeState::Disconnected);
            node.record_heartbeat(flow_version, metrics);
            was_disconnected
        } else {
            false
        }
    }

    /// Disconnect a node (set state to DISCONNECTED).
    pub fn disconnect_node(&self, node_id: &ClusterNodeId) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id)
            && node.transition_to(NodeState::Disconnecting)
        {
            let _ = node.transition_to(NodeState::Disconnected);
            tracing::info!(node = %node_id, "Node disconnected due to heartbeat timeout");
        }
    }

    /// Get the current state of a node.
    pub fn node_state(&self, node_id: &ClusterNodeId) -> Option<NodeState> {
        self.nodes.read().get(node_id).map(|n| n.state)
    }

    /// Get the count of connected nodes (including self).
    pub fn connected_count(&self) -> usize {
        self.nodes
            .read()
            .values()
            .filter(|n| n.state == NodeState::Connected)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_config() -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            node_id: "node-1".into(),
            bind_address: "0.0.0.0:9443".into(),
            nodes: vec![
                "node-1:9443".into(),
                "node-2:9443".into(),
                "node-3:9443".into(),
            ],
            heartbeat_interval_ms: 100, // fast for tests
            heartbeat_miss_threshold: 3,
            election_timeout_ms: 500,
            ..Default::default()
        }
    }

    fn make_test_nodes() -> Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>> {
        let mut nodes = HashMap::new();
        let mut n1 = NodeInfo::new("node-1".into(), "node-1:9443".into());
        n1.state = NodeState::Connected;
        nodes.insert("node-1".into(), n1);

        let mut n2 = NodeInfo::new("node-2".into(), "node-2:9443".into());
        n2.state = NodeState::Connected;
        nodes.insert("node-2".into(), n2);

        let mut n3 = NodeInfo::new("node-3".into(), "node-3:9443".into());
        n3.state = NodeState::Connected;
        nodes.insert("node-3".into(), n3);

        Arc::new(RwLock::new(nodes))
    }

    #[test]
    fn heartbeat_manager_creation() {
        let config = make_test_config();
        let nodes = make_test_nodes();
        let manager = HeartbeatManager::new(&config, nodes);

        assert_eq!(manager.interval(), Duration::from_millis(100));
        assert_eq!(manager.connected_count(), 3);
    }

    #[test]
    fn record_heartbeat_updates_node() {
        let config = make_test_config();
        let nodes = make_test_nodes();
        let manager = HeartbeatManager::new(&config, nodes.clone());

        let was_reconnecting = manager.record_heartbeat(&"node-2".into(), 5, None);
        assert!(!was_reconnecting);

        let nodes_read = nodes.read();
        let n2 = nodes_read.get("node-2").unwrap();
        assert_eq!(n2.flow_version, 5);
        assert!(n2.last_heartbeat.is_some());
        assert_eq!(n2.missed_heartbeats, 0);
    }

    #[test]
    fn disconnect_node_transitions_state() {
        let config = make_test_config();
        let nodes = make_test_nodes();
        let manager = HeartbeatManager::new(&config, nodes.clone());

        manager.disconnect_node(&"node-2".into());

        assert_eq!(
            manager.node_state(&"node-2".into()),
            Some(NodeState::Disconnected)
        );
        assert_eq!(manager.connected_count(), 2);
    }

    #[test]
    fn record_heartbeat_from_disconnected_returns_true() {
        let config = make_test_config();
        let nodes = make_test_nodes();
        let manager = HeartbeatManager::new(&config, nodes.clone());

        // Disconnect node-2.
        manager.disconnect_node(&"node-2".into());

        // Heartbeat from disconnected node signals reconnection.
        let was_reconnecting = manager.record_heartbeat(&"node-2".into(), 1, None);
        assert!(was_reconnecting);
    }

    #[tokio::test]
    async fn check_loop_emits_timeout_events() {
        let config = make_test_config();
        let nodes = make_test_nodes();

        // Set node-2 to have a heartbeat far in the past (beyond threshold).
        {
            let mut nodes_write = nodes.write();
            let n2 = nodes_write.get_mut("node-2").unwrap();
            n2.last_heartbeat = Some(Instant::now() - Duration::from_secs(60));
            n2.missed_heartbeats = 0;
        }

        let manager = HeartbeatManager::new(&config, nodes);
        let (event_tx, mut event_rx) = mpsc::channel(10);

        // Run a single check.
        manager.check_heartbeats(&event_tx).await;

        // Should have timeout events (node-2 and node-3 both have stale heartbeats).
        // Use try_recv to avoid blocking.
        let mut timed_out_ids = Vec::new();
        while let Ok(event) = event_rx.try_recv() {
            match event {
                HeartbeatEvent::NodeTimedOut { node_id } => {
                    timed_out_ids.push(node_id);
                }
                _ => {}
            }
        }

        // node-2 had a heartbeat 60s ago (exceeds 300ms threshold).
        assert!(timed_out_ids.contains(&"node-2".to_string()));
    }
}
