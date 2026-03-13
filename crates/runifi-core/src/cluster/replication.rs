//! Flow configuration replication across cluster nodes.
//!
//! The coordinator is responsible for broadcasting flow configuration changes
//! to all connected nodes. Each flow update carries a monotonically increasing
//! version number. Nodes apply updates atomically — either the entire update
//! succeeds or it is rejected.
//!
//! Joining nodes request the current flow configuration via `FlowSyncRequest`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use super::node::{ClusterNodeId, NodeInfo, NodeState};
use super::protocol::{FlowUpdateAckData, FlowUpdateData};

/// Tracks flow replication state across the cluster.
///
/// The coordinator uses this to broadcast flow changes and track which nodes
/// have acknowledged each version.
pub struct FlowReplicator {
    /// Current flow configuration version (monotonically increasing).
    current_version: AtomicU64,

    /// Current flow configuration as TOML string.
    current_config: RwLock<String>,

    /// Shared node state map.
    nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>,

    /// Pending acknowledgements for in-flight updates.
    /// Maps version -> set of node IDs that have acknowledged.
    pending_acks: RwLock<HashMap<u64, ReplicationStatus>>,
}

/// Status of a flow replication round.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    /// The flow version being replicated.
    pub version: u64,
    /// Nodes that have acknowledged this version.
    pub acknowledged: Vec<ClusterNodeId>,
    /// Nodes that have not yet acknowledged.
    pub pending: Vec<ClusterNodeId>,
    /// Nodes that rejected the update.
    pub rejected: Vec<(ClusterNodeId, String)>,
}

impl FlowReplicator {
    /// Create a new flow replicator.
    pub fn new(nodes: Arc<RwLock<HashMap<ClusterNodeId, NodeInfo>>>) -> Self {
        Self {
            current_version: AtomicU64::new(0),
            current_config: RwLock::new(String::new()),
            nodes,
            pending_acks: RwLock::new(HashMap::new()),
        }
    }

    /// Get the current flow version.
    pub fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::Relaxed)
    }

    /// Get the current flow configuration.
    pub fn current_config(&self) -> String {
        self.current_config.read().clone()
    }

    /// Set the initial flow configuration (used at startup).
    pub fn set_initial_config(&self, config: String, version: u64) {
        self.current_version.store(version, Ordering::Relaxed);
        *self.current_config.write() = config;
    }

    /// Prepare a flow update for broadcast.
    ///
    /// Increments the version, stores the new config, and returns the update
    /// data to be sent to all peers. Also initializes the pending ack tracking.
    ///
    /// Returns `None` if there are no connected peers to replicate to.
    pub fn prepare_update(
        &self,
        new_config: String,
        coordinator_term: u64,
    ) -> Option<FlowUpdateData> {
        let new_version = self.current_version.fetch_add(1, Ordering::Relaxed) + 1;
        *self.current_config.write() = new_config.clone();

        // Determine which nodes need to acknowledge.
        let connected_peers: Vec<ClusterNodeId> = {
            let nodes = self.nodes.read();
            nodes
                .iter()
                .filter(|(_, n)| n.state == NodeState::Connected)
                .map(|(id, _)| id.clone())
                .collect()
        };

        if connected_peers.is_empty() {
            // Single-node cluster or no peers connected.
            return None;
        }

        let status = ReplicationStatus {
            version: new_version,
            acknowledged: Vec::new(),
            pending: connected_peers,
            rejected: Vec::new(),
        };

        self.pending_acks.write().insert(new_version, status);

        Some(FlowUpdateData {
            version: new_version,
            flow_config: new_config,
            coordinator_term,
        })
    }

    /// Record an acknowledgement from a peer node.
    ///
    /// Returns `true` if all nodes have acknowledged this version.
    pub fn record_ack(&self, ack: &FlowUpdateAckData, node_id: &ClusterNodeId) -> bool {
        let mut pending = self.pending_acks.write();
        if let Some(status) = pending.get_mut(&ack.version) {
            status.pending.retain(|id| id != node_id);

            if ack.success {
                status.acknowledged.push(node_id.clone());
            } else {
                status
                    .rejected
                    .push((node_id.clone(), ack.error.clone().unwrap_or_default()));
            }

            let all_done = status.pending.is_empty();
            if all_done {
                tracing::info!(
                    version = ack.version,
                    acked = status.acknowledged.len(),
                    rejected = status.rejected.len(),
                    "Flow replication round complete"
                );
            }
            all_done
        } else {
            // Unknown version — stale ack.
            false
        }
    }

    /// Apply a received flow update (on non-coordinator nodes).
    ///
    /// Returns `FlowUpdateAckData` indicating success or failure.
    pub fn apply_update(&self, update: &FlowUpdateData) -> FlowUpdateAckData {
        let current = self.current_version.load(Ordering::Relaxed);

        if update.version <= current {
            // Already have this version or newer.
            return FlowUpdateAckData {
                version: update.version,
                success: true,
                error: None,
            };
        }

        // Apply the update atomically.
        self.current_version
            .store(update.version, Ordering::Relaxed);
        *self.current_config.write() = update.flow_config.clone();

        tracing::info!(
            version = update.version,
            coordinator_term = update.coordinator_term,
            "Applied flow configuration update"
        );

        FlowUpdateAckData {
            version: update.version,
            success: true,
            error: None,
        }
    }

    /// Get the replication status for a specific version.
    pub fn replication_status(&self, version: u64) -> Option<ReplicationStatus> {
        self.pending_acks.read().get(&version).cloned()
    }

    /// Check whether all connected nodes have the current flow version.
    pub fn is_fully_replicated(&self) -> bool {
        let current = self.current_version.load(Ordering::Relaxed);
        let nodes = self.nodes.read();
        nodes
            .values()
            .filter(|n| n.state == NodeState::Connected)
            .all(|n| n.flow_version >= current)
    }

    /// Clean up old pending ack entries (keep only the most recent N versions).
    pub fn cleanup_old_acks(&self, keep_count: usize) {
        let mut pending = self.pending_acks.write();
        if pending.len() > keep_count {
            let mut versions: Vec<u64> = pending.keys().cloned().collect();
            versions.sort();
            let remove_count = versions.len() - keep_count;
            for version in versions.into_iter().take(remove_count) {
                pending.remove(&version);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn initial_state() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);
        assert_eq!(replicator.current_version(), 0);
        assert!(replicator.current_config().is_empty());
    }

    #[test]
    fn set_initial_config() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        replicator.set_initial_config("[flow]\nname = \"test\"".into(), 1);
        assert_eq!(replicator.current_version(), 1);
        assert!(replicator.current_config().contains("test"));
    }

    #[test]
    fn prepare_update_increments_version() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        let update = replicator.prepare_update("[flow]\nname = \"v1\"".into(), 1);
        assert!(update.is_some());
        let update = update.unwrap();
        assert_eq!(update.version, 1);
        assert_eq!(replicator.current_version(), 1);

        let update2 = replicator.prepare_update("[flow]\nname = \"v2\"".into(), 1);
        assert!(update2.is_some());
        assert_eq!(update2.unwrap().version, 2);
    }

    #[test]
    fn prepare_update_no_peers() {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let replicator = FlowReplicator::new(nodes);

        let update = replicator.prepare_update("[flow]".into(), 1);
        assert!(update.is_none()); // No connected peers.
    }

    #[test]
    fn ack_tracking() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        let _update = replicator.prepare_update("[flow]".into(), 1);

        // First ack — not complete yet.
        let ack = FlowUpdateAckData {
            version: 1,
            success: true,
            error: None,
        };
        assert!(!replicator.record_ack(&ack, &"node-1".into()));

        // Second ack — not complete yet.
        assert!(!replicator.record_ack(&ack, &"node-2".into()));

        // Third ack — all done.
        assert!(replicator.record_ack(&ack, &"node-3".into()));
    }

    #[test]
    fn ack_with_rejection() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        let _update = replicator.prepare_update("[flow]".into(), 1);

        // Success ack.
        let ack_ok = FlowUpdateAckData {
            version: 1,
            success: true,
            error: None,
        };
        replicator.record_ack(&ack_ok, &"node-1".into());

        // Rejection ack.
        let ack_fail = FlowUpdateAckData {
            version: 1,
            success: false,
            error: Some("parse error".into()),
        };
        replicator.record_ack(&ack_fail, &"node-2".into());

        // Last ack completes the round.
        assert!(replicator.record_ack(&ack_ok, &"node-3".into()));

        let status = replicator.replication_status(1).unwrap();
        assert_eq!(status.acknowledged.len(), 2);
        assert_eq!(status.rejected.len(), 1);
    }

    #[test]
    fn apply_update_stores_config() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        let update = FlowUpdateData {
            version: 5,
            flow_config: "[flow]\nname = \"remote\"".into(),
            coordinator_term: 3,
        };

        let ack = replicator.apply_update(&update);
        assert!(ack.success);
        assert_eq!(ack.version, 5);
        assert_eq!(replicator.current_version(), 5);
        assert!(replicator.current_config().contains("remote"));
    }

    #[test]
    fn apply_stale_update() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        replicator.set_initial_config("v10".into(), 10);

        let update = FlowUpdateData {
            version: 5, // Stale.
            flow_config: "old".into(),
            coordinator_term: 1,
        };

        let ack = replicator.apply_update(&update);
        assert!(ack.success); // Accepted silently.
        assert_eq!(replicator.current_version(), 10); // Not overwritten.
    }

    #[test]
    fn cleanup_old_acks() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes);

        for _ in 0..10 {
            replicator.prepare_update("[flow]".into(), 1);
        }

        assert_eq!(replicator.pending_acks.read().len(), 10);

        replicator.cleanup_old_acks(3);
        assert_eq!(replicator.pending_acks.read().len(), 3);
    }

    #[test]
    fn is_fully_replicated() {
        let nodes = make_test_nodes();
        let replicator = FlowReplicator::new(nodes.clone());

        replicator.set_initial_config("[flow]".into(), 5);

        // Not fully replicated yet (nodes still at version 0).
        assert!(!replicator.is_fully_replicated());

        // Update node versions.
        {
            let mut nodes_write = nodes.write();
            for node in nodes_write.values_mut() {
                node.flow_version = 5;
            }
        }

        assert!(replicator.is_fully_replicated());
    }
}
