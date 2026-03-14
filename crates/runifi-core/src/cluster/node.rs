use std::fmt;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::protocol::NodeMetricsSummary;

/// Unique identifier for a node within the cluster.
///
/// Wraps a `String` for human-readable node names (e.g. "node-1") while
/// providing cheap cloning via the inner `Arc<str>` inside `String`.
pub type ClusterNodeId = String;

/// Node lifecycle states, matching NiFi's cluster node model.
///
/// ```text
///   CONNECTING ──► CONNECTED ──► DISCONNECTING ──► DISCONNECTED
///       │              │               ▲                │
///       │              ▼               │                │
///       │         DECOMMISSIONING ─────┘                │
///       └───────────────────────────────────────────────┘
///              (reconnect after heartbeat recovery)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeState {
    /// Node is attempting to join the cluster.
    Connecting,
    /// Node is an active member of the cluster.
    Connected,
    /// Node is gracefully leaving the cluster.
    Disconnecting,
    /// Node is not part of the cluster.
    Disconnected,
    /// Node is being decommissioned — draining queues and stopping processors.
    Decommissioning,
}

impl NodeState {
    /// Returns `true` if the node is in a state where it can process data.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Returns `true` if the node is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Disconnected)
    }

    /// Returns the valid next states from the current state.
    pub fn valid_transitions(&self) -> &'static [NodeState] {
        match self {
            Self::Connecting => &[Self::Connected, Self::Disconnected],
            Self::Connected => &[
                Self::Disconnecting,
                Self::Disconnected,
                Self::Decommissioning,
            ],
            Self::Disconnecting => &[Self::Disconnected],
            Self::Disconnected => &[Self::Connecting],
            Self::Decommissioning => &[Self::Disconnected],
        }
    }

    /// Check whether a transition to the given target state is valid.
    pub fn can_transition_to(&self, target: NodeState) -> bool {
        self.valid_transitions().contains(&target)
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connecting => write!(f, "CONNECTING"),
            Self::Connected => write!(f, "CONNECTED"),
            Self::Disconnecting => write!(f, "DISCONNECTING"),
            Self::Disconnected => write!(f, "DISCONNECTED"),
            Self::Decommissioning => write!(f, "DECOMMISSIONING"),
        }
    }
}

/// Elected cluster roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClusterRole {
    /// Monitors heartbeats, replicates flow changes, manages membership.
    Coordinator,
    /// Runs processors marked "Primary Node Only".
    PrimaryNode,
}

impl fmt::Display for ClusterRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordinator => write!(f, "COORDINATOR"),
            Self::PrimaryNode => write!(f, "PRIMARY_NODE"),
        }
    }
}

/// Information about a node in the cluster, including its current state,
/// roles, and last heartbeat timestamp.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: ClusterNodeId,
    /// Network address for cluster communication.
    pub address: String,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Elected roles held by this node (can be multiple).
    pub roles: Vec<ClusterRole>,
    /// Timestamp of the last successful heartbeat received from this node.
    pub last_heartbeat: Option<Instant>,
    /// Number of consecutive missed heartbeats.
    pub missed_heartbeats: u32,
    /// Flow configuration version this node has applied.
    pub flow_version: u64,
    /// Latest metrics received from the node's heartbeat.
    pub metrics: Option<NodeMetricsSummary>,
    /// Node start time (for uptime calculation).
    pub start_time: Option<Instant>,
}

impl NodeInfo {
    /// Create a new `NodeInfo` in the `Connecting` state.
    pub fn new(id: ClusterNodeId, address: String) -> Self {
        Self {
            id,
            address,
            state: NodeState::Connecting,
            roles: Vec::new(),
            last_heartbeat: None,
            missed_heartbeats: 0,
            flow_version: 0,
            metrics: None,
            start_time: None,
        }
    }

    /// Attempt to transition to a new state. Returns `true` if the
    /// transition was valid and applied.
    pub fn transition_to(&mut self, target: NodeState) -> bool {
        if self.state.can_transition_to(target) {
            self.state = target;
            true
        } else {
            false
        }
    }

    /// Record a successful heartbeat, optionally updating cached metrics.
    pub fn record_heartbeat(&mut self, flow_version: u64, metrics: Option<NodeMetricsSummary>) {
        self.last_heartbeat = Some(Instant::now());
        self.missed_heartbeats = 0;
        self.flow_version = flow_version;
        if metrics.is_some() {
            self.metrics = metrics;
        }
    }

    /// Record a missed heartbeat. Returns `true` if the miss threshold
    /// has been exceeded (caller should disconnect the node).
    pub fn record_missed_heartbeat(&mut self, threshold: u32) -> bool {
        self.missed_heartbeats += 1;
        self.missed_heartbeats >= threshold
    }

    /// Check whether this node holds the coordinator role.
    pub fn is_coordinator(&self) -> bool {
        self.roles.contains(&ClusterRole::Coordinator)
    }

    /// Check whether this node holds the primary node role.
    pub fn is_primary(&self) -> bool {
        self.roles.contains(&ClusterRole::PrimaryNode)
    }

    /// Add a role to this node (idempotent).
    pub fn add_role(&mut self, role: ClusterRole) {
        if !self.roles.contains(&role) {
            self.roles.push(role);
        }
    }

    /// Remove a role from this node.
    pub fn remove_role(&mut self, role: ClusterRole) {
        self.roles.retain(|r| *r != role);
    }
}

/// Summary of the current cluster state, suitable for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    /// Whether clustering is enabled.
    pub enabled: bool,
    /// This node's identifier.
    pub node_id: ClusterNodeId,
    /// This node's current state.
    pub state: NodeState,
    /// This node's elected roles.
    pub roles: Vec<ClusterRole>,
    /// All known nodes in the cluster.
    pub nodes: Vec<NodeSummary>,
    /// Current flow configuration version.
    pub flow_version: u64,
    /// Current election term.
    pub election_term: u64,
}

/// Abbreviated node info for API responses.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSummary {
    pub id: ClusterNodeId,
    pub address: String,
    pub state: NodeState,
    pub roles: Vec<ClusterRole>,
    pub missed_heartbeats: u32,
    pub flow_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<NodeMetricsSummary>,
    /// Uptime in seconds, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_secs: Option<u64>,
}

impl From<&NodeInfo> for NodeSummary {
    fn from(info: &NodeInfo) -> Self {
        let uptime_secs = info.start_time.map(|t| t.elapsed().as_secs());
        Self {
            id: info.id.clone(),
            address: info.address.clone(),
            state: info.state,
            roles: info.roles.clone(),
            missed_heartbeats: info.missed_heartbeats,
            flow_version: info.flow_version,
            metrics: info.metrics.clone(),
            uptime_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_state_transitions() {
        assert!(NodeState::Connecting.can_transition_to(NodeState::Connected));
        assert!(NodeState::Connecting.can_transition_to(NodeState::Disconnected));
        assert!(!NodeState::Connecting.can_transition_to(NodeState::Disconnecting));

        assert!(NodeState::Connected.can_transition_to(NodeState::Disconnecting));
        assert!(NodeState::Connected.can_transition_to(NodeState::Disconnected));
        assert!(NodeState::Connected.can_transition_to(NodeState::Decommissioning));
        assert!(!NodeState::Connected.can_transition_to(NodeState::Connecting));

        assert!(NodeState::Disconnecting.can_transition_to(NodeState::Disconnected));
        assert!(!NodeState::Disconnecting.can_transition_to(NodeState::Connected));

        assert!(NodeState::Disconnected.can_transition_to(NodeState::Connecting));
        assert!(!NodeState::Disconnected.can_transition_to(NodeState::Connected));

        // Decommissioning can only transition to Disconnected.
        assert!(NodeState::Decommissioning.can_transition_to(NodeState::Disconnected));
        assert!(!NodeState::Decommissioning.can_transition_to(NodeState::Connected));
        assert!(!NodeState::Decommissioning.can_transition_to(NodeState::Connecting));
        assert!(!NodeState::Decommissioning.is_active());
    }

    #[test]
    fn node_info_lifecycle() {
        let mut node = NodeInfo::new("node-1".into(), "10.0.0.1:9443".into());
        assert_eq!(node.state, NodeState::Connecting);

        assert!(node.transition_to(NodeState::Connected));
        assert_eq!(node.state, NodeState::Connected);

        assert!(!node.transition_to(NodeState::Connecting));
        assert_eq!(node.state, NodeState::Connected);

        assert!(node.transition_to(NodeState::Disconnecting));
        assert!(node.transition_to(NodeState::Disconnected));
    }

    #[test]
    fn node_heartbeat_tracking() {
        let mut node = NodeInfo::new("node-1".into(), "10.0.0.1:9443".into());

        node.record_heartbeat(1, None);
        assert_eq!(node.missed_heartbeats, 0);
        assert!(node.last_heartbeat.is_some());

        assert!(!node.record_missed_heartbeat(3));
        assert!(!node.record_missed_heartbeat(3));
        assert!(node.record_missed_heartbeat(3));
    }

    #[test]
    fn node_heartbeat_stores_metrics() {
        let mut node = NodeInfo::new("node-1".into(), "10.0.0.1:9443".into());
        assert!(node.metrics.is_none());

        let m = NodeMetricsSummary {
            total_flowfiles: 100,
            active_processors: 4,
            system_load: Some(0.5),
        };
        node.record_heartbeat(1, Some(m));
        assert!(node.metrics.is_some());
        assert_eq!(node.metrics.as_ref().unwrap().total_flowfiles, 100);
    }

    #[test]
    fn node_roles() {
        let mut node = NodeInfo::new("node-1".into(), "10.0.0.1:9443".into());
        assert!(!node.is_coordinator());
        assert!(!node.is_primary());

        node.add_role(ClusterRole::Coordinator);
        assert!(node.is_coordinator());

        node.add_role(ClusterRole::Coordinator); // idempotent
        assert_eq!(node.roles.len(), 1);

        node.add_role(ClusterRole::PrimaryNode);
        assert!(node.is_primary());
        assert_eq!(node.roles.len(), 2);

        node.remove_role(ClusterRole::Coordinator);
        assert!(!node.is_coordinator());
        assert!(node.is_primary());
    }

    #[test]
    fn node_state_display() {
        assert_eq!(NodeState::Connecting.to_string(), "CONNECTING");
        assert_eq!(NodeState::Connected.to_string(), "CONNECTED");
        assert_eq!(NodeState::Disconnecting.to_string(), "DISCONNECTING");
        assert_eq!(NodeState::Disconnected.to_string(), "DISCONNECTED");
        assert_eq!(NodeState::Decommissioning.to_string(), "DECOMMISSIONING");
    }

    #[test]
    fn node_summary_from_info() {
        let mut info = NodeInfo::new("node-1".into(), "10.0.0.1:9443".into());
        info.flow_version = 5;
        info.add_role(ClusterRole::Coordinator);
        let summary = NodeSummary::from(&info);
        assert_eq!(summary.id, "node-1");
        assert_eq!(summary.flow_version, 5);
        assert_eq!(summary.roles.len(), 1);
    }
}
