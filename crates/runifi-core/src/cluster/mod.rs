//! Multi-node clustering with zero-leader architecture.
//!
//! Implements NiFi-style clustering where every node performs identical work on
//! different data.  Two elected roles exist:
//!
//! - **Cluster Coordinator**: monitors heartbeats, replicates flow changes,
//!   manages node membership.
//! - **Primary Node**: runs processors marked "Primary Node Only".
//!
//! Election uses a lightweight Raft-inspired protocol over TCP.  The module is
//! designed so that a full `openraft` backend can be swapped in once it reaches
//! a stable release.
//!
//! ## Phase 1
//!
//! - Static cluster membership via configuration
//! - Heartbeat mechanism (5 s interval, 3 missed = disconnect)
//! - Coordinator / Primary Node election
//! - Flow configuration replication
//! - Connection load-balancing strategy types
//!
//! ## Phase 2
//!
//! - Dynamic membership via SWIM gossip protocol
//! - Seed-node based cluster bootstrap
//! - Node health dashboard with per-node metrics
//! - Graceful decommission workflow
//! - Split-brain protection via quorum checks
//! - Primary node management with automatic failover
//! - Cluster-wide bulletin aggregation

pub mod config;
pub mod coordinator;
pub mod election;
pub mod gossip;
pub mod heartbeat;
pub mod load_balance;
pub mod node;
pub mod protocol;
pub mod quorum;
pub mod replication;

pub use config::ClusterConfig;
pub use coordinator::ClusterCoordinator;
pub use election::{ElectionRole, ElectionState};
pub use gossip::GossipState;
pub use heartbeat::HeartbeatManager;
pub use load_balance::LoadBalanceStrategy;
pub use node::{ClusterNodeId, ClusterRole, NodeInfo, NodeState, NodeSummary};
pub use quorum::QuorumState;
pub use replication::FlowReplicator;

/// Extract a node ID from an address string like "node-1:9443".
pub(crate) fn extract_node_id(addr: &str) -> String {
    addr.split(':').next().unwrap_or(addr).to_string()
}
