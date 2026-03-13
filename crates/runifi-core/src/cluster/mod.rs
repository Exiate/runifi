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
//! ## Phase 1 scope
//!
//! - Static cluster membership via configuration
//! - Heartbeat mechanism (5 s interval, 3 missed = disconnect)
//! - Coordinator / Primary Node election
//! - Flow configuration replication
//! - Connection load-balancing strategy types

pub mod config;
pub mod coordinator;
pub mod election;
pub mod heartbeat;
pub mod load_balance;
pub mod node;
pub mod protocol;
pub mod replication;

pub use config::ClusterConfig;
pub use coordinator::ClusterCoordinator;
pub use election::{ElectionRole, ElectionState};
pub use heartbeat::HeartbeatManager;
pub use load_balance::LoadBalanceStrategy;
pub use node::{ClusterNodeId, NodeInfo, NodeState};
pub use replication::FlowReplicator;
