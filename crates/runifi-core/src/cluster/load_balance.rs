//! Connection load-balancing strategies for distributing FlowFiles across
//! cluster nodes.
//!
//! When clustering is enabled, each connection can be configured with a load
//! balancing strategy that determines how FlowFiles are distributed across
//! the cluster.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use super::node::ClusterNodeId;

/// Load balancing strategy for distributing FlowFiles across cluster nodes.
///
/// ```toml
/// [flow.connections.load_balancing]
/// strategy = "round_robin"
/// compression = true
/// partition_attribute = "filename"   # only for partition_by_attribute
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    /// Data stays on the producing node (default).
    #[default]
    DoNotLoadBalance,

    /// Distribute FlowFiles evenly across all connected nodes in round-robin
    /// fashion.
    RoundRobin,

    /// Route FlowFiles to a deterministic node based on a hash of the
    /// specified attribute value. This ensures FlowFiles with the same
    /// attribute value always go to the same node.
    PartitionByAttribute {
        /// The FlowFile attribute name to partition on.
        attribute: String,
    },

    /// Send all FlowFiles to a single designated node.
    SingleNode {
        /// The target node ID. If `None`, the primary node is used.
        target_node: Option<ClusterNodeId>,
    },
}

/// Load balancing configuration for a connection (TOML-friendly).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LoadBalanceConfig {
    /// The strategy to use.
    #[serde(default)]
    pub strategy: LoadBalanceStrategy,
    /// Whether to compress FlowFile content during inter-node transfer.
    #[serde(default)]
    pub compression: bool,
}

/// Runtime load balancer that selects target nodes for FlowFiles.
///
/// This struct is `Send + Sync` and designed to be shared across processor
/// tasks via `Arc`.
pub struct LoadBalancer {
    strategy: LoadBalanceStrategy,
    /// Atomic counter for round-robin distribution.
    round_robin_counter: AtomicUsize,
}

impl LoadBalancer {
    /// Create a new load balancer with the given strategy.
    pub fn new(strategy: LoadBalanceStrategy) -> Self {
        Self {
            strategy,
            round_robin_counter: AtomicUsize::new(0),
        }
    }

    /// Select a target node for a FlowFile.
    ///
    /// # Arguments
    /// * `local_node_id` — This node's identifier.
    /// * `connected_nodes` — List of currently connected node IDs.
    /// * `attribute_value` — Value of the partition attribute (for
    ///   `PartitionByAttribute` strategy). `None` for other strategies.
    ///
    /// Returns the selected node ID, or `None` if no node is available.
    pub fn select_node(
        &self,
        local_node_id: &ClusterNodeId,
        connected_nodes: &[ClusterNodeId],
        attribute_value: Option<&str>,
    ) -> Option<ClusterNodeId> {
        if connected_nodes.is_empty() {
            return None;
        }

        match &self.strategy {
            LoadBalanceStrategy::DoNotLoadBalance => {
                // Always stay local.
                Some(local_node_id.clone())
            }

            LoadBalanceStrategy::RoundRobin => {
                let idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
                let target_idx = idx % connected_nodes.len();
                Some(connected_nodes[target_idx].clone())
            }

            LoadBalanceStrategy::PartitionByAttribute { attribute: _ } => {
                // Hash the attribute value to select a deterministic node.
                let value = attribute_value.unwrap_or("");
                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                let hash = hasher.finish();
                let target_idx = (hash as usize) % connected_nodes.len();
                Some(connected_nodes[target_idx].clone())
            }

            LoadBalanceStrategy::SingleNode { target_node } => {
                if let Some(target) = target_node {
                    // Use the specified target if it's connected.
                    if connected_nodes.contains(target) {
                        Some(target.clone())
                    } else {
                        // Target not connected — fall back to first connected.
                        Some(connected_nodes[0].clone())
                    }
                } else {
                    // No target specified — use the first connected node.
                    Some(connected_nodes[0].clone())
                }
            }
        }
    }

    /// Get the current strategy.
    pub fn strategy(&self) -> &LoadBalanceStrategy {
        &self.strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_nodes() -> Vec<ClusterNodeId> {
        vec![
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ]
    }

    #[test]
    fn do_not_load_balance_stays_local() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::DoNotLoadBalance);
        let nodes = test_nodes();

        for _ in 0..10 {
            let target = lb.select_node(&"node-1".into(), &nodes, None);
            assert_eq!(target, Some("node-1".to_string()));
        }
    }

    #[test]
    fn round_robin_distributes_evenly() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::RoundRobin);
        let nodes = test_nodes();
        let mut counts = [0u32; 3];

        for _ in 0..30 {
            let target = lb.select_node(&"node-1".into(), &nodes, None).unwrap();
            let idx = nodes.iter().position(|n| *n == target).unwrap();
            counts[idx] += 1;
        }

        // Each node should get exactly 10.
        assert_eq!(counts[0], 10);
        assert_eq!(counts[1], 10);
        assert_eq!(counts[2], 10);
    }

    #[test]
    fn partition_by_attribute_is_deterministic() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::PartitionByAttribute {
            attribute: "filename".into(),
        });
        let nodes = test_nodes();

        // Same attribute value always maps to the same node.
        let target1 = lb.select_node(&"node-1".into(), &nodes, Some("report.csv"));
        let target2 = lb.select_node(&"node-1".into(), &nodes, Some("report.csv"));
        assert_eq!(target1, target2);

        // Different attribute values may map to different nodes.
        let target_a = lb.select_node(&"node-1".into(), &nodes, Some("file-a.csv"));
        let target_b = lb.select_node(&"node-1".into(), &nodes, Some("file-b.csv"));
        // They might be the same or different — just verify they're valid.
        assert!(nodes.contains(&target_a.unwrap()));
        assert!(nodes.contains(&target_b.unwrap()));
    }

    #[test]
    fn partition_by_attribute_empty_value() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::PartitionByAttribute {
            attribute: "key".into(),
        });
        let nodes = test_nodes();

        let target = lb.select_node(&"node-1".into(), &nodes, None);
        assert!(target.is_some());
    }

    #[test]
    fn single_node_uses_target() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::SingleNode {
            target_node: Some("node-2".into()),
        });
        let nodes = test_nodes();

        for _ in 0..10 {
            let target = lb.select_node(&"node-1".into(), &nodes, None);
            assert_eq!(target, Some("node-2".to_string()));
        }
    }

    #[test]
    fn single_node_fallback_when_target_disconnected() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::SingleNode {
            target_node: Some("node-4".into()), // not in connected list
        });
        let nodes = test_nodes();

        let target = lb.select_node(&"node-1".into(), &nodes, None);
        assert_eq!(target, Some("node-1".to_string())); // falls back to first
    }

    #[test]
    fn single_node_no_target_uses_first() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::SingleNode { target_node: None });
        let nodes = test_nodes();

        let target = lb.select_node(&"node-1".into(), &nodes, None);
        assert_eq!(target, Some("node-1".to_string()));
    }

    #[test]
    fn empty_nodes_returns_none() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::RoundRobin);
        let target = lb.select_node(&"node-1".into(), &[], None);
        assert!(target.is_none());
    }

    #[test]
    fn default_strategy_is_do_not_load_balance() {
        assert_eq!(
            LoadBalanceStrategy::default(),
            LoadBalanceStrategy::DoNotLoadBalance
        );
    }

    #[test]
    fn load_balance_config_deserialize() {
        let toml_str = r#"
            strategy = "round_robin"
            compression = true
        "#;
        let config: LoadBalanceConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.strategy, LoadBalanceStrategy::RoundRobin);
        assert!(config.compression);
    }

    #[test]
    fn load_balance_config_default() {
        let config = LoadBalanceConfig::default();
        assert_eq!(config.strategy, LoadBalanceStrategy::DoNotLoadBalance);
        assert!(!config.compression);
    }
}
