use std::net::SocketAddr;

use serde::Deserialize;

/// Cluster configuration, typically loaded from the `[cluster]` section of a
/// TOML config file.
///
/// ```toml
/// [cluster]
/// enabled = true
/// node_id = "node-1"
/// bind_address = "0.0.0.0:9443"
/// seed_nodes = ["node-1:9443", "node-2:9443", "node-3:9443"]
/// heartbeat_interval_ms = 5000
/// heartbeat_miss_threshold = 3
/// election_timeout_ms = 10000
/// gossip_interval_ms = 1000
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    /// Whether clustering is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Unique identifier for this node within the cluster.
    #[serde(default = "default_node_id")]
    pub node_id: String,

    /// Address this node listens on for cluster traffic.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Static list of all cluster node addresses (`host:port`).
    /// Deprecated: use `seed_nodes` instead. If `seed_nodes` is empty,
    /// `nodes` is used as the seed list for backward compatibility.
    #[serde(default)]
    pub nodes: Vec<String>,

    /// Seed nodes for dynamic cluster membership. New nodes join by
    /// contacting any seed node. Minimum 1 for single-node, 3 for production.
    #[serde(default)]
    pub seed_nodes: Vec<String>,

    /// Heartbeat interval in milliseconds (default: 5000).
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// Number of missed heartbeats before a node is considered disconnected
    /// (default: 3).
    #[serde(default = "default_heartbeat_miss_threshold")]
    pub heartbeat_miss_threshold: u32,

    /// Election timeout in milliseconds (default: 10000).
    /// A random jitter of 0..election_timeout_ms is added to prevent
    /// simultaneous elections.
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,

    /// Gossip protocol interval in milliseconds (default: 1000).
    /// Controls how often SWIM membership probes are sent.
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,

    /// Number of random peers to gossip membership updates to per cycle
    /// (default: 3).
    #[serde(default = "default_gossip_fanout")]
    pub gossip_fanout: usize,

    /// Number of peers to ask for indirect probing when direct probe fails
    /// (default: 3).
    #[serde(default = "default_indirect_probe_count")]
    pub indirect_probe_count: usize,

    /// Time in milliseconds a node stays in suspect state before being
    /// declared dead (default: 5000).
    #[serde(default = "default_suspicion_timeout_ms")]
    pub suspicion_timeout_ms: u64,

    /// Whether split-brain quorum protection is enabled (default: true).
    #[serde(default = "default_quorum_enabled")]
    pub quorum_enabled: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: default_node_id(),
            bind_address: default_bind_address(),
            nodes: Vec::new(),
            seed_nodes: Vec::new(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            heartbeat_miss_threshold: default_heartbeat_miss_threshold(),
            election_timeout_ms: default_election_timeout_ms(),
            gossip_interval_ms: default_gossip_interval_ms(),
            gossip_fanout: default_gossip_fanout(),
            indirect_probe_count: default_indirect_probe_count(),
            suspicion_timeout_ms: default_suspicion_timeout_ms(),
            quorum_enabled: default_quorum_enabled(),
        }
    }
}

impl ClusterConfig {
    /// Parse the bind address into a `SocketAddr`.
    ///
    /// Returns `None` if the address is invalid.
    pub fn bind_socket_addr(&self) -> Option<SocketAddr> {
        self.bind_address.parse().ok()
    }

    /// Resolve peer addresses (all configured nodes except this node).
    ///
    /// Uses `seed_nodes` if available, otherwise falls back to `nodes`.
    pub fn peer_addresses(&self) -> Vec<String> {
        self.effective_seed_nodes()
            .into_iter()
            .filter(|addr| !self.is_self(addr))
            .collect()
    }

    /// Returns the effective seed node list. Prefers `seed_nodes` if non-empty,
    /// otherwise falls back to `nodes` for backward compatibility.
    pub fn effective_seed_nodes(&self) -> Vec<String> {
        if !self.seed_nodes.is_empty() {
            self.seed_nodes.clone()
        } else {
            self.nodes.clone()
        }
    }

    /// Check whether a given address string refers to this node.
    ///
    /// Matches by node_id prefix (e.g. "node-1:9443" starts with "node-1")
    /// or by exact bind_address match.
    fn is_self(&self, addr: &str) -> bool {
        addr.starts_with(&self.node_id) || addr == self.bind_address
    }

    /// Validate the cluster configuration.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        if self.node_id.is_empty() {
            return Err("cluster.node_id must not be empty".into());
        }

        if self.bind_address.is_empty() {
            return Err("cluster.bind_address must not be empty".into());
        }

        let effective = self.effective_seed_nodes();
        if effective.is_empty() {
            return Err(
                "cluster.seed_nodes (or cluster.nodes) must contain at least one address".into(),
            );
        }

        if effective.len() < 2 {
            return Err(
                "cluster.seed_nodes (or cluster.nodes) must contain at least 2 nodes for clustering"
                    .into(),
            );
        }

        if self.heartbeat_interval_ms == 0 {
            return Err("cluster.heartbeat_interval_ms must be > 0".into());
        }

        if self.heartbeat_miss_threshold == 0 {
            return Err("cluster.heartbeat_miss_threshold must be > 0".into());
        }

        if self.gossip_interval_ms == 0 {
            return Err("cluster.gossip_interval_ms must be > 0".into());
        }

        // Warn if both seed_nodes and nodes are provided.
        if !self.seed_nodes.is_empty() && !self.nodes.is_empty() {
            tracing::warn!(
                "Both cluster.seed_nodes and cluster.nodes are set; \
                 seed_nodes takes precedence, nodes is ignored"
            );
        }

        Ok(())
    }
}

fn default_node_id() -> String {
    "node-1".to_string()
}

fn default_bind_address() -> String {
    "0.0.0.0:9443".to_string()
}

fn default_heartbeat_interval_ms() -> u64 {
    5000
}

fn default_heartbeat_miss_threshold() -> u32 {
    3
}

fn default_election_timeout_ms() -> u64 {
    10_000
}

fn default_gossip_interval_ms() -> u64 {
    1000
}

fn default_gossip_fanout() -> usize {
    3
}

fn default_indirect_probe_count() -> usize {
    3
}

fn default_suspicion_timeout_ms() -> u64 {
    5000
}

fn default_quorum_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cluster_config_is_disabled() {
        let config = ClusterConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.heartbeat_interval_ms, 5000);
        assert_eq!(config.heartbeat_miss_threshold, 3);
        assert_eq!(config.gossip_interval_ms, 1000);
        assert_eq!(config.gossip_fanout, 3);
        assert_eq!(config.indirect_probe_count, 3);
        assert_eq!(config.suspicion_timeout_ms, 5000);
        assert!(config.quorum_enabled);
    }

    #[test]
    fn validate_disabled_cluster_always_ok() {
        let config = ClusterConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_enabled_requires_nodes() {
        let config = ClusterConfig {
            enabled: true,
            nodes: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_enabled_requires_two_nodes() {
        let config = ClusterConfig {
            enabled: true,
            nodes: vec!["node-1:9443".into()],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_enabled_with_two_nodes_ok() {
        let config = ClusterConfig {
            enabled: true,
            nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_seed_nodes_take_precedence() {
        let config = ClusterConfig {
            enabled: true,
            nodes: vec!["old-1:9443".into(), "old-2:9443".into()],
            seed_nodes: vec!["seed-1:9443".into(), "seed-2:9443".into()],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        let effective = config.effective_seed_nodes();
        assert_eq!(effective, vec!["seed-1:9443", "seed-2:9443"]);
    }

    #[test]
    fn effective_seed_nodes_fallback_to_nodes() {
        let config = ClusterConfig {
            nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };
        let effective = config.effective_seed_nodes();
        assert_eq!(effective, vec!["node-1:9443", "node-2:9443"]);
    }

    #[test]
    fn peer_addresses_excludes_self() {
        let config = ClusterConfig {
            node_id: "node-1".into(),
            nodes: vec![
                "node-1:9443".into(),
                "node-2:9443".into(),
                "node-3:9443".into(),
            ],
            ..Default::default()
        };
        let peers = config.peer_addresses();
        assert_eq!(peers.len(), 2);
        assert!(!peers.contains(&"node-1:9443".to_string()));
    }

    #[test]
    fn peer_addresses_uses_seed_nodes() {
        let config = ClusterConfig {
            node_id: "node-1".into(),
            seed_nodes: vec!["node-1:9443".into(), "node-2:9443".into()],
            ..Default::default()
        };
        let peers = config.peer_addresses();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], "node-2:9443");
    }

    #[test]
    fn deserialize_from_toml() {
        let toml_str = r#"
            enabled = true
            node_id = "node-2"
            bind_address = "0.0.0.0:9443"
            nodes = ["node-1:9443", "node-2:9443", "node-3:9443"]
            heartbeat_interval_ms = 3000
            heartbeat_miss_threshold = 5
        "#;
        let config: ClusterConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.node_id, "node-2");
        assert_eq!(config.nodes.len(), 3);
        assert_eq!(config.heartbeat_interval_ms, 3000);
        assert_eq!(config.heartbeat_miss_threshold, 5);
        // New defaults should be present
        assert_eq!(config.gossip_interval_ms, 1000);
        assert!(config.quorum_enabled);
    }

    #[test]
    fn deserialize_seed_nodes_from_toml() {
        let toml_str = r#"
            enabled = true
            node_id = "node-1"
            bind_address = "0.0.0.0:9443"
            seed_nodes = ["seed-1:9443", "seed-2:9443", "seed-3:9443"]
            gossip_interval_ms = 500
            gossip_fanout = 2
            indirect_probe_count = 2
            suspicion_timeout_ms = 3000
            quorum_enabled = false
        "#;
        let config: ClusterConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.seed_nodes.len(), 3);
        assert!(config.nodes.is_empty());
        assert_eq!(config.gossip_interval_ms, 500);
        assert_eq!(config.gossip_fanout, 2);
        assert_eq!(config.indirect_probe_count, 2);
        assert_eq!(config.suspicion_timeout_ms, 3000);
        assert!(!config.quorum_enabled);
    }
}
