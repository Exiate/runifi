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
/// nodes = ["node-1:9443", "node-2:9443", "node-3:9443"]
/// heartbeat_interval_ms = 5000
/// heartbeat_miss_threshold = 3
/// election_timeout_ms = 10000
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
    #[serde(default)]
    pub nodes: Vec<String>,

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
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: default_node_id(),
            bind_address: default_bind_address(),
            nodes: Vec::new(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            heartbeat_miss_threshold: default_heartbeat_miss_threshold(),
            election_timeout_ms: default_election_timeout_ms(),
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
    pub fn peer_addresses(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|addr| !self.is_self(addr))
            .cloned()
            .collect()
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

        if self.nodes.is_empty() {
            return Err("cluster.nodes must contain at least one node address".into());
        }

        if self.nodes.len() < 2 {
            return Err("cluster.nodes must contain at least 2 nodes for clustering".into());
        }

        if self.heartbeat_interval_ms == 0 {
            return Err("cluster.heartbeat_interval_ms must be > 0".into());
        }

        if self.heartbeat_miss_threshold == 0 {
            return Err("cluster.heartbeat_miss_threshold must be > 0".into());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cluster_config_is_disabled() {
        let config = ClusterConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.heartbeat_interval_ms, 5000);
        assert_eq!(config.heartbeat_miss_threshold, 3);
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
    }
}
