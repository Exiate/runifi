//! Remote Process Group (RPG) data structures.
//!
//! An RPG represents a reference to a remote RuniFi instance on the flow canvas.
//! It holds configuration for Site-to-Site (S2S) communication and tracks the
//! status of remote input/output ports used for inter-instance data transfer.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

// ── ID generation ────────────────────────────────────────────────────────────

static RPG_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique RPG ID.
pub fn next_rpg_id() -> String {
    let id = RPG_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rpg-{}", id)
}

/// Reset the RPG ID counter (used when restoring persisted state).
pub fn reset_rpg_id_counter(next_id: u64) {
    RPG_ID_COUNTER.store(next_id, Ordering::Relaxed);
}

// ── Core types ───────────────────────────────────────────────────────────────

/// A Remote Process Group — represents a remote RuniFi instance on the canvas.
#[derive(Debug)]
pub struct RemoteProcessGroup {
    /// Unique identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Remote instance URLs (one or more for failover).
    pub target_uris: Vec<String>,
    /// Transport protocol ("QUIC").
    pub transport_protocol: String,
    /// Communications timeout in milliseconds.
    pub communications_timeout_ms: u64,
    /// Yield duration in milliseconds when no data is available.
    pub yield_duration_ms: u64,
    /// Maximum FlowFiles per batch.
    pub batch_count: usize,
    /// Maximum bytes per batch.
    pub batch_size_bytes: u64,
    /// Maximum batch duration in milliseconds.
    pub batch_duration_ms: u64,
    /// Optional proxy host.
    pub proxy_host: Option<String>,
    /// Optional proxy port.
    pub proxy_port: Option<u16>,
    /// Whether transmission is globally enabled for this RPG.
    pub transmitting: bool,
    /// Remote input ports discovered or configured.
    pub input_ports: Vec<RemotePortStatus>,
    /// Remote output ports discovered or configured.
    pub output_ports: Vec<RemotePortStatus>,
    /// Last time remote ports were refreshed from the remote instance.
    pub last_refresh: Option<Instant>,
    /// Authentication token for the remote instance.
    pub auth_token: Option<String>,
    /// Parent process group ID.
    pub parent_group_id: Option<String>,
    /// Canvas position (x, y).
    pub position: Option<(f64, f64)>,
    /// User comments.
    pub comments: String,
}

impl RemoteProcessGroup {
    /// Create a new RPG with the given name and target URIs.
    pub fn new(name: String, target_uris: Vec<String>) -> Self {
        Self {
            id: next_rpg_id(),
            name,
            target_uris,
            transport_protocol: "QUIC".to_string(),
            communications_timeout_ms: 30_000,
            yield_duration_ms: 1_000,
            batch_count: 100,
            batch_size_bytes: 5_000_000,
            batch_duration_ms: 5_000,
            proxy_host: None,
            proxy_port: None,
            transmitting: false,
            input_ports: Vec::new(),
            output_ports: Vec::new(),
            last_refresh: None,
            auth_token: None,
            parent_group_id: None,
            position: None,
            comments: String::new(),
        }
    }

    /// Aggregate metrics from all ports.
    pub fn metrics(&self) -> RemoteProcessGroupMetrics {
        let mut m = RemoteProcessGroupMetrics {
            bytes_sent: 0,
            bytes_received: 0,
            flow_files_sent: 0,
            flow_files_received: 0,
            active_thread_count: 0,
            transmission_status: if self.transmitting {
                TransmissionStatus::Transmitting
            } else {
                TransmissionStatus::Stopped
            },
        };

        for port in &self.input_ports {
            m.bytes_sent += port.bytes_sent.load(Ordering::Relaxed);
            m.bytes_received += port.bytes_received.load(Ordering::Relaxed);
            m.flow_files_sent += port.flow_files_sent.load(Ordering::Relaxed);
            m.flow_files_received += port.flow_files_received.load(Ordering::Relaxed);
            m.active_thread_count += port.active_threads.load(Ordering::Relaxed);
        }

        for port in &self.output_ports {
            m.bytes_sent += port.bytes_sent.load(Ordering::Relaxed);
            m.bytes_received += port.bytes_received.load(Ordering::Relaxed);
            m.flow_files_sent += port.flow_files_sent.load(Ordering::Relaxed);
            m.flow_files_received += port.flow_files_received.load(Ordering::Relaxed);
            m.active_thread_count += port.active_threads.load(Ordering::Relaxed);
        }

        m
    }
}

/// Status and metrics for a single remote port.
#[derive(Debug)]
pub struct RemotePortStatus {
    /// Unique identifier for this port mapping.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// ID of the port on the remote instance.
    pub target_id: Option<String>,
    /// Whether this port is connected to a local connection.
    pub connected: bool,
    /// Whether this individual port is enabled for transmission.
    pub transmitting: bool,
    /// Whether the port exists on the remote instance (verified on refresh).
    pub exists_on_remote: bool,
    // Metrics (atomics for lock-free concurrent access)
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub flow_files_sent: AtomicU64,
    pub flow_files_received: AtomicU64,
    pub active_threads: AtomicU32,
}

impl RemotePortStatus {
    /// Create a new port status with the given id and name.
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            target_id: None,
            connected: false,
            transmitting: false,
            exists_on_remote: false,
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            flow_files_sent: AtomicU64::new(0),
            flow_files_received: AtomicU64::new(0),
            active_threads: AtomicU32::new(0),
        }
    }
}

/// Aggregated metrics for a Remote Process Group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteProcessGroupMetrics {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub flow_files_sent: u64,
    pub flow_files_received: u64,
    pub active_thread_count: u32,
    pub transmission_status: TransmissionStatus,
}

/// Transmission status for an RPG.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransmissionStatus {
    Transmitting,
    Stopped,
    Error(String),
}

impl std::fmt::Display for TransmissionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransmissionStatus::Transmitting => write!(f, "TRANSMITTING"),
            TransmissionStatus::Stopped => write!(f, "STOPPED"),
            TransmissionStatus::Error(msg) => write!(f, "ERROR: {}", msg),
        }
    }
}

/// Cloneable snapshot of an RPG, suitable for API responses.
/// Unlike `RemoteProcessGroup` (which has atomics), this is a plain data struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteProcessGroupInfo {
    pub id: String,
    pub name: String,
    pub target_uris: Vec<String>,
    pub transport_protocol: String,
    pub communications_timeout_ms: u64,
    pub yield_duration_ms: u64,
    pub batch_count: usize,
    pub batch_size_bytes: u64,
    pub batch_duration_ms: u64,
    pub proxy_host: Option<String>,
    pub proxy_port: Option<u16>,
    pub transmitting: bool,
    pub input_ports: Vec<RemotePortInfo>,
    pub output_ports: Vec<RemotePortInfo>,
    pub parent_group_id: Option<String>,
    pub position: Option<(f64, f64)>,
    pub comments: String,
    pub metrics: RemoteProcessGroupMetrics,
}

/// Cloneable snapshot of a remote port status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemotePortInfo {
    pub id: String,
    pub name: String,
    pub target_id: Option<String>,
    pub connected: bool,
    pub transmitting: bool,
    pub exists_on_remote: bool,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub flow_files_sent: u64,
    pub flow_files_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_rpg() {
        let rpg = RemoteProcessGroup::new(
            "remote-cluster".to_string(),
            vec!["https://remote:8443".to_string()],
        );
        assert!(rpg.id.starts_with("rpg-"));
        assert_eq!(rpg.name, "remote-cluster");
        assert_eq!(rpg.target_uris.len(), 1);
        assert_eq!(rpg.transport_protocol, "QUIC");
        assert!(!rpg.transmitting);
        assert!(rpg.input_ports.is_empty());
        assert!(rpg.output_ports.is_empty());
    }

    #[test]
    fn test_rpg_metrics_empty() {
        let rpg = RemoteProcessGroup::new("test".to_string(), vec![]);
        let m = rpg.metrics();
        assert_eq!(m.bytes_sent, 0);
        assert_eq!(m.bytes_received, 0);
        assert_eq!(m.flow_files_sent, 0);
        assert_eq!(m.flow_files_received, 0);
        assert_eq!(m.active_thread_count, 0);
        assert_eq!(m.transmission_status, TransmissionStatus::Stopped);
    }

    #[test]
    fn test_rpg_metrics_aggregation() {
        let mut rpg = RemoteProcessGroup::new("test".to_string(), vec![]);
        rpg.transmitting = true;

        let port1 = RemotePortStatus::new("p1".to_string(), "port-1".to_string());
        port1.bytes_sent.store(100, Ordering::Relaxed);
        port1.flow_files_sent.store(5, Ordering::Relaxed);

        let port2 = RemotePortStatus::new("p2".to_string(), "port-2".to_string());
        port2.bytes_received.store(200, Ordering::Relaxed);
        port2.flow_files_received.store(10, Ordering::Relaxed);
        port2.active_threads.store(2, Ordering::Relaxed);

        rpg.input_ports.push(port1);
        rpg.output_ports.push(port2);

        let m = rpg.metrics();
        assert_eq!(m.bytes_sent, 100);
        assert_eq!(m.bytes_received, 200);
        assert_eq!(m.flow_files_sent, 5);
        assert_eq!(m.flow_files_received, 10);
        assert_eq!(m.active_thread_count, 2);
        assert_eq!(m.transmission_status, TransmissionStatus::Transmitting);
    }

    #[test]
    fn test_port_status_new() {
        let port = RemotePortStatus::new("port-1".to_string(), "test-port".to_string());
        assert_eq!(port.id, "port-1");
        assert_eq!(port.name, "test-port");
        assert!(!port.connected);
        assert!(!port.transmitting);
        assert!(!port.exists_on_remote);
        assert_eq!(port.bytes_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_transmission_status_display() {
        assert_eq!(TransmissionStatus::Transmitting.to_string(), "TRANSMITTING");
        assert_eq!(TransmissionStatus::Stopped.to_string(), "STOPPED");
        assert_eq!(
            TransmissionStatus::Error("timeout".to_string()).to_string(),
            "ERROR: timeout"
        );
    }

    #[test]
    fn test_rpg_id_generation() {
        let id1 = next_rpg_id();
        let id2 = next_rpg_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("rpg-"));
        assert!(id2.starts_with("rpg-"));
    }
}
