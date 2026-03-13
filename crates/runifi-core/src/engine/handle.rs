use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};

use super::bulletin::BulletinBoard;
use super::metrics::ProcessorMetrics;
use super::mutation::{MutationCommand, MutationError};
use super::processor_node::{SharedInputConnections, SharedInputNotifiers, SharedOutputConnections};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::query::ConnectionQuery;
use crate::repository::content_repo::ContentRepository;

/// Error type for processor configuration updates.
#[derive(Debug)]
pub enum ConfigUpdateError {
    /// Processor not found.
    NotFound(String),
    /// Processor is not in a stopped state (409 Conflict).
    StateConflict(String),
    /// Validation failure: missing required property or invalid allowed value (400 Bad Request).
    ValidationError(String),
}

impl fmt::Display for ConfigUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigUpdateError::NotFound(msg) => write!(f, "{}", msg),
            ConfigUpdateError::StateConflict(msg) => write!(f, "{}", msg),
            ConfigUpdateError::ValidationError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ConfigUpdateError {}

/// Static metadata about a processor property, suitable for API responses.
#[derive(Debug, Clone)]
pub struct PropertyDescriptorInfo {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
    pub allowed_values: Option<Vec<String>>,
}

/// Static metadata about a processor relationship, suitable for API responses.
#[derive(Debug, Clone)]
pub struct RelationshipInfo {
    pub name: String,
    pub description: String,
    pub auto_terminated: bool,
}

/// Information about a processor instance, visible to the API.
#[derive(Clone)]
pub struct ProcessorInfo {
    pub name: String,
    pub type_name: String,
    /// Human-readable scheduling description, e.g. "timer-driven (1000ms)" or "event-driven".
    /// The concrete `SchedulingStrategy` enum is kept internal to the engine.
    pub scheduling_display: String,
    pub metrics: Arc<ProcessorMetrics>,
    /// Property descriptors (static metadata from the processor type).
    pub property_descriptors: Vec<PropertyDescriptorInfo>,
    /// Relationships (static metadata from the processor type).
    pub relationships: Vec<RelationshipInfo>,
    /// Current property values, shared with the processor node for runtime updates.
    pub properties: Arc<RwLock<HashMap<String, String>>>,
    /// Shared input connection list — held so the mutation handler can wire
    /// hot-added connections into the running processor task.
    pub input_connections: SharedInputConnections,
    /// Shared output connection list — same purpose as `input_connections`.
    pub output_connections: SharedOutputConnections,
    /// Shared input notifier list — held so the mutation handler can register
    /// event-driven wakeup notifiers for hot-added input connections.
    pub input_notifiers: SharedInputNotifiers,
}

/// Information about a connection, visible to the API.
#[derive(Clone)]
pub struct ConnectionInfo {
    pub id: String,
    pub source_name: String,
    pub relationship: String,
    pub dest_name: String,
    pub connection: Arc<dyn ConnectionQuery>,
}

/// Information about a registered plugin type.
#[derive(Clone)]
pub struct PluginTypeInfo {
    pub type_name: String,
    pub kind: PluginKind,
}

#[derive(Debug, Clone, Copy)]
pub enum PluginKind {
    Processor,
    Source,
    Sink,
}

/// Canvas position for a processor node (UI metadata only, not core engine data).
#[derive(Debug, Clone, Copy)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

/// A Clone-able, Send+Sync handle for API queries and mutations against a running engine.
///
/// Created by `FlowEngine::start()`. The processor and connection lists use
/// interior mutability (`RwLock`) so that runtime topology changes are visible
/// to all handle clones without reinitialising the handle.
///
/// Topology mutations (add/remove processor/connection) are serialised through
/// a `tokio::mpsc` command channel processed by the engine mutation task.
#[derive(Clone)]
pub struct EngineHandle {
    pub flow_name: String,
    pub started_at: Instant,
    /// Live processor list — read via `processors.read()`.
    pub processors: Arc<RwLock<Vec<ProcessorInfo>>>,
    /// Live connection list — read via `connections.read()`.
    pub connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    pub plugin_types: Arc<Vec<PluginTypeInfo>>,
    pub bulletin_board: Arc<BulletinBoard>,
    pub content_repo: Arc<dyn ContentRepository>,
    /// Canvas position store (processor name -> position). UI metadata only.
    pub positions: Arc<DashMap<String, Position>>,
    /// Sender half of the engine mutation command channel.
    pub(crate) mutation_tx: mpsc::Sender<MutationCommand>,
}

impl EngineHandle {
    // ── Lifecycle controls ───────────────────────────────────────────────────

    /// Request a circuit breaker reset for a processor by name.
    /// Returns `true` if the processor was found and the flag was set.
    pub fn request_circuit_reset(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics
                    .reset_requested
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Stop a processor by name (set enabled=false).
    /// Returns `true` if the processor was found.
    pub fn stop_processor(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics
                    .enabled
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Start a processor by name (set enabled=true).
    /// Returns `true` if the processor was found.
    pub fn start_processor(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                let _props = info.properties.read();
                info.metrics
                    .enabled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Pause a processor by name (set paused=true).
    /// Returns `true` if the processor was found.
    pub fn pause_processor(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics
                    .paused
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Resume a processor by name (set paused=false).
    /// Returns `true` if the processor was found.
    pub fn resume_processor(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    // ── Reads ────────────────────────────────────────────────────────────────

    /// Get a cloned `ProcessorInfo` for a processor by name.
    ///
    /// Returns a clone so callers never hold the `RwLock` across an await point.
    pub fn get_processor_info(&self, name: &str) -> Option<ProcessorInfo> {
        self.processors
            .read()
            .iter()
            .find(|p| p.name == name)
            .cloned()
    }

    // ── Config updates ───────────────────────────────────────────────────────

    /// Update properties for a processor by name.
    /// Returns `Ok(())` if successful, or an error describing the failure.
    pub fn update_processor_properties(
        &self,
        name: &str,
        new_properties: HashMap<String, String>,
    ) -> Result<(), ConfigUpdateError> {
        let processors = self.processors.read();
        let info = processors
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| ConfigUpdateError::NotFound(format!("Processor not found: {}", name)))?;

        // Acquire the write lock BEFORE checking state to prevent TOCTOU race
        // with concurrent start requests.
        let mut props = info.properties.write();

        let enabled = info
            .metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed);
        let active = info
            .metrics
            .active
            .load(std::sync::atomic::Ordering::Relaxed);

        if enabled || active {
            return Err(ConfigUpdateError::StateConflict(
                "Processor must be stopped before updating configuration".to_string(),
            ));
        }

        for desc in &info.property_descriptors {
            if desc.required {
                let has_value = new_properties.contains_key(&desc.name);
                let has_default = desc.default_value.is_some();
                if !has_value && !has_default {
                    return Err(ConfigUpdateError::ValidationError(format!(
                        "Required property '{}' is missing",
                        desc.name
                    )));
                }
            }
        }

        for (key, value) in &new_properties {
            if let Some(desc) = info.property_descriptors.iter().find(|d| d.name == *key)
                && let Some(ref allowed) = desc.allowed_values
                && !allowed.iter().any(|v| v == value)
            {
                return Err(ConfigUpdateError::ValidationError(format!(
                    "Invalid value '{}' for property '{}'. Allowed: {:?}",
                    value, key, allowed
                )));
            }
        }

        *props = new_properties;
        Ok(())
    }

    // ── Runtime topology mutations ───────────────────────────────────────────

    /// Add a new processor at runtime (hot-add).
    ///
    /// The processor starts in STOPPED state.
    pub async fn add_processor(
        &self,
        name: String,
        type_name: String,
        properties: HashMap<String, String>,
        scheduling_strategy: String,
        interval_ms: u64,
    ) -> Result<(), MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::AddProcessor {
                name,
                type_name,
                properties,
                scheduling_strategy,
                interval_ms,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?
    }

    /// Remove a processor at runtime (hot-remove).
    ///
    /// The processor must be STOPPED and have no active connections.
    pub async fn remove_processor(&self, name: String) -> Result<(), MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::RemoveProcessor {
                name,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?
    }

    /// Add a new connection at runtime (hot-add).
    ///
    /// Returns the generated connection ID on success.
    pub async fn add_connection(
        &self,
        source_name: String,
        relationship: String,
        dest_name: String,
        config: BackPressureConfig,
    ) -> Result<String, MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::AddConnection {
                source_name,
                relationship,
                dest_name,
                config,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?
    }

    /// Remove a connection at runtime (hot-remove).
    ///
    /// If `force=false` and the queue is non-empty, returns `MutationError::QueueNotEmpty`.
    /// With `force=true` the queue is drained and FlowFiles are discarded with a warning.
    pub async fn remove_connection(&self, id: String, force: bool) -> Result<(), MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::RemoveConnection {
                id,
                force,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?
    }

    // ── Position metadata ────────────────────────────────────────────────────

    /// Store the canvas position for a processor (UI metadata).
    pub fn set_position(&self, name: &str, x: f64, y: f64) {
        self.positions.insert(name.to_string(), Position { x, y });
    }

    /// Read the canvas position for a processor.
    pub fn get_position(&self, name: &str) -> Option<Position> {
        self.positions.get(name).map(|p| *p)
    }
}
