use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::bulletin::BulletinBoard;
use super::metrics::ProcessorMetrics;
use super::mutation::{MutationCommand, MutationError};
use super::persistence::FlowPersistence;
use super::processor_node::{
    SharedInputConnections, SharedInputNotifiers, SharedOutputConnections,
};
use crate::audit::{AuditAction, AuditEvent, AuditLogger, AuditTarget};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::query::ConnectionQuery;
use crate::registry::service_registry::{ServiceError, ServiceInfo, SharedServiceRegistry};
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
    /// Category tags for UI grouping (from plugin descriptor).
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum PluginKind {
    Processor,
    Source,
    Sink,
    Service,
}

/// Canvas position for a processor node (UI metadata only, not core engine data).
#[derive(Debug, Clone, Copy)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

/// A canvas label — text annotation with no data flow impact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelInfo {
    pub id: String,
    pub text: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    /// CSS background colour (hex string, e.g. "#3b82f6").
    #[serde(default)]
    pub background_color: String,
    /// Font size in pixels.
    #[serde(default = "default_font_size")]
    pub font_size: f64,
}

fn default_font_size() -> f64 {
    14.0
}

/// Partial update for a canvas label. Only non-None fields are applied.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LabelUpdate {
    pub text: Option<String>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub width: Option<f64>,
    pub height: Option<f64>,
    pub background_color: Option<String>,
    pub font_size: Option<f64>,
}

/// Auto-incrementing label ID generator.
static LABEL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique label ID.
pub fn next_label_id() -> String {
    let id = LABEL_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("label-{}", id)
}

/// Reset the label ID counter to a specific value (used when restoring persisted state).
pub fn reset_label_id_counter(next_id: u64) {
    LABEL_ID_COUNTER.store(next_id, Ordering::Relaxed);
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
    /// Structured audit logger for compliance events.
    pub audit_logger: Arc<dyn AuditLogger>,
    /// Controller service registry.
    pub service_registry: SharedServiceRegistry,
    /// Canvas labels — text annotations, no data flow impact.
    pub labels: Arc<RwLock<Vec<LabelInfo>>>,
    /// Sender half of the engine mutation command channel.
    pub(crate) mutation_tx: mpsc::Sender<MutationCommand>,
    /// Flow persistence layer (debounced background writer).
    pub(crate) persistence: Option<FlowPersistence>,
}

impl EngineHandle {
    /// Notify the persistence layer that the flow state has changed.
    /// Called internally after mutations and externally for initial persist.
    pub fn notify_persist(&self) {
        if let Some(ref p) = self.persistence {
            p.notify_changed();
        }
    }

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
                self.audit_logger.log(&AuditEvent::success(
                    AuditAction::ProcessorStopped,
                    AuditTarget::processor(name),
                ));
                return true;
            }
        }
        false
    }

    /// Start a processor by name (set enabled=true).
    ///
    /// Validates that all required properties (that lack defaults) are present
    /// before enabling the processor. Returns an error if validation fails.
    pub fn start_processor(&self, name: &str) -> Result<(), ConfigUpdateError> {
        for info in self.processors.read().iter() {
            if info.name == name {
                let props = info.properties.read();

                // Validate required properties before starting.
                for desc in &info.property_descriptors {
                    if desc.required
                        && desc.default_value.is_none()
                        && !props.contains_key(&desc.name)
                    {
                        return Err(ConfigUpdateError::ValidationError(format!(
                            "Cannot start processor '{}': required property '{}' is missing",
                            name, desc.name
                        )));
                    }
                }
                drop(props);

                info.metrics
                    .enabled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                self.audit_logger.log(&AuditEvent::success(
                    AuditAction::ProcessorStarted,
                    AuditTarget::processor(name),
                ));
                return Ok(());
            }
        }
        Err(ConfigUpdateError::NotFound(format!(
            "Processor not found: {}",
            name
        )))
    }

    /// Pause a processor by name (set paused=true).
    /// Returns `true` if the processor was found.
    pub fn pause_processor(&self, name: &str) -> bool {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics
                    .paused
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                self.audit_logger.log(&AuditEvent::success(
                    AuditAction::ProcessorPaused,
                    AuditTarget::processor(name),
                ));
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
                self.audit_logger.log(&AuditEvent::success(
                    AuditAction::ProcessorResumed,
                    AuditTarget::processor(name),
                ));
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
        drop(props);
        drop(processors);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ProcessorConfigured,
            AuditTarget::processor(name),
        ));
        self.notify_persist();
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
        let result = reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        if result.is_ok() {
            self.notify_persist();
        }
        result
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
        let result = reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        if result.is_ok() {
            self.notify_persist();
        }
        result
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
        let result = reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        if result.is_ok() {
            self.notify_persist();
        }
        result
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
        let result = reply_rx
            .await
            .map_err(|_| MutationError::EngineNotRunning)?;
        if result.is_ok() {
            self.notify_persist();
        }
        result
    }

    // ── Position metadata ────────────────────────────────────────────────────

    /// Store the canvas position for a processor (UI metadata).
    pub fn set_position(&self, name: &str, x: f64, y: f64) {
        self.positions.insert(name.to_string(), Position { x, y });
        self.notify_persist();
    }

    /// Restore a canvas position without triggering persistence.
    ///
    /// Used during startup to load persisted positions back into memory
    /// without causing an unnecessary disk write of the state that was
    /// just loaded.
    pub fn restore_position(&self, name: &str, x: f64, y: f64) {
        self.positions.insert(name.to_string(), Position { x, y });
    }

    /// Read the canvas position for a processor.
    pub fn get_position(&self, name: &str) -> Option<Position> {
        self.positions.get(name).map(|p| *p)
    }

    // ── Controller service management ─────────────────────────────────────

    /// Add a new controller service instance.
    pub fn add_service(
        &self,
        name: String,
        type_name: String,
        service: Box<dyn runifi_plugin_api::ControllerService>,
    ) -> Result<(), ServiceError> {
        let mut reg = self.service_registry.write();
        reg.add_service(name.clone(), type_name, service)?;
        drop(reg);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ServiceCreated,
            AuditTarget::service(&name),
        ));
        self.notify_persist();
        Ok(())
    }

    /// Configure a controller service with properties.
    pub fn configure_service(
        &self,
        name: &str,
        properties: HashMap<String, String>,
    ) -> Result<(), ServiceError> {
        let mut reg = self.service_registry.write();
        reg.configure_service(name, properties)?;
        drop(reg);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ServiceConfigured,
            AuditTarget::service(name),
        ));
        self.notify_persist();
        Ok(())
    }

    /// Enable a controller service.
    pub fn enable_service(&self, name: &str) -> Result<(), ServiceError> {
        let mut reg = self.service_registry.write();
        reg.enable_service(name)?;
        drop(reg);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ServiceEnabled,
            AuditTarget::service(name),
        ));
        Ok(())
    }

    /// Disable a controller service.
    pub fn disable_service(&self, name: &str) -> Result<(), ServiceError> {
        let mut reg = self.service_registry.write();
        reg.disable_service(name)?;
        drop(reg);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ServiceDisabled,
            AuditTarget::service(name),
        ));
        Ok(())
    }

    /// Remove a controller service.
    pub fn remove_service(&self, name: &str) -> Result<(), ServiceError> {
        let mut reg = self.service_registry.write();
        reg.remove_service(name)?;
        drop(reg);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ServiceRemoved,
            AuditTarget::service(name),
        ));
        self.notify_persist();
        Ok(())
    }

    /// List all controller services.
    pub fn list_services(&self) -> Vec<ServiceInfo> {
        self.service_registry.read().list_services()
    }

    /// Get info about a specific controller service.
    pub fn get_service_info(&self, name: &str) -> Option<ServiceInfo> {
        self.service_registry.read().get_service(name)
    }

    // ── Label management ────────────────────────────────────────────────

    /// Add a canvas label. Returns an error if a label with the same ID exists.
    pub fn add_label(&self, label: LabelInfo) -> Result<(), String> {
        let mut labels = self.labels.write();
        if labels.iter().any(|l| l.id == label.id) {
            return Err(format!("Label already exists: {}", label.id));
        }
        labels.push(label);
        drop(labels);
        self.notify_persist();
        Ok(())
    }

    /// Update a canvas label. Applies all non-None fields. Returns false if not found.
    pub fn update_label(&self, id: &str, update: LabelUpdate) -> bool {
        let mut labels = self.labels.write();
        if let Some(label) = labels.iter_mut().find(|l| l.id == id) {
            if let Some(text) = update.text {
                label.text = text;
            }
            if let Some(x) = update.x {
                label.x = x;
            }
            if let Some(y) = update.y {
                label.y = y;
            }
            if let Some(w) = update.width {
                label.width = w;
            }
            if let Some(h) = update.height {
                label.height = h;
            }
            if let Some(color) = update.background_color {
                label.background_color = color;
            }
            if let Some(size) = update.font_size {
                label.font_size = size;
            }
            drop(labels);
            self.notify_persist();
            true
        } else {
            false
        }
    }

    /// Remove a canvas label by ID. Returns false if not found.
    pub fn remove_label(&self, id: &str) -> bool {
        let mut labels = self.labels.write();
        let len_before = labels.len();
        labels.retain(|l| l.id != id);
        let removed = labels.len() < len_before;
        drop(labels);
        if removed {
            self.notify_persist();
        }
        removed
    }

    /// Get a label by ID.
    pub fn get_label(&self, id: &str) -> Option<LabelInfo> {
        self.labels.read().iter().find(|l| l.id == id).cloned()
    }

    /// List all labels.
    pub fn list_labels(&self) -> Vec<LabelInfo> {
        self.labels.read().clone()
    }
}
