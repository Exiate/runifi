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
use super::persistence::{
    FlowPersistence, PersistedBackPressure, PersistedConnection, PersistedFlowState,
    PersistedLabel, PersistedPort, PersistedPosition, PersistedProcessGroup, PersistedProcessor,
    PersistedService, scheduling_display_to_persisted,
};
use super::process_group::{PortInfo, PortType, ProcessGroupId, ProcessGroupInfo};
use super::processor_node::{
    SharedInputConnections, SharedInputNotifiers, SharedOutputConnections,
};
use crate::audit::{AuditAction, AuditEvent, AuditLogger, AuditTarget};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::query::ConnectionQuery;
use crate::registry::service_registry::{ServiceError, ServiceInfo, SharedServiceRegistry};
use crate::repository::content_repo::ContentRepository;
use crate::repository::provenance_repo::SharedProvenanceRepository;
use crate::repository::state_provider::SharedLocalStateProvider;

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
    pub expression_language_supported: bool,
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
    /// Penalty duration in milliseconds (default 30000).
    pub penalty_duration_ms: Arc<AtomicU64>,
    /// Yield duration in milliseconds (default 1000).
    pub yield_duration_ms: Arc<AtomicU64>,
    /// Bulletin level threshold: DEBUG, INFO, WARN, ERROR (default WARN).
    pub bulletin_level: Arc<RwLock<String>>,
    /// Number of concurrent tasks (default 1, currently enforced as max 1).
    pub concurrent_tasks: Arc<AtomicU64>,
    /// User comments (persisted with config).
    pub comments: Arc<RwLock<String>>,
    /// Auto-terminated relationship names (configurable at runtime).
    pub auto_terminated_relationships: Arc<RwLock<Vec<String>>>,
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

/// Auto-incrementing process group ID counter.
static GROUP_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Reset the process group ID counter to a specific value (used when restoring persisted state).
pub fn reset_group_id_counter(next_id: u64) {
    GROUP_ID_COUNTER.store(next_id, Ordering::Relaxed);
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
    /// Process groups for hierarchical flow organization.
    pub process_groups: Arc<RwLock<Vec<ProcessGroupInfo>>>,
    /// Sender half of the engine mutation command channel.
    pub(crate) mutation_tx: mpsc::Sender<MutationCommand>,
    /// Flow persistence layer (debounced background writer).
    pub(crate) persistence: Option<FlowPersistence>,
    /// Provenance repository for FlowFile lineage tracking.
    pub provenance_repo: SharedProvenanceRepository,
    /// Local state provider for processor state persistence.
    pub state_provider: Option<SharedLocalStateProvider>,
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
                // Reject if processor is administratively disabled.
                if info
                    .metrics
                    .disabled
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(ConfigUpdateError::StateConflict(format!(
                        "Cannot start processor '{}': processor is disabled",
                        name
                    )));
                }

                // Reject if processor has validation errors.
                let validation_errors = info.metrics.validation_errors();
                if !validation_errors.is_empty() {
                    return Err(ConfigUpdateError::ValidationError(format!(
                        "Cannot start processor '{}': validation errors: {}",
                        name,
                        validation_errors.join("; ")
                    )));
                }

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

    /// Disable a processor by name (set disabled=true, enabled=false).
    /// Returns `Ok(())` if the processor was found.
    pub fn disable_processor(&self, name: &str) -> Result<(), String> {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics.enabled.store(false, Ordering::Relaxed);
                info.metrics.disabled.store(true, Ordering::Relaxed);
                return Ok(());
            }
        }
        Err(format!("Processor '{}' not found", name))
    }

    /// Enable a processor by name (clear disabled flag).
    /// Note: this only clears the disabled flag; the processor must still be
    /// started separately.
    pub fn enable_processor(&self, name: &str) -> Result<(), String> {
        for info in self.processors.read().iter() {
            if info.name == name {
                info.metrics.disabled.store(false, Ordering::Relaxed);
                return Ok(());
            }
        }
        Err(format!("Processor '{}' not found", name))
    }

    /// Get the current validation errors for a processor by name.
    pub fn get_validation_errors(&self, name: &str) -> Result<Vec<String>, String> {
        for info in self.processors.read().iter() {
            if info.name == name {
                return Ok(info.metrics.validation_errors());
            }
        }
        Err(format!("Processor '{}' not found", name))
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

    /// Update extended processor configuration (properties + settings + scheduling + relationships + comments).
    /// Accepts partial updates — only non-None fields are applied.
    #[allow(clippy::too_many_arguments)]
    pub fn update_processor_config(
        &self,
        name: &str,
        properties: Option<HashMap<String, String>>,
        penalty_duration_ms: Option<u64>,
        yield_duration_ms: Option<u64>,
        bulletin_level: Option<String>,
        concurrent_tasks: Option<u64>,
        auto_terminated_relationships: Option<Vec<String>>,
        comments: Option<String>,
    ) -> Result<(), ConfigUpdateError> {
        let processors = self.processors.read();
        let info = processors
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| ConfigUpdateError::NotFound(format!("Processor not found: {}", name)))?;

        // Check stopped state.
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

        // Validate and apply properties if provided.
        if let Some(ref new_properties) = properties {
            let mut props = info.properties.write();
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
            for (key, value) in new_properties {
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
            *props = new_properties.clone();
        }

        // Apply penalty duration.
        if let Some(penalty) = penalty_duration_ms {
            info.penalty_duration_ms
                .store(penalty, std::sync::atomic::Ordering::Relaxed);
        }

        // Apply yield duration.
        if let Some(yield_ms) = yield_duration_ms {
            info.yield_duration_ms
                .store(yield_ms, std::sync::atomic::Ordering::Relaxed);
        }

        // Validate and apply bulletin level.
        if let Some(ref level) = bulletin_level {
            match level.as_str() {
                "DEBUG" | "INFO" | "WARN" | "ERROR" => {
                    *info.bulletin_level.write() = level.clone();
                }
                _ => {
                    return Err(ConfigUpdateError::ValidationError(format!(
                        "Invalid bulletin level '{}'. Allowed: DEBUG, INFO, WARN, ERROR",
                        level
                    )));
                }
            }
        }

        // Apply concurrent tasks (accept for forward compat, enforce max 1 for now).
        if let Some(tasks) = concurrent_tasks {
            let clamped = tasks.clamp(1, 1);
            info.concurrent_tasks
                .store(clamped, std::sync::atomic::Ordering::Relaxed);
        }

        // Apply auto-terminated relationships.
        if let Some(ref auto_term) = auto_terminated_relationships {
            *info.auto_terminated_relationships.write() = auto_term.clone();
        }

        // Apply comments.
        if let Some(ref c) = comments {
            *info.comments.write() = c.clone();
        }

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
        self.add_processor_with_cron(
            name,
            type_name,
            properties,
            scheduling_strategy,
            interval_ms,
            None,
        )
        .await
    }

    /// Add a new processor at runtime with optional CRON expression.
    pub async fn add_processor_with_cron(
        &self,
        name: String,
        type_name: String,
        properties: HashMap<String, String>,
        scheduling_strategy: String,
        interval_ms: u64,
        cron_expression: Option<String>,
    ) -> Result<(), MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::AddProcessor {
                name,
                type_name,
                properties,
                scheduling_strategy,
                interval_ms,
                cron_expression,
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
        load_balance: Option<crate::cluster::load_balance::LoadBalanceConfig>,
    ) -> Result<String, MutationError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.mutation_tx
            .send(MutationCommand::AddConnection {
                source_name,
                relationship,
                dest_name,
                config,
                load_balance,
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

    // ── Flow state snapshot ────────────────────────────────────────────

    /// Capture a point-in-time snapshot of the entire flow state.
    ///
    /// Used by the versioning system to save the current flow as a named version.
    /// Builds the snapshot directly from the handle's live data references.
    pub fn snapshot_flow_state(&self) -> PersistedFlowState {
        let processors: Vec<PersistedProcessor> = self
            .processors
            .read()
            .iter()
            .map(|p| {
                let penalty = p
                    .penalty_duration_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                let yield_ms = p
                    .yield_duration_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                let concurrent = p
                    .concurrent_tasks
                    .load(std::sync::atomic::Ordering::Relaxed);
                let bulletin = p.bulletin_level.read().clone();
                let comments = p.comments.read().clone();
                let auto_term = p.auto_terminated_relationships.read().clone();
                PersistedProcessor {
                    name: p.name.clone(),
                    type_name: p.type_name.clone(),
                    scheduling: scheduling_display_to_persisted(&p.scheduling_display),
                    properties: p.properties.read().clone(),
                    penalty_duration_ms: if penalty != 30_000 {
                        Some(penalty)
                    } else {
                        None
                    },
                    yield_duration_ms: if yield_ms != 1_000 {
                        Some(yield_ms)
                    } else {
                        None
                    },
                    bulletin_level: if bulletin != "WARN" {
                        Some(bulletin)
                    } else {
                        None
                    },
                    concurrent_tasks: if concurrent != 1 {
                        Some(concurrent)
                    } else {
                        None
                    },
                    auto_terminated_relationships: if auto_term.is_empty() {
                        None
                    } else {
                        Some(auto_term)
                    },
                    comments: if comments.is_empty() {
                        None
                    } else {
                        Some(comments)
                    },
                }
            })
            .collect();

        let connections: Vec<PersistedConnection> = self
            .connections
            .read()
            .iter()
            .map(|c| {
                let bp = c.connection.back_pressure_config();
                // Persist expiration and priority from the underlying FlowConnection.
                let (expiration, priority, priority_attribute) =
                    if let Some(fc) = c.connection.flow_connection() {
                        let exp = fc.expiration().map(|d| {
                            let secs = d.as_secs();
                            if secs >= 86400 && secs % 86400 == 0 {
                                format!("{}d", secs / 86400)
                            } else if secs >= 3600 && secs % 3600 == 0 {
                                format!("{}h", secs / 3600)
                            } else if secs >= 60 && secs % 60 == 0 {
                                format!("{}m", secs / 60)
                            } else {
                                format!("{}s", secs)
                            }
                        });
                        let (prio, prio_attr) = match fc.queue_priority() {
                            crate::connection::flow_connection::QueuePriority::Fifo => (None, None),
                            crate::connection::flow_connection::QueuePriority::NewestFirst => {
                                (Some("NewestFirst".to_string()), None)
                            }
                            crate::connection::flow_connection::QueuePriority::PriorityAttribute(
                                attr,
                            ) => (Some("PriorityAttribute".to_string()), Some(attr.clone())),
                        };
                        (exp, prio, prio_attr)
                    } else {
                        (None, None, None)
                    };
                PersistedConnection {
                    source: c.source_name.clone(),
                    relationship: c.relationship.clone(),
                    destination: c.dest_name.clone(),
                    back_pressure: Some(PersistedBackPressure {
                        max_count: Some(bp.max_count),
                        max_bytes: Some(bp.max_bytes),
                    }),
                    expiration,
                    priority,
                    priority_attribute,
                    load_balancing: c.connection.load_balance_config().cloned(),
                }
            })
            .collect();

        let mut positions = HashMap::new();
        for entry in self.positions.iter() {
            positions.insert(
                entry.key().clone(),
                PersistedPosition {
                    x: entry.value().x,
                    y: entry.value().y,
                },
            );
        }

        let services: Vec<PersistedService> = self
            .service_registry
            .read()
            .list_services()
            .into_iter()
            .map(|s| PersistedService {
                name: s.name,
                type_name: s.type_name,
                properties: s.properties,
            })
            .collect();

        let labels: Vec<PersistedLabel> = self
            .labels
            .read()
            .iter()
            .map(|l| PersistedLabel {
                id: l.id.clone(),
                text: l.text.clone(),
                x: l.x,
                y: l.y,
                width: l.width,
                height: l.height,
                background_color: l.background_color.clone(),
                font_size: l.font_size,
            })
            .collect();

        let process_groups: Vec<PersistedProcessGroup> = self
            .process_groups
            .read()
            .iter()
            .map(|g| {
                let input_ports: Vec<PersistedPort> = g
                    .input_ports
                    .iter()
                    .map(|p| PersistedPort {
                        id: p.id.clone(),
                        name: p.name.clone(),
                        port_type: "input".to_string(),
                    })
                    .collect();
                let output_ports: Vec<PersistedPort> = g
                    .output_ports
                    .iter()
                    .map(|p| PersistedPort {
                        id: p.id.clone(),
                        name: p.name.clone(),
                        port_type: "output".to_string(),
                    })
                    .collect();
                PersistedProcessGroup {
                    id: g.id.clone(),
                    name: g.name.clone(),
                    input_ports,
                    output_ports,
                    processor_names: g.processor_names.clone(),
                    connection_ids: g.connection_ids.clone(),
                    child_group_ids: g.child_group_ids.clone(),
                    parent_group_id: g.parent_group_id.clone(),
                    variables: g.variables.clone(),
                }
            })
            .collect();

        PersistedFlowState {
            version: 1,
            flow_name: self.flow_name.clone(),
            processors,
            connections,
            positions,
            services,
            labels,
            process_groups,
        }
    }

    // ── Process group management ──────────────────────────────────────────

    /// Auto-incrementing process group ID counter.
    fn next_group_id() -> ProcessGroupId {
        let id = GROUP_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("pg-{}", id)
    }

    /// Create a new process group. Returns the generated group ID.
    ///
    /// If `parent_group_id` is `Some`, the group is nested inside the parent.
    /// Input/output port names are converted to `PortInfo` entries.
    pub fn create_process_group(
        &self,
        name: String,
        parent_group_id: Option<ProcessGroupId>,
        input_port_names: Vec<String>,
        output_port_names: Vec<String>,
        variables: HashMap<String, String>,
    ) -> Result<ProcessGroupId, String> {
        let mut groups = self.process_groups.write();

        // Validate parent exists if specified.
        if let Some(ref parent_id) = parent_group_id
            && !groups.iter().any(|g| g.id == *parent_id)
        {
            return Err(format!("Parent process group not found: {}", parent_id));
        }

        // Check for duplicate name within the same parent scope.
        let duplicate = groups
            .iter()
            .any(|g| g.name == name && g.parent_group_id == parent_group_id);
        if duplicate {
            return Err(format!(
                "A process group named '{}' already exists in this scope",
                name
            ));
        }

        let group_id = Self::next_group_id();

        let input_ports: Vec<PortInfo> = input_port_names
            .into_iter()
            .enumerate()
            .map(|(i, port_name)| PortInfo {
                id: format!("{}-in-{}", group_id, i),
                name: port_name,
                port_type: PortType::Input,
                group_id: group_id.clone(),
            })
            .collect();

        let output_ports: Vec<PortInfo> = output_port_names
            .into_iter()
            .enumerate()
            .map(|(i, port_name)| PortInfo {
                id: format!("{}-out-{}", group_id, i),
                name: port_name,
                port_type: PortType::Output,
                group_id: group_id.clone(),
            })
            .collect();

        let group_info = ProcessGroupInfo {
            id: group_id.clone(),
            name,
            input_ports,
            output_ports,
            processor_names: Vec::new(),
            connection_ids: Vec::new(),
            child_group_ids: Vec::new(),
            parent_group_id: parent_group_id.clone(),
            variables,
        };

        groups.push(group_info);

        // Register this group as a child of its parent.
        if let Some(ref parent_id) = parent_group_id
            && let Some(parent) = groups.iter_mut().find(|g| g.id == *parent_id)
        {
            parent.child_group_ids.push(group_id.clone());
        }

        drop(groups);

        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ProcessGroupCreated,
            AuditTarget::process_group(&group_id),
        ));
        self.notify_persist();

        Ok(group_id)
    }

    /// Get a process group by ID.
    pub fn get_process_group(&self, id: &str) -> Option<ProcessGroupInfo> {
        self.process_groups
            .read()
            .iter()
            .find(|g| g.id == id)
            .cloned()
    }

    /// List all process groups.
    pub fn list_process_groups(&self) -> Vec<ProcessGroupInfo> {
        self.process_groups.read().clone()
    }

    /// List top-level process groups (those with no parent).
    pub fn list_root_process_groups(&self) -> Vec<ProcessGroupInfo> {
        self.process_groups
            .read()
            .iter()
            .filter(|g| g.parent_group_id.is_none())
            .cloned()
            .collect()
    }

    /// Update a process group's name and/or variables.
    pub fn update_process_group(
        &self,
        id: &str,
        name: Option<String>,
        variables: Option<HashMap<String, String>>,
    ) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == id)
            .ok_or_else(|| format!("Process group not found: {}", id))?;

        if let Some(new_name) = name {
            group.name = new_name;
        }
        if let Some(new_vars) = variables {
            group.variables = new_vars;
        }

        drop(groups);
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ProcessGroupUpdated,
            AuditTarget::process_group(id),
        ));
        self.notify_persist();
        Ok(())
    }

    /// Remove a process group by ID.
    ///
    /// The group must be empty (no processors, connections, or child groups).
    pub fn remove_process_group(&self, id: &str) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter()
            .find(|g| g.id == id)
            .ok_or_else(|| format!("Process group not found: {}", id))?;

        if !group.processor_names.is_empty() {
            return Err(format!(
                "Process group '{}' still contains {} processor(s). Remove them first.",
                id,
                group.processor_names.len()
            ));
        }
        if !group.connection_ids.is_empty() {
            return Err(format!(
                "Process group '{}' still contains {} connection(s). Remove them first.",
                id,
                group.connection_ids.len()
            ));
        }
        if !group.child_group_ids.is_empty() {
            return Err(format!(
                "Process group '{}' still contains {} child group(s). Remove them first.",
                id,
                group.child_group_ids.len()
            ));
        }

        let parent_id = group.parent_group_id.clone();
        let group_id = group.id.clone();

        // Remove from parent's child list.
        if let Some(ref parent_id) = parent_id
            && let Some(parent) = groups.iter_mut().find(|g| g.id == *parent_id)
        {
            parent.child_group_ids.retain(|cid| cid != &group_id);
        }

        groups.retain(|g| g.id != id);
        drop(groups);

        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ProcessGroupRemoved,
            AuditTarget::process_group(id),
        ));
        self.notify_persist();
        Ok(())
    }

    /// Add a processor to a process group by name.
    pub fn add_processor_to_group(
        &self,
        group_id: &str,
        processor_name: &str,
    ) -> Result<(), String> {
        // Verify the processor exists.
        if self.get_processor_info(processor_name).is_none() {
            return Err(format!("Processor not found: {}", processor_name));
        }

        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        if group.processor_names.contains(&processor_name.to_string()) {
            return Err(format!(
                "Processor '{}' is already in group '{}'",
                processor_name, group_id
            ));
        }

        group.processor_names.push(processor_name.to_string());
        drop(groups);
        self.notify_persist();
        Ok(())
    }

    /// Remove a processor from a process group.
    pub fn remove_processor_from_group(
        &self,
        group_id: &str,
        processor_name: &str,
    ) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        let len_before = group.processor_names.len();
        group.processor_names.retain(|n| n != processor_name);
        if group.processor_names.len() == len_before {
            return Err(format!(
                "Processor '{}' is not in group '{}'",
                processor_name, group_id
            ));
        }

        drop(groups);
        self.notify_persist();
        Ok(())
    }

    /// Add a connection to a process group by connection ID.
    pub fn add_connection_to_group(
        &self,
        group_id: &str,
        connection_id: &str,
    ) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        if group.connection_ids.contains(&connection_id.to_string()) {
            return Err(format!(
                "Connection '{}' is already in group '{}'",
                connection_id, group_id
            ));
        }

        group.connection_ids.push(connection_id.to_string());
        drop(groups);
        self.notify_persist();
        Ok(())
    }

    /// Remove a connection from a process group.
    pub fn remove_connection_from_group(
        &self,
        group_id: &str,
        connection_id: &str,
    ) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        let len_before = group.connection_ids.len();
        group.connection_ids.retain(|c| c != connection_id);
        if group.connection_ids.len() == len_before {
            return Err(format!(
                "Connection '{}' is not in group '{}'",
                connection_id, group_id
            ));
        }

        drop(groups);
        self.notify_persist();
        Ok(())
    }

    /// Add an input port to a process group.
    pub fn add_input_port(&self, group_id: &str, port_name: String) -> Result<String, String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        if group.input_ports.iter().any(|p| p.name == port_name) {
            return Err(format!(
                "Input port '{}' already exists in group '{}'",
                port_name, group_id
            ));
        }

        let port_id = format!("{}-in-{}", group_id, group.input_ports.len());
        group.input_ports.push(PortInfo {
            id: port_id.clone(),
            name: port_name,
            port_type: PortType::Input,
            group_id: group_id.to_string(),
        });

        drop(groups);
        self.notify_persist();
        Ok(port_id)
    }

    /// Add an output port to a process group.
    pub fn add_output_port(&self, group_id: &str, port_name: String) -> Result<String, String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        if group.output_ports.iter().any(|p| p.name == port_name) {
            return Err(format!(
                "Output port '{}' already exists in group '{}'",
                port_name, group_id
            ));
        }

        let port_id = format!("{}-out-{}", group_id, group.output_ports.len());
        group.output_ports.push(PortInfo {
            id: port_id.clone(),
            name: port_name,
            port_type: PortType::Output,
            group_id: group_id.to_string(),
        });

        drop(groups);
        self.notify_persist();
        Ok(port_id)
    }

    /// Remove a port from a process group by port ID.
    pub fn remove_port(&self, group_id: &str, port_id: &str) -> Result<(), String> {
        let mut groups = self.process_groups.write();
        let group = groups
            .iter_mut()
            .find(|g| g.id == group_id)
            .ok_or_else(|| format!("Process group not found: {}", group_id))?;

        let in_len = group.input_ports.len();
        group.input_ports.retain(|p| p.id != port_id);
        if group.input_ports.len() < in_len {
            drop(groups);
            self.notify_persist();
            return Ok(());
        }

        let out_len = group.output_ports.len();
        group.output_ports.retain(|p| p.id != port_id);
        if group.output_ports.len() < out_len {
            drop(groups);
            self.notify_persist();
            return Ok(());
        }

        Err(format!(
            "Port '{}' not found in group '{}'",
            port_id, group_id
        ))
    }
}
