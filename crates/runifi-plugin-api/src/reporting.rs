use crate::property::{PropertyDescriptor, PropertyValue};
use crate::result::ProcessResult;
use crate::state::{StateManager, StatefulSpec};
use crate::validation::ValidationResult;

/// Read-only snapshot of a processor's status for reporting.
#[derive(Debug, Clone)]
pub struct ProcessorStatus {
    pub name: String,
    pub type_name: String,
    pub state: String,
    pub total_invocations: u64,
    pub total_failures: u64,
    pub flowfiles_in: u64,
    pub flowfiles_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub active: bool,
}

/// Read-only snapshot of a connection's status for reporting.
#[derive(Debug, Clone)]
pub struct ConnectionStatus {
    pub id: String,
    pub source_name: String,
    pub destination_name: String,
    pub queued_count: usize,
    pub queued_bytes: u64,
    pub back_pressured: bool,
}

/// Read-only snapshot of a bulletin for reporting.
#[derive(Debug, Clone)]
pub struct BulletinSnapshot {
    pub id: u64,
    pub timestamp_ms: u64,
    pub severity: String,
    pub source_name: String,
    pub message: String,
}

/// Provides read-only engine status to a reporting task during execution.
///
/// Reporting tasks cannot modify the flow. They observe processor metrics,
/// connection queue sizes, and bulletins for external reporting.
pub trait ReportingContext: Send + Sync {
    /// Get a resolved property value by name.
    fn get_property(&self, name: &str) -> PropertyValue;

    /// Return the names of all configured properties.
    fn property_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// The configured instance name of this reporting task.
    fn name(&self) -> &str;

    /// The unique instance ID of this reporting task.
    fn id(&self) -> &str;

    /// Access the task's state manager for persistent state storage.
    fn state_manager(&self) -> Option<&dyn StateManager> {
        None
    }

    /// Get a snapshot of all processor statuses.
    fn processor_statuses(&self) -> Vec<ProcessorStatus>;

    /// Get a snapshot of all connection statuses.
    fn connection_statuses(&self) -> Vec<ConnectionStatus>;

    /// Get recent bulletins.
    fn bulletins(&self) -> Vec<BulletinSnapshot>;
}

/// A background task that observes engine status and reports it externally.
///
/// Lifecycle: `on_scheduled()` -> repeated `on_trigger()` -> `on_stopped()`
///
/// Reporting tasks are synchronous — the engine wraps them in
/// `spawn_blocking` + `catch_unwind` for fault isolation.
pub trait ReportingTask: Send + Sync + 'static {
    /// Called once when the task is scheduled to run.
    fn on_scheduled(&mut self, _context: &dyn ReportingContext) -> ProcessResult {
        Ok(())
    }

    /// Called each time the task is triggered (on schedule).
    fn on_trigger(&mut self, context: &dyn ReportingContext) -> ProcessResult;

    /// Called once when the task is stopped.
    fn on_stopped(&mut self, _context: &dyn ReportingContext) {
        // Default: no-op
    }

    /// The properties this task accepts.
    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        Vec::new()
    }

    /// Validate the task's configuration.
    fn validate(&self, _context: &dyn ReportingContext) -> Vec<ValidationResult> {
        Vec::new()
    }

    /// Declare that this task is stateful.
    fn stateful(&self) -> Option<StatefulSpec> {
        None
    }
}

/// Describes a reporting task type for plugin registration.
pub struct ReportingTaskDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn ReportingTask>,
    pub tags: &'static [&'static str],
}

inventory::collect!(ReportingTaskDescriptor);
