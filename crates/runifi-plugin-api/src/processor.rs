use crate::context::ProcessContext;
use crate::property::PropertyDescriptor;
use crate::relationship::Relationship;
use crate::result::ProcessResult;
use crate::session::ProcessSession;
use crate::state::StatefulSpec;
use crate::validation::ValidationResult;

/// The core processor trait. Processors are synchronous — the engine wraps
/// them in `spawn_blocking` + `catch_unwind` for fault isolation.
///
/// Lifecycle: `on_scheduled()` → repeated `on_trigger()` → `on_stopped()`
pub trait Processor: Send + Sync + 'static {
    /// Called once when the processor is scheduled to run.
    fn on_scheduled(&mut self, _context: &dyn ProcessContext) -> ProcessResult {
        Ok(())
    }

    /// Called each time the processor is triggered. This is the main processing logic.
    ///
    /// The processor should:
    /// 1. Get FlowFiles from the session
    /// 2. Process them (read/write content, modify attributes)
    /// 3. Transfer them to relationships
    /// 4. Commit or rollback the session
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult;

    /// Called once when the processor is stopped.
    fn on_stopped(&mut self, _context: &dyn ProcessContext) {
        // Default: no-op
    }

    /// The relationships this processor supports.
    fn relationships(&self) -> Vec<Relationship>;

    /// The properties this processor accepts.
    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        Vec::new()
    }

    /// Validate the processor's configuration. Returns a list of validation errors.
    ///
    /// An empty list means the processor is valid and can be started. The engine
    /// calls this automatically on configuration changes and before starting.
    /// The default implementation performs no custom validation (always valid).
    ///
    /// Built-in validation (required properties, allowed values) is handled by
    /// the engine and does not need to be reimplemented here.
    fn validate(&self, _context: &dyn ProcessContext) -> Vec<ValidationResult> {
        Vec::new()
    }

    /// Declare that this processor is stateful.
    ///
    /// Returns `Some(StatefulSpec)` if the processor stores persistent state,
    /// `None` otherwise (default). Stateful processors can access a
    /// `StateManager` via `ProcessContext::state_manager()`.
    fn stateful(&self) -> Option<StatefulSpec> {
        None
    }
}

/// Describes a processor type for plugin registration.
pub struct ProcessorDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn Processor>,
    /// Category tags for UI grouping (e.g., &["Routing", "Attribute Manipulation"]).
    pub tags: &'static [&'static str],
}

inventory::collect!(ProcessorDescriptor);
