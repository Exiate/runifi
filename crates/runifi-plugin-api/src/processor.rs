use crate::context::ProcessContext;
use crate::property::PropertyDescriptor;
use crate::relationship::Relationship;
use crate::result::ProcessResult;
use crate::session::ProcessSession;

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
