use crate::ExecutionNode;
use crate::context::ProcessContext;
use crate::property::PropertyDescriptor;
use crate::relationship::Relationship;
use crate::result::ProcessResult;
use crate::session::ProcessSession;
use crate::state::StatefulSpec;
use crate::validation::ValidationResult;

/// Declares whether a processor requires, allows, or forbids incoming connections.
///
/// The engine uses this annotation to validate flow topology at design time,
/// preventing invalid wiring (e.g., connecting to a source-only processor).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InputRequirement {
    /// Processor requires at least one incoming connection (e.g., LogAttribute, PutFile).
    Required,
    /// Processor can optionally have incoming connections (default).
    #[default]
    Allowed,
    /// Processor must not have incoming connections — it is a pure source
    /// (e.g., GenerateFlowFile, GetFile).
    Forbidden,
}

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

    /// Declare the execution node requirement for this processor.
    ///
    /// Returns `ExecutionNode::All` by default. Override to return
    /// `ExecutionNode::Primary` for processors that should only run
    /// on the primary node (e.g., ListFile, ListS3).
    fn execution_node(&self) -> ExecutionNode {
        ExecutionNode::All
    }

    /// Declare the input requirement for this processor.
    ///
    /// Returns `InputRequirement::Allowed` by default. Override to return
    /// `InputRequirement::Required` for processors that need incoming data,
    /// or `InputRequirement::Forbidden` for source processors that generate
    /// their own data.
    fn input_requirement(&self) -> InputRequirement {
        InputRequirement::Allowed
    }

    /// Whether this processor should be triggered even when no FlowFiles
    /// are queued on its incoming connections.
    ///
    /// Defaults to `false`. Source processors (e.g., GenerateFlowFile, GetFile)
    /// should return `true`.
    fn trigger_when_empty(&self) -> bool {
        false
    }

    /// Whether this processor is side-effect-free (idempotent, safe to retry).
    ///
    /// Defaults to `false`. Processors that only read/route FlowFiles without
    /// modifying external state (e.g., LogAttribute, RouteOnAttribute) should
    /// return `true`.
    fn side_effect_free(&self) -> bool {
        false
    }

    /// Whether this processor is compatible with run-duration batching.
    ///
    /// Defaults to `false`. Processors that can efficiently process multiple
    /// FlowFiles in a single trigger invocation should return `true`.
    fn supports_batching(&self) -> bool {
        false
    }

    /// Whether this processor supports user-defined dynamic properties.
    ///
    /// When true, the engine allows arbitrary properties beyond the declared
    /// `property_descriptors()`. When false (default), unknown properties
    /// are rejected during validation.
    fn supports_dynamic_properties(&self) -> bool {
        false
    }

    /// Whether dynamic properties on this processor can contain sensitive values.
    ///
    /// When true, dynamic property values are masked in API responses.
    /// Only meaningful when `supports_dynamic_properties()` is also true.
    fn supports_sensitive_dynamic_properties(&self) -> bool {
        false
    }

    /// Whether each dynamic property name creates a corresponding relationship.
    ///
    /// When true, the engine automatically registers a relationship for every
    /// dynamic property whose name does not match a declared property descriptor.
    /// Used by processors like RouteOnAttribute where dynamic properties define
    /// routing rules and the property name becomes the relationship name.
    fn dynamic_property_creates_relationship(&self) -> bool {
        false
    }
}

/// Describes a processor type for plugin registration.
pub struct ProcessorDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn Processor>,
    /// Category tags for UI grouping (e.g., &["Routing", "Attribute Manipulation"]).
    pub tags: &'static [&'static str],
    /// Input requirement annotation for this processor type.
    pub input_requirement: InputRequirement,
    /// Whether the processor runs when input queue is empty.
    pub trigger_when_empty: bool,
    /// Whether the processor is side-effect-free (idempotent).
    pub side_effect_free: bool,
    /// Whether the processor supports run-duration batching.
    pub supports_batching: bool,
}

inventory::collect!(ProcessorDescriptor);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_requirement_default_is_allowed() {
        assert_eq!(InputRequirement::default(), InputRequirement::Allowed);
    }

    #[test]
    fn input_requirement_equality() {
        assert_ne!(InputRequirement::Required, InputRequirement::Forbidden);
        assert_ne!(InputRequirement::Required, InputRequirement::Allowed);
        assert_ne!(InputRequirement::Allowed, InputRequirement::Forbidden);
    }
}
