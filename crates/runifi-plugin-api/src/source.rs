use crate::context::ProcessContext;
use crate::property::PropertyDescriptor;
use crate::relationship::Relationship;
use crate::result::ProcessResult;
use crate::session::ProcessSession;

/// A source processor that generates FlowFiles (no input queue).
///
/// Same lifecycle as `Processor`, but semantically indicates this component
/// produces data rather than transforming it.
pub trait Source: Send + Sync + 'static {
    fn on_scheduled(&mut self, _context: &dyn ProcessContext) -> ProcessResult {
        Ok(())
    }

    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult;

    fn on_stopped(&mut self, _context: &dyn ProcessContext) {
        // Default: no-op
    }

    fn relationships(&self) -> Vec<Relationship>;

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        Vec::new()
    }
}

/// Describes a source type for plugin registration.
pub struct SourceDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn Source>,
}

inventory::collect!(SourceDescriptor);
