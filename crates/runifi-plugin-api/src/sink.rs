use crate::context::ProcessContext;
use crate::property::PropertyDescriptor;
use crate::relationship::Relationship;
use crate::result::ProcessResult;
use crate::session::ProcessSession;

/// A sink processor that consumes FlowFiles (terminal node in the flow).
pub trait Sink: Send + Sync + 'static {
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

/// Describes a sink type for plugin registration.
pub struct SinkDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn Sink>,
}

inventory::collect!(SinkDescriptor);
