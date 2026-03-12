use crate::property::PropertyDescriptor;
use crate::result::ProcessResult;

/// A shared service that processors can reference (e.g. connection pools, SSL contexts).
pub trait ControllerService: Send + Sync + 'static {
    /// Enable the service, making it available to processors.
    fn enable(&mut self) -> ProcessResult;

    /// Disable the service.
    fn disable(&mut self) -> ProcessResult;

    /// Check if the service is currently enabled.
    fn is_enabled(&self) -> bool;

    /// The properties this service accepts.
    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        Vec::new()
    }
}

/// Describes a controller service type for plugin registration.
pub struct ControllerServiceDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn ControllerService>,
}

inventory::collect!(ControllerServiceDescriptor);
