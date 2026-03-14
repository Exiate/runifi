use std::collections::HashMap;
use std::sync::Arc;

use crate::property::PropertyDescriptor;
use crate::result::ProcessResult;

/// A shared service that processors can reference (e.g. connection pools, SSL contexts).
///
/// Lifecycle: `on_configure(properties)` → `validate()` → `enable()` → ... → `disable()`
///
/// Services are long-lived, shared resources. Processors reference them by name
/// through the `ServiceLookup` trait on `ProcessContext`.
pub trait ControllerService: Send + Sync + 'static {
    /// Configure the service with the given properties.
    ///
    /// Called before `enable()`. Properties are passed as a flat key-value map.
    /// The service should store the configuration for use during `enable()`.
    fn on_configure(&mut self, _properties: &HashMap<String, String>) -> ProcessResult {
        Ok(())
    }

    /// Validate the current configuration.
    ///
    /// Called after `on_configure()` and before `enable()`. Return an error if
    /// required properties are missing or invalid.
    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    /// Enable the service, making it available to processors.
    ///
    /// Called after `on_configure()` and `validate()` succeed.
    fn enable(&mut self) -> ProcessResult;

    /// Disable the service.
    ///
    /// After this call, the service should release any held resources.
    fn disable(&mut self) -> ProcessResult;

    /// Check if the service is currently enabled.
    fn is_enabled(&self) -> bool;

    /// The properties this service accepts.
    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        Vec::new()
    }
}

/// Trait for looking up controller services by name.
///
/// Added to `ProcessContext` so processors can resolve shared services.
pub trait ServiceLookup: Send + Sync {
    /// Look up a controller service by its instance name.
    ///
    /// Returns `None` if the service doesn't exist or isn't enabled.
    fn get_service(&self, name: &str) -> Option<Arc<dyn ControllerService>>;
}

/// Describes a controller service type for plugin registration.
pub struct ControllerServiceDescriptor {
    pub type_name: &'static str,
    pub description: &'static str,
    pub factory: fn() -> Box<dyn ControllerService>,
}

inventory::collect!(ControllerServiceDescriptor);
