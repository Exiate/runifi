use crate::property::PropertyValue;
use crate::service::ServiceLookup;

/// Provides runtime context to a processor during execution.
///
/// Implemented by the engine — processors receive this as a `&dyn ProcessContext`.
pub trait ProcessContext: Send + Sync {
    /// Get a resolved property value by name.
    fn get_property(&self, name: &str) -> PropertyValue;

    /// Return the names of all configured properties.
    ///
    /// This includes both declared descriptor properties and any dynamic
    /// (user-defined) properties set on the processor instance.
    fn property_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// The configured instance name of this processor.
    fn name(&self) -> &str;

    /// The unique instance ID of this processor.
    fn id(&self) -> &str;

    /// How long (ms) the engine should wait before re-triggering after a yield.
    fn yield_duration_ms(&self) -> u64;

    /// Access the controller service lookup.
    ///
    /// Returns `None` if no services are configured. Processors should use this
    /// to resolve shared services by name, e.g.:
    /// ```ignore
    /// if let Some(lookup) = context.service_lookup() {
    ///     if let Some(cache) = lookup.get_service("my-cache") { ... }
    /// }
    /// ```
    fn service_lookup(&self) -> Option<&dyn ServiceLookup> {
        None
    }
}
