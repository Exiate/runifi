use crate::property::PropertyValue;

/// Provides runtime context to a processor during execution.
///
/// Implemented by the engine — processors receive this as a `&dyn ProcessContext`.
pub trait ProcessContext: Send + Sync {
    /// Get a resolved property value by name.
    fn get_property(&self, name: &str) -> PropertyValue;

    /// The configured instance name of this processor.
    fn name(&self) -> &str;

    /// The unique instance ID of this processor.
    fn id(&self) -> &str;

    /// How long (ms) the engine should wait before re-triggering after a yield.
    fn yield_duration_ms(&self) -> u64;
}
