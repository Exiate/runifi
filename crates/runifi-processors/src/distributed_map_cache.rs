use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

const PROP_MAX_ENTRIES: PropertyDescriptor =
    PropertyDescriptor::new("Max Entries", "Maximum number of entries in the cache")
        .default_value("10000");

/// An in-memory distributed map cache service.
///
/// Provides a shared key-value cache that processors can use to coordinate
/// state across the flow. Modeled after NiFi's DistributedMapCacheServer.
pub struct DistributedMapCacheServer {
    enabled: bool,
    max_entries: usize,
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Default for DistributedMapCacheServer {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedMapCacheServer {
    pub fn new() -> Self {
        Self {
            enabled: false,
            max_entries: 10_000,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ControllerService for DistributedMapCacheServer {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        if let Some(max_str) = properties.get("Max Entries") {
            self.max_entries = max_str.parse::<usize>().map_err(|_| {
                runifi_plugin_api::PluginError::ProcessingFailed(format!(
                    "Invalid 'Max Entries' value: {}",
                    max_str
                ))
            })?;
        }
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        if self.max_entries == 0 {
            return Err(runifi_plugin_api::PluginError::ProcessingFailed(
                "'Max Entries' must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.cache = Arc::new(RwLock::new(HashMap::with_capacity(
            self.max_entries.min(1024),
        )));
        self.enabled = true;
        tracing::info!(
            max_entries = self.max_entries,
            "DistributedMapCacheServer enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.enabled = false;
        self.cache.write().clear();
        tracing::info!("DistributedMapCacheServer disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_MAX_ENTRIES]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "DistributedMapCacheServer",
        description: "In-memory key-value cache service for cross-processor state sharing",
        factory: || Box::new(DistributedMapCacheServer::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle() {
        let mut svc = DistributedMapCacheServer::new();

        // Configure.
        let props = HashMap::from([("Max Entries".to_string(), "500".to_string())]);
        svc.on_configure(&props).unwrap();
        assert_eq!(svc.max_entries, 500);

        // Validate.
        svc.validate().unwrap();

        // Enable.
        svc.enable().unwrap();
        assert!(svc.is_enabled());

        // Disable.
        svc.disable().unwrap();
        assert!(!svc.is_enabled());
    }

    #[test]
    fn test_validate_zero_entries_fails() {
        let mut svc = DistributedMapCacheServer::new();
        let props = HashMap::from([("Max Entries".to_string(), "0".to_string())]);
        svc.on_configure(&props).unwrap();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn test_invalid_max_entries() {
        let mut svc = DistributedMapCacheServer::new();
        let props = HashMap::from([("Max Entries".to_string(), "not-a-number".to_string())]);
        assert!(svc.on_configure(&props).is_err());
    }
}
