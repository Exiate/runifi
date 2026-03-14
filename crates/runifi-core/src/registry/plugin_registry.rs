use dashmap::DashMap;
use runifi_plugin_api::{
    ControllerServiceDescriptor, Processor, ProcessorDescriptor, ReportingTask,
    ReportingTaskDescriptor, Sink, SinkDescriptor, Source, SourceDescriptor,
};

/// Registry of all discovered plugin descriptors.
///
/// Uses `inventory` to discover all processors, sources, sinks, services,
/// and reporting tasks that were registered at compile time via `inventory::submit!`.
pub struct PluginRegistry {
    processors: DashMap<&'static str, &'static ProcessorDescriptor>,
    sources: DashMap<&'static str, &'static SourceDescriptor>,
    sinks: DashMap<&'static str, &'static SinkDescriptor>,
    services: DashMap<&'static str, &'static ControllerServiceDescriptor>,
    reporting_tasks: DashMap<&'static str, &'static ReportingTaskDescriptor>,
}

impl PluginRegistry {
    /// Discover all registered plugins via `inventory`.
    pub fn discover() -> Self {
        let registry = Self {
            processors: DashMap::new(),
            sources: DashMap::new(),
            sinks: DashMap::new(),
            services: DashMap::new(),
            reporting_tasks: DashMap::new(),
        };

        for desc in inventory::iter::<ProcessorDescriptor> {
            tracing::info!(type_name = desc.type_name, "Registered processor");
            registry.processors.insert(desc.type_name, desc);
        }

        for desc in inventory::iter::<SourceDescriptor> {
            tracing::info!(type_name = desc.type_name, "Registered source");
            registry.sources.insert(desc.type_name, desc);
        }

        for desc in inventory::iter::<SinkDescriptor> {
            tracing::info!(type_name = desc.type_name, "Registered sink");
            registry.sinks.insert(desc.type_name, desc);
        }

        for desc in inventory::iter::<ControllerServiceDescriptor> {
            tracing::info!(type_name = desc.type_name, "Registered controller service");
            registry.services.insert(desc.type_name, desc);
        }

        for desc in inventory::iter::<ReportingTaskDescriptor> {
            tracing::info!(type_name = desc.type_name, "Registered reporting task");
            registry.reporting_tasks.insert(desc.type_name, desc);
        }

        registry
    }

    /// Create a new processor instance by type name.
    pub fn create_processor(&self, type_name: &str) -> Option<Box<dyn Processor>> {
        self.processors
            .get(type_name)
            .map(|desc| (desc.value().factory)())
    }

    /// Create a new source instance by type name.
    pub fn create_source(&self, type_name: &str) -> Option<Box<dyn Source>> {
        self.sources
            .get(type_name)
            .map(|desc| (desc.value().factory)())
    }

    /// Create a new sink instance by type name.
    pub fn create_sink(&self, type_name: &str) -> Option<Box<dyn Sink>> {
        self.sinks
            .get(type_name)
            .map(|desc| (desc.value().factory)())
    }

    /// List all registered processor type names.
    pub fn processor_types(&self) -> Vec<&'static str> {
        self.processors.iter().map(|e| *e.key()).collect()
    }

    /// List all registered source type names.
    pub fn source_types(&self) -> Vec<&'static str> {
        self.sources.iter().map(|e| *e.key()).collect()
    }

    /// List all registered sink type names.
    pub fn sink_types(&self) -> Vec<&'static str> {
        self.sinks.iter().map(|e| *e.key()).collect()
    }

    /// Create a new controller service instance by type name.
    pub fn create_service(
        &self,
        type_name: &str,
    ) -> Option<Box<dyn runifi_plugin_api::ControllerService>> {
        self.services
            .get(type_name)
            .map(|desc| (desc.value().factory)())
    }

    /// List all registered controller service type names.
    pub fn service_types(&self) -> Vec<&'static str> {
        self.services.iter().map(|e| *e.key()).collect()
    }

    /// Get the description for a controller service type.
    pub fn service_description(&self, type_name: &str) -> Option<&'static str> {
        self.services.get(type_name).map(|desc| desc.description)
    }

    /// Get tags for a processor type.
    pub fn processor_tags(&self, type_name: &str) -> Vec<String> {
        self.processors
            .get(type_name)
            .map(|desc| desc.tags.iter().map(|t| t.to_string()).collect())
            .unwrap_or_default()
    }

    /// Get tags for a source type.
    pub fn source_tags(&self, type_name: &str) -> Vec<String> {
        self.sources
            .get(type_name)
            .map(|desc| desc.tags.iter().map(|t| t.to_string()).collect())
            .unwrap_or_default()
    }

    /// Get tags for a sink type.
    pub fn sink_tags(&self, type_name: &str) -> Vec<String> {
        self.sinks
            .get(type_name)
            .map(|desc| desc.tags.iter().map(|t| t.to_string()).collect())
            .unwrap_or_default()
    }

    /// Create a new reporting task instance by type name.
    pub fn create_reporting_task(&self, type_name: &str) -> Option<Box<dyn ReportingTask>> {
        self.reporting_tasks
            .get(type_name)
            .map(|desc| (desc.value().factory)())
    }

    /// List all registered reporting task type names.
    pub fn reporting_task_types(&self) -> Vec<&'static str> {
        self.reporting_tasks.iter().map(|e| *e.key()).collect()
    }

    /// Get tags for a reporting task type.
    pub fn reporting_task_tags(&self, type_name: &str) -> Vec<String> {
        self.reporting_tasks
            .get(type_name)
            .map(|desc| desc.tags.iter().map(|t| t.to_string()).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_finds_registered_processors() {
        let registry = PluginRegistry::discover();
        let types = registry.processor_types();
        // We can't guarantee specific types without linking runifi-processors,
        // but the API should work without panicking.
        let _ = types;
    }

    #[test]
    fn create_unknown_processor_returns_none() {
        let registry = PluginRegistry::discover();
        assert!(registry.create_processor("NonexistentProcessor").is_none());
    }

    #[test]
    fn create_unknown_source_returns_none() {
        let registry = PluginRegistry::discover();
        assert!(registry.create_source("NonexistentSource").is_none());
    }

    #[test]
    fn create_unknown_sink_returns_none() {
        let registry = PluginRegistry::discover();
        assert!(registry.create_sink("NonexistentSink").is_none());
    }

    #[test]
    fn create_unknown_service_returns_none() {
        let registry = PluginRegistry::discover();
        assert!(registry.create_service("NonexistentService").is_none());
    }

    #[test]
    fn processor_tags_for_unknown_type_returns_empty() {
        let registry = PluginRegistry::discover();
        let tags = registry.processor_tags("NonexistentProcessor");
        assert!(tags.is_empty());
    }

    #[test]
    fn service_description_for_unknown_type_returns_none() {
        let registry = PluginRegistry::discover();
        assert!(registry.service_description("NonexistentService").is_none());
    }

    #[test]
    fn source_types_does_not_panic() {
        let registry = PluginRegistry::discover();
        let _ = registry.source_types();
    }

    #[test]
    fn sink_types_does_not_panic() {
        let registry = PluginRegistry::discover();
        let _ = registry.sink_types();
    }

    #[test]
    fn service_types_does_not_panic() {
        let registry = PluginRegistry::discover();
        let _ = registry.service_types();
    }
}
