use dashmap::DashMap;
use runifi_plugin_api::{
    ControllerServiceDescriptor, Processor, ProcessorDescriptor, Sink, SinkDescriptor, Source,
    SourceDescriptor,
};

/// Registry of all discovered plugin descriptors.
///
/// Uses `inventory` to discover all processors, sources, sinks, and services
/// that were registered at compile time via `inventory::submit!`.
pub struct PluginRegistry {
    processors: DashMap<&'static str, &'static ProcessorDescriptor>,
    sources: DashMap<&'static str, &'static SourceDescriptor>,
    sinks: DashMap<&'static str, &'static SinkDescriptor>,
    services: DashMap<&'static str, &'static ControllerServiceDescriptor>,
}

impl PluginRegistry {
    /// Discover all registered plugins via `inventory`.
    pub fn discover() -> Self {
        let registry = Self {
            processors: DashMap::new(),
            sources: DashMap::new(),
            sinks: DashMap::new(),
            services: DashMap::new(),
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
}
