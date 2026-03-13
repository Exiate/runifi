use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use runifi_core::config::flow_config::SecurityConfig;
use runifi_core::engine::handle::EngineHandle;
use runifi_core::registry::plugin_registry::PluginRegistry;

/// Shared API state, cheap to clone via Arc.
#[derive(Clone)]
#[allow(dead_code)]
pub struct ApiState {
    pub handle: Arc<EngineHandle>,
    /// Current number of active SSE connections.
    pub sse_connections: Arc<AtomicUsize>,
    /// Maximum allowed concurrent SSE connections.
    pub max_sse_connections: usize,
    /// Whether to include detailed internal names in error messages.
    pub detailed_errors: bool,
    /// Security configuration (API keys, TLS settings).
    pub security: SecurityConfig,
    /// Plugin registry for creating service instances at runtime.
    pub plugin_registry: Option<Arc<PluginRegistry>>,
}

impl ApiState {
    #[allow(dead_code)]
    pub fn new(handle: EngineHandle) -> Self {
        Self {
            handle: Arc::new(handle),
            sse_connections: Arc::new(AtomicUsize::new(0)),
            max_sse_connections: 50,
            detailed_errors: false,
            security: SecurityConfig::default(),
            plugin_registry: None,
        }
    }

    /// Create with explicit configuration.
    pub fn with_config(
        handle: EngineHandle,
        max_sse_connections: usize,
        detailed_errors: bool,
        security: SecurityConfig,
    ) -> Self {
        Self {
            handle: Arc::new(handle),
            sse_connections: Arc::new(AtomicUsize::new(0)),
            max_sse_connections,
            detailed_errors,
            security,
            plugin_registry: None,
        }
    }

    /// Set the plugin registry (called during API server setup).
    pub fn set_plugin_registry(&mut self, registry: Arc<PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    /// Create a controller service instance by type name.
    pub fn create_controller_service(
        &self,
        type_name: &str,
    ) -> Option<Box<dyn runifi_plugin_api::ControllerService>> {
        self.plugin_registry
            .as_ref()
            .and_then(|r| r.create_service(type_name))
    }
}
