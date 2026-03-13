use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use runifi_core::config::flow_config::SecurityConfig;
use runifi_core::engine::handle::EngineHandle;

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
        }
    }
}
