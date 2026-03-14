use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use runifi_core::auth::chain::AuthProviderChain;
use runifi_core::auth::identity_mapper::IdentityMapper;
use runifi_core::auth::jwt::JwtConfig;
use runifi_core::auth::session::SessionManager;
use runifi_core::auth::store::UserStore;
use runifi_core::cluster::ClusterCoordinator;
use runifi_core::config::flow_config::{AuthConfig, SecurityConfig};
use runifi_core::engine::handle::EngineHandle;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::versioning::FlowVersionStore;

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
    /// User store for identity management.
    pub user_store: Arc<UserStore>,
    /// JWT configuration for token generation/validation.
    pub jwt_config: Option<Arc<JwtConfig>>,
    /// Auth configuration.
    pub auth_config: AuthConfig,
    /// Flow version store for git-based versioning.
    pub version_store: Option<Arc<FlowVersionStore>>,
    /// Authentication provider chain (pluggable enterprise auth).
    pub auth_chain: Option<Arc<AuthProviderChain>>,
    /// Identity-to-role mapper for external providers.
    pub identity_mapper: Arc<IdentityMapper>,
    /// Session manager for unified session handling.
    pub session_manager: Option<Arc<SessionManager>>,
    /// Cluster coordinator for cluster management operations.
    pub cluster_coordinator: Option<Arc<ClusterCoordinator>>,
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
            user_store: Arc::new(UserStore::new()),
            jwt_config: None,
            auth_config: AuthConfig::default(),
            version_store: None,
            auth_chain: None,
            identity_mapper: Arc::new(IdentityMapper::default()),
            session_manager: None,
            cluster_coordinator: None,
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
            user_store: Arc::new(UserStore::new()),
            jwt_config: None,
            auth_config: AuthConfig::default(),
            version_store: None,
            auth_chain: None,
            identity_mapper: Arc::new(IdentityMapper::default()),
            session_manager: None,
            cluster_coordinator: None,
        }
    }

    /// Set the plugin registry (called during API server setup).
    pub fn set_plugin_registry(&mut self, registry: Arc<PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    /// Set the user store and JWT config for identity-based auth.
    pub fn set_auth(
        &mut self,
        user_store: Arc<UserStore>,
        jwt_config: JwtConfig,
        auth_config: AuthConfig,
    ) {
        self.user_store = user_store;
        self.jwt_config = Some(Arc::new(jwt_config));
        self.auth_config = auth_config;
    }

    /// Set the enterprise auth chain, identity mapper, and session manager.
    pub fn set_auth_chain(
        &mut self,
        chain: Arc<AuthProviderChain>,
        mapper: Arc<IdentityMapper>,
        session_manager: Arc<SessionManager>,
    ) {
        self.auth_chain = Some(chain);
        self.identity_mapper = mapper;
        self.session_manager = Some(session_manager);
    }

    /// Returns `true` if JWT-based user auth is enabled.
    pub fn user_auth_enabled(&self) -> bool {
        self.auth_config.enabled && self.jwt_config.is_some()
    }

    /// Set the flow version store for git-based versioning.
    pub fn set_version_store(&mut self, store: Arc<FlowVersionStore>) {
        self.version_store = Some(store);
    }

    /// Set the cluster coordinator for cluster management operations.
    pub fn set_cluster_coordinator(&mut self, coordinator: Arc<ClusterCoordinator>) {
        self.cluster_coordinator = Some(coordinator);
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

    /// Create a reporting task instance by type name.
    pub fn create_reporting_task(
        &self,
        type_name: &str,
    ) -> Option<Box<dyn runifi_plugin_api::ReportingTask>> {
        self.plugin_registry
            .as_ref()
            .and_then(|r| r.create_reporting_task(type_name))
    }
}
