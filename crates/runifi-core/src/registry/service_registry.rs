use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use runifi_plugin_api::ControllerService;
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::service::ServiceLookup;

/// Lifecycle state of a controller service instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    /// Created but not yet configured.
    Created,
    /// Configured but not enabled.
    Disabled,
    /// Active and available to processors.
    Enabled,
}

impl ServiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceState::Created => "CREATED",
            ServiceState::Disabled => "DISABLED",
            ServiceState::Enabled => "ENABLED",
        }
    }
}

/// A managed controller service instance.
struct ManagedService {
    type_name: String,
    service: Arc<RwLock<Box<dyn ControllerService>>>,
    state: ServiceState,
    properties: HashMap<String, String>,
    /// Processors that reference this service.
    referencing_processors: HashSet<String>,
}

/// Runtime registry of controller service instances.
///
/// Manages service lifecycle (create → configure → enable → disable → remove)
/// and tracks which processors reference each service.
pub struct ServiceRegistry {
    services: HashMap<String, ManagedService>,
}

/// Information about a service instance, suitable for API responses.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub type_name: String,
    pub state: ServiceState,
    pub properties: HashMap<String, String>,
    pub property_descriptors: Vec<ServicePropertyDescriptorInfo>,
    pub referencing_processors: Vec<String>,
}

/// Static metadata about a service property.
#[derive(Debug, Clone)]
pub struct ServicePropertyDescriptorInfo {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
}

impl From<&PropertyDescriptor> for ServicePropertyDescriptorInfo {
    fn from(pd: &PropertyDescriptor) -> Self {
        Self {
            name: pd.name.to_string(),
            description: pd.description.to_string(),
            required: pd.required,
            default_value: pd.default_value.map(|v| v.to_string()),
            sensitive: pd.sensitive,
        }
    }
}

/// Error type for service registry operations.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("service not found: {0}")]
    NotFound(String),

    #[error("duplicate service name: {0}")]
    DuplicateName(String),

    #[error("unknown service type: {0}")]
    UnknownType(String),

    #[error("invalid state: service '{name}' is {current_state}, expected {expected_state}")]
    InvalidState {
        name: String,
        current_state: String,
        expected_state: String,
    },

    #[error("service '{name}' is referenced by processors: {processors}")]
    HasReferences { name: String, processors: String },

    #[error("service configuration failed: {0}")]
    ConfigFailed(String),

    #[error("service validation failed: {0}")]
    ValidationFailed(String),

    #[error("service enable failed: {0}")]
    EnableFailed(String),
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
        }
    }

    /// Add a new service instance. Starts in `Created` state.
    pub fn add_service(
        &mut self,
        name: String,
        type_name: String,
        service: Box<dyn ControllerService>,
    ) -> Result<(), ServiceError> {
        if self.services.contains_key(&name) {
            return Err(ServiceError::DuplicateName(name));
        }

        self.services.insert(
            name,
            ManagedService {
                type_name,
                service: Arc::new(RwLock::new(service)),
                state: ServiceState::Created,
                properties: HashMap::new(),
                referencing_processors: HashSet::new(),
            },
        );

        Ok(())
    }

    /// Configure a service with properties. Transitions to `Disabled` state.
    pub fn configure_service(
        &mut self,
        name: &str,
        properties: HashMap<String, String>,
    ) -> Result<(), ServiceError> {
        let managed = self
            .services
            .get_mut(name)
            .ok_or_else(|| ServiceError::NotFound(name.to_string()))?;

        if managed.state == ServiceState::Enabled {
            return Err(ServiceError::InvalidState {
                name: name.to_string(),
                current_state: managed.state.as_str().to_string(),
                expected_state: "CREATED or DISABLED".to_string(),
            });
        }

        let mut svc = managed.service.write();
        svc.on_configure(&properties)
            .map_err(|e| ServiceError::ConfigFailed(format!("service '{}': {}", name, e)))?;
        drop(svc);

        managed.properties = properties;
        managed.state = ServiceState::Disabled;

        Ok(())
    }

    /// Enable a service. Validates first, then calls `enable()`.
    pub fn enable_service(&mut self, name: &str) -> Result<(), ServiceError> {
        let managed = self
            .services
            .get_mut(name)
            .ok_or_else(|| ServiceError::NotFound(name.to_string()))?;

        if managed.state == ServiceState::Enabled {
            return Ok(()); // Already enabled, idempotent.
        }

        if managed.state == ServiceState::Created {
            return Err(ServiceError::InvalidState {
                name: name.to_string(),
                current_state: "CREATED".to_string(),
                expected_state: "DISABLED (configure first)".to_string(),
            });
        }

        let mut svc = managed.service.write();
        svc.validate()
            .map_err(|e| ServiceError::ValidationFailed(format!("service '{}': {}", name, e)))?;

        svc.enable()
            .map_err(|e| ServiceError::EnableFailed(format!("service '{}': {}", name, e)))?;
        drop(svc);

        managed.state = ServiceState::Enabled;
        Ok(())
    }

    /// Disable a service. Fails if processors still reference it.
    pub fn disable_service(&mut self, name: &str) -> Result<(), ServiceError> {
        let managed = self
            .services
            .get_mut(name)
            .ok_or_else(|| ServiceError::NotFound(name.to_string()))?;

        if managed.state != ServiceState::Enabled {
            return Ok(()); // Already disabled, idempotent.
        }

        if !managed.referencing_processors.is_empty() {
            let procs: Vec<&str> = managed
                .referencing_processors
                .iter()
                .map(|s| s.as_str())
                .collect();
            return Err(ServiceError::HasReferences {
                name: name.to_string(),
                processors: procs.join(", "),
            });
        }

        let mut svc = managed.service.write();
        svc.disable()
            .map_err(|e| ServiceError::ConfigFailed(format!("service '{}': {}", name, e)))?;
        drop(svc);

        managed.state = ServiceState::Disabled;
        Ok(())
    }

    /// Remove a service. Must be disabled and have no references.
    pub fn remove_service(&mut self, name: &str) -> Result<(), ServiceError> {
        let managed = self
            .services
            .get(name)
            .ok_or_else(|| ServiceError::NotFound(name.to_string()))?;

        if managed.state == ServiceState::Enabled {
            return Err(ServiceError::InvalidState {
                name: name.to_string(),
                current_state: "ENABLED".to_string(),
                expected_state: "DISABLED or CREATED".to_string(),
            });
        }

        if !managed.referencing_processors.is_empty() {
            let procs: Vec<&str> = managed
                .referencing_processors
                .iter()
                .map(|s| s.as_str())
                .collect();
            return Err(ServiceError::HasReferences {
                name: name.to_string(),
                processors: procs.join(", "),
            });
        }

        self.services.remove(name);
        Ok(())
    }

    /// Register that a processor references a service.
    pub fn add_reference(
        &mut self,
        service_name: &str,
        processor_name: &str,
    ) -> Result<(), ServiceError> {
        let managed = self
            .services
            .get_mut(service_name)
            .ok_or_else(|| ServiceError::NotFound(service_name.to_string()))?;
        managed
            .referencing_processors
            .insert(processor_name.to_string());
        Ok(())
    }

    /// Remove a processor's reference to a service.
    pub fn remove_reference(&mut self, service_name: &str, processor_name: &str) {
        if let Some(managed) = self.services.get_mut(service_name) {
            managed.referencing_processors.remove(processor_name);
        }
    }

    /// Get information about all services.
    pub fn list_services(&self) -> Vec<ServiceInfo> {
        self.services
            .iter()
            .map(|(name, managed)| {
                let svc = managed.service.read();
                let descriptors = svc
                    .property_descriptors()
                    .iter()
                    .map(ServicePropertyDescriptorInfo::from)
                    .collect();
                drop(svc);

                ServiceInfo {
                    name: name.clone(),
                    type_name: managed.type_name.clone(),
                    state: managed.state,
                    properties: managed.properties.clone(),
                    property_descriptors: descriptors,
                    referencing_processors: managed
                        .referencing_processors
                        .iter()
                        .cloned()
                        .collect(),
                }
            })
            .collect()
    }

    /// Get information about a specific service.
    pub fn get_service(&self, name: &str) -> Option<ServiceInfo> {
        self.services.get(name).map(|managed| {
            let svc = managed.service.read();
            let descriptors = svc
                .property_descriptors()
                .iter()
                .map(ServicePropertyDescriptorInfo::from)
                .collect();
            drop(svc);

            ServiceInfo {
                name: name.to_string(),
                type_name: managed.type_name.clone(),
                state: managed.state,
                properties: managed.properties.clone(),
                property_descriptors: descriptors,
                referencing_processors: managed.referencing_processors.iter().cloned().collect(),
            }
        })
    }

    /// Get the underlying service Arc (for ServiceLookup).
    /// Only returns enabled services.
    fn get_enabled_service(&self, name: &str) -> Option<Arc<RwLock<Box<dyn ControllerService>>>> {
        self.services.get(name).and_then(|managed| {
            if managed.state == ServiceState::Enabled {
                Some(managed.service.clone())
            } else {
                None
            }
        })
    }
}

/// Shared service registry wrapped in an Arc<RwLock> for concurrent access.
#[derive(Clone)]
pub struct SharedServiceRegistry {
    inner: Arc<RwLock<ServiceRegistry>>,
}

impl Default for SharedServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedServiceRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ServiceRegistry::new())),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, ServiceRegistry> {
        self.inner.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, ServiceRegistry> {
        self.inner.write()
    }
}

/// Adapter that implements `ServiceLookup` by reading from the shared registry.
///
/// This adapter wraps the inner `ControllerService` in a newtype that
/// implements `ControllerService` by delegating through the `RwLock`.
pub struct RegistryServiceLookup {
    registry: SharedServiceRegistry,
}

impl RegistryServiceLookup {
    pub fn new(registry: SharedServiceRegistry) -> Self {
        Self { registry }
    }
}

impl ServiceLookup for RegistryServiceLookup {
    fn get_service(&self, name: &str) -> Option<Arc<dyn ControllerService>> {
        let reg = self.registry.read();
        reg.get_enabled_service(name)
            .map(|svc| -> Arc<dyn ControllerService> { Arc::new(LockedServiceAdapter(svc)) })
    }
}

/// Wraps `Arc<RwLock<Box<dyn ControllerService>>>` into an `Arc<dyn ControllerService>`.
///
/// This is needed because processors expect `Arc<dyn ControllerService>` from
/// `ServiceLookup`, but the registry stores services behind a `RwLock` for
/// lifecycle management. The adapter delegates all trait methods through the lock.
struct LockedServiceAdapter(Arc<RwLock<Box<dyn ControllerService>>>);

impl ControllerService for LockedServiceAdapter {
    fn on_configure(
        &mut self,
        properties: &HashMap<String, String>,
    ) -> runifi_plugin_api::ProcessResult {
        self.0.write().on_configure(properties)
    }

    fn validate(&self) -> runifi_plugin_api::ProcessResult {
        self.0.read().validate()
    }

    fn enable(&mut self) -> runifi_plugin_api::ProcessResult {
        self.0.write().enable()
    }

    fn disable(&mut self) -> runifi_plugin_api::ProcessResult {
        self.0.write().disable()
    }

    fn is_enabled(&self) -> bool {
        self.0.read().is_enabled()
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        self.0.read().property_descriptors()
    }
}

// SAFETY: The inner RwLock handles synchronization.
unsafe impl Send for LockedServiceAdapter {}
unsafe impl Sync for LockedServiceAdapter {}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::ProcessResult;

    struct TestService {
        enabled: bool,
        configured: bool,
    }

    impl TestService {
        fn new() -> Self {
            Self {
                enabled: false,
                configured: false,
            }
        }
    }

    impl ControllerService for TestService {
        fn on_configure(&mut self, _properties: &HashMap<String, String>) -> ProcessResult {
            self.configured = true;
            Ok(())
        }

        fn validate(&self) -> ProcessResult {
            if !self.configured {
                return Err(runifi_plugin_api::PluginError::ProcessingFailed(
                    "not configured".to_string(),
                ));
            }
            Ok(())
        }

        fn enable(&mut self) -> ProcessResult {
            self.enabled = true;
            Ok(())
        }

        fn disable(&mut self) -> ProcessResult {
            self.enabled = false;
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            self.enabled
        }

        fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
            vec![PropertyDescriptor::new(
                "cache.size",
                "Maximum cache entries",
            )]
        }
    }

    #[test]
    fn test_service_lifecycle() {
        let mut registry = ServiceRegistry::new();

        // Add service.
        registry
            .add_service(
                "my-cache".to_string(),
                "TestCacheService".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();

        // Cannot enable before configuring.
        assert!(registry.enable_service("my-cache").is_err());

        // Configure.
        registry
            .configure_service(
                "my-cache",
                HashMap::from([("cache.size".to_string(), "1000".to_string())]),
            )
            .unwrap();

        // Enable.
        registry.enable_service("my-cache").unwrap();

        let info = registry.get_service("my-cache").unwrap();
        assert_eq!(info.state, ServiceState::Enabled);
        assert_eq!(info.type_name, "TestCacheService");

        // Disable.
        registry.disable_service("my-cache").unwrap();
        let info = registry.get_service("my-cache").unwrap();
        assert_eq!(info.state, ServiceState::Disabled);

        // Remove.
        registry.remove_service("my-cache").unwrap();
        assert!(registry.get_service("my-cache").is_none());
    }

    #[test]
    fn test_duplicate_name_rejected() {
        let mut registry = ServiceRegistry::new();
        registry
            .add_service(
                "svc1".to_string(),
                "Type".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();
        let err = registry
            .add_service(
                "svc1".to_string(),
                "Type".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap_err();
        assert!(matches!(err, ServiceError::DuplicateName(_)));
    }

    #[test]
    fn test_cannot_disable_with_references() {
        let mut registry = ServiceRegistry::new();
        registry
            .add_service(
                "svc1".to_string(),
                "Type".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();
        registry.configure_service("svc1", HashMap::new()).unwrap();
        registry.enable_service("svc1").unwrap();

        // Add a reference.
        registry.add_reference("svc1", "processor-1").unwrap();

        // Cannot disable.
        let err = registry.disable_service("svc1").unwrap_err();
        assert!(matches!(err, ServiceError::HasReferences { .. }));

        // Remove reference, then disable.
        registry.remove_reference("svc1", "processor-1");
        registry.disable_service("svc1").unwrap();
    }

    #[test]
    fn test_cannot_remove_enabled_service() {
        let mut registry = ServiceRegistry::new();
        registry
            .add_service(
                "svc1".to_string(),
                "Type".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();
        registry.configure_service("svc1", HashMap::new()).unwrap();
        registry.enable_service("svc1").unwrap();

        let err = registry.remove_service("svc1").unwrap_err();
        assert!(matches!(err, ServiceError::InvalidState { .. }));
    }

    #[test]
    fn test_service_lookup() {
        let shared = SharedServiceRegistry::new();

        {
            let mut reg = shared.write();
            reg.add_service(
                "svc1".to_string(),
                "Type".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();
            reg.configure_service("svc1", HashMap::new()).unwrap();
            reg.enable_service("svc1").unwrap();
        }

        let lookup = RegistryServiceLookup::new(shared.clone());

        // Enabled service is found.
        assert!(lookup.get_service("svc1").is_some());

        // Non-existent service is not found.
        assert!(lookup.get_service("no-such").is_none());

        // Disabled service is not found.
        {
            let mut reg = shared.write();
            reg.disable_service("svc1").unwrap();
        }
        assert!(lookup.get_service("svc1").is_none());
    }

    #[test]
    fn test_list_services() {
        let mut registry = ServiceRegistry::new();
        registry
            .add_service(
                "svc1".to_string(),
                "TypeA".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();
        registry
            .add_service(
                "svc2".to_string(),
                "TypeB".to_string(),
                Box::new(TestService::new()),
            )
            .unwrap();

        let list = registry.list_services();
        assert_eq!(list.len(), 2);
    }
}
