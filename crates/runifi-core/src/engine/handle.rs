use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use super::metrics::ProcessorMetrics;
use super::processor_node::SchedulingStrategy;
use crate::connection::flow_connection::FlowConnection;
use crate::repository::content_repo::ContentRepository;

/// Static metadata about a processor property, suitable for API responses.
#[derive(Debug, Clone)]
pub struct PropertyDescriptorInfo {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
    pub allowed_values: Option<Vec<String>>,
}

/// Static metadata about a processor relationship, suitable for API responses.
#[derive(Debug, Clone)]
pub struct RelationshipInfo {
    pub name: String,
    pub description: String,
    pub auto_terminated: bool,
}

/// Information about a processor instance, visible to the API.
#[derive(Clone)]
pub struct ProcessorInfo {
    pub name: String,
    pub type_name: String,
    pub scheduling: SchedulingStrategy,
    pub metrics: Arc<ProcessorMetrics>,
    /// Property descriptors (static metadata from the processor type).
    pub property_descriptors: Vec<PropertyDescriptorInfo>,
    /// Relationships (static metadata from the processor type).
    pub relationships: Vec<RelationshipInfo>,
    /// Current property values, shared with the processor node for runtime updates.
    pub properties: Arc<RwLock<HashMap<String, String>>>,
}

/// Information about a connection, visible to the API.
#[derive(Clone)]
pub struct ConnectionInfo {
    pub id: String,
    pub source_name: String,
    pub relationship: String,
    pub dest_name: String,
    pub connection: Arc<FlowConnection>,
}

/// Information about a registered plugin type.
#[derive(Clone)]
pub struct PluginTypeInfo {
    pub type_name: String,
    pub kind: PluginKind,
}

#[derive(Debug, Clone, Copy)]
pub enum PluginKind {
    Processor,
    Source,
    Sink,
}

/// A Clone-able, Send+Sync handle for API queries against a running engine.
///
/// Created by `FlowEngine::start()` before tasks are spawned, providing
/// read-only access to metrics and connection state without holding
/// a reference to the engine itself.
#[derive(Clone)]
pub struct EngineHandle {
    pub flow_name: String,
    pub started_at: Instant,
    pub processors: Arc<Vec<ProcessorInfo>>,
    pub connections: Arc<Vec<ConnectionInfo>>,
    pub plugin_types: Arc<Vec<PluginTypeInfo>>,
    pub content_repo: Arc<dyn ContentRepository>,
}

impl EngineHandle {
    /// Request a circuit breaker reset for a processor by name.
    /// Returns `true` if the processor was found and the flag was set.
    pub fn request_circuit_reset(&self, name: &str) -> bool {
        for info in self.processors.iter() {
            if info.name == name {
                info.metrics
                    .reset_requested
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Stop a processor by name (set enabled=false).
    /// Returns `true` if the processor was found.
    pub fn stop_processor(&self, name: &str) -> bool {
        for info in self.processors.iter() {
            if info.name == name {
                info.metrics
                    .enabled
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Start a processor by name (set enabled=true).
    /// Returns `true` if the processor was found.
    pub fn start_processor(&self, name: &str) -> bool {
        for info in self.processors.iter() {
            if info.name == name {
                info.metrics
                    .enabled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Pause a processor by name (set paused=true).
    /// Returns `true` if the processor was found.
    pub fn pause_processor(&self, name: &str) -> bool {
        for info in self.processors.iter() {
            if info.name == name {
                info.metrics
                    .paused
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Resume a processor by name (set paused=false).
    /// Returns `true` if the processor was found.
    pub fn resume_processor(&self, name: &str) -> bool {
        for info in self.processors.iter() {
            if info.name == name {
                info.metrics
                    .paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Get the processor info for a processor by name.
    pub fn get_processor_info(&self, name: &str) -> Option<&ProcessorInfo> {
        self.processors.iter().find(|p| p.name == name)
    }

    /// Update properties for a processor by name.
    /// Returns `Ok(())` if successful, `Err(reason)` if the processor is not found
    /// or is not in a stopped state.
    pub fn update_processor_properties(
        &self,
        name: &str,
        new_properties: HashMap<String, String>,
    ) -> Result<(), String> {
        let info = self
            .processors
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| format!("Processor not found: {}", name))?;

        // Require processor to be stopped (enabled=false and not active).
        let enabled = info
            .metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed);
        let active = info
            .metrics
            .active
            .load(std::sync::atomic::Ordering::Relaxed);

        if enabled || active {
            return Err(
                "Processor must be stopped before updating configuration".to_string(),
            );
        }

        // Validate required properties.
        for desc in &info.property_descriptors {
            if desc.required {
                let has_value = new_properties.contains_key(&desc.name);
                let has_default = desc.default_value.is_some();
                if !has_value && !has_default {
                    return Err(format!("Required property '{}' is missing", desc.name));
                }
            }
        }

        // Validate allowed values.
        for (key, value) in &new_properties {
            if let Some(desc) = info.property_descriptors.iter().find(|d| d.name == *key)
                && let Some(ref allowed) = desc.allowed_values
                && !allowed.iter().any(|v| v == value)
            {
                return Err(format!(
                    "Invalid value '{}' for property '{}'. Allowed: {:?}",
                    value, key, allowed
                ));
            }
        }

        // Apply the new properties.
        let mut props = info.properties.write();
        *props = new_properties;

        Ok(())
    }
}
