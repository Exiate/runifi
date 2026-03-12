use std::sync::Arc;
use std::time::Instant;

use super::metrics::ProcessorMetrics;
use super::processor_node::SchedulingStrategy;
use crate::connection::flow_connection::FlowConnection;
use crate::repository::content_repo::ContentRepository;

/// Information about a processor instance, visible to the API.
#[derive(Clone)]
pub struct ProcessorInfo {
    pub name: String,
    pub type_name: String,
    pub scheduling: SchedulingStrategy,
    pub metrics: Arc<ProcessorMetrics>,
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
}
