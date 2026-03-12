use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::Processor;

use super::handle::{ConnectionInfo, EngineHandle, PluginTypeInfo, ProcessorInfo};
use super::metrics::ProcessorMetrics;
use super::processor_node::{ProcessorNode, SchedulingStrategy};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::flow_connection::FlowConnection;
use crate::error::{Result, RuniFiError};
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;

/// A unique identifier for a processor node in the engine.
pub type NodeId = usize;

/// A unique identifier for a connection in the engine.
pub type ConnId = usize;

/// The core flow engine — orchestrates a DAG of processors and connections.
///
/// Lifecycle: build the graph with `add_processor` / `connect`, then `start()`.
/// The engine spawns one tokio task per processor. Call `stop()` for graceful shutdown.
pub struct FlowEngine {
    flow_name: String,
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    cancel_token: CancellationToken,

    // Build-phase state (consumed on start).
    nodes: Vec<NodeBuilder>,
    connections: Vec<ConnBuilder>,
    next_node_id: NodeId,
    next_conn_id: ConnId,

    // Run-phase state.
    task_handles: Vec<JoinHandle<()>>,
    running: bool,
    handle: Option<EngineHandle>,
}

struct NodeBuilder {
    id: NodeId,
    name: String,
    type_name: String,
    processor: Option<Box<dyn Processor>>,
    scheduling: SchedulingStrategy,
    properties: HashMap<String, String>,
}

struct ConnBuilder {
    _id: ConnId,
    source_node: NodeId,
    relationship: &'static str,
    dest_node: NodeId,
    config: BackPressureConfig,
}

impl FlowEngine {
    pub fn new(flow_name: impl Into<String>, content_repo: Arc<dyn ContentRepository>) -> Self {
        Self {
            flow_name: flow_name.into(),
            content_repo,
            id_gen: Arc::new(IdGenerator::new()),
            cancel_token: CancellationToken::new(),
            nodes: Vec::new(),
            connections: Vec::new(),
            next_node_id: 0,
            next_conn_id: 0,
            task_handles: Vec::new(),
            running: false,
            handle: None,
        }
    }

    /// Add a processor to the engine. Returns a node ID for wiring connections.
    pub fn add_processor(
        &mut self,
        name: impl Into<String>,
        type_name: impl Into<String>,
        processor: Box<dyn Processor>,
        scheduling: SchedulingStrategy,
        properties: HashMap<String, String>,
    ) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;
        self.nodes.push(NodeBuilder {
            id,
            name: name.into(),
            type_name: type_name.into(),
            processor: Some(processor),
            scheduling,
            properties,
        });
        id
    }

    /// Connect two processors via a relationship.
    pub fn connect(
        &mut self,
        source_id: NodeId,
        relationship: &'static str,
        dest_id: NodeId,
        config: BackPressureConfig,
    ) -> ConnId {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections.push(ConnBuilder {
            _id: id,
            source_node: source_id,
            relationship,
            dest_node: dest_id,
            config,
        });
        id
    }

    /// Start the engine — validates the DAG and spawns a task per processor.
    pub async fn start(&mut self) -> Result<()> {
        if self.running {
            return Err(RuniFiError::EngineAlreadyRunning);
        }

        // Build FlowConnections.
        let mut flow_connections: Vec<(usize, &'static str, usize, Arc<FlowConnection>)> =
            Vec::new();
        for (idx, conn) in self.connections.iter().enumerate() {
            let fc = Arc::new(FlowConnection::new(format!("conn-{}", idx), conn.config));
            flow_connections.push((conn.source_node, conn.relationship, conn.dest_node, fc));
        }

        // Create metrics per processor.
        let mut processor_infos: Vec<ProcessorInfo> = Vec::new();
        let mut metrics_by_node: HashMap<NodeId, Arc<ProcessorMetrics>> = HashMap::new();

        for node_builder in &self.nodes {
            let metrics = Arc::new(ProcessorMetrics::new());
            metrics_by_node.insert(node_builder.id, metrics.clone());
            processor_infos.push(ProcessorInfo {
                name: node_builder.name.clone(),
                type_name: node_builder.type_name.clone(),
                scheduling: node_builder.scheduling.clone(),
                metrics,
            });
        }

        // Build ConnectionInfo for the handle.
        // We need a name-lookup map for source/dest node IDs.
        let node_names: HashMap<NodeId, String> =
            self.nodes.iter().map(|n| (n.id, n.name.clone())).collect();

        let connection_infos: Vec<ConnectionInfo> = flow_connections
            .iter()
            .map(|(src, rel, dst, fc)| ConnectionInfo {
                id: fc.id.clone(),
                source_name: node_names.get(src).cloned().unwrap_or_default(),
                relationship: rel.to_string(),
                dest_name: node_names.get(dst).cloned().unwrap_or_default(),
                connection: fc.clone(),
            })
            .collect();

        // Build ProcessorNodes.
        let mut processor_nodes: Vec<ProcessorNode> = Vec::new();
        for node_builder in &mut self.nodes {
            let processor = node_builder
                .processor
                .take()
                .ok_or(RuniFiError::EngineAlreadyRunning)?;

            let child_token = self.cancel_token.child_token();
            let metrics = metrics_by_node
                .get(&node_builder.id)
                .expect("metrics must exist")
                .clone();

            let mut pn = ProcessorNode::new(
                node_builder.name.clone(),
                format!("node-{}", node_builder.id),
                processor,
                node_builder.scheduling.clone(),
                node_builder.properties.clone(),
                self.content_repo.clone(),
                self.id_gen.clone(),
                child_token,
                metrics,
            );

            // Wire connections.
            for (src, rel, dst, fc) in &flow_connections {
                if *src == node_builder.id {
                    // Find the relationship object from the processor.
                    let relationships = pn.relationships();
                    if let Some(relationship) = relationships.into_iter().find(|r| r.name == *rel) {
                        pn.add_output(relationship, fc.clone());
                    }
                }
                if *dst == node_builder.id {
                    pn.add_input(fc.clone());
                }
            }

            processor_nodes.push(pn);
        }

        // Build the EngineHandle before spawning tasks.
        let engine_handle = EngineHandle {
            flow_name: self.flow_name.clone(),
            started_at: Instant::now(),
            processors: Arc::new(processor_infos),
            connections: Arc::new(connection_infos),
            plugin_types: Arc::new(Vec::new()), // populated by server after start
            content_repo: self.content_repo.clone(),
        };
        self.handle = Some(engine_handle);

        // Spawn processor tasks.
        for node in processor_nodes {
            let handle = tokio::spawn(node.run());
            self.task_handles.push(handle);
        }

        // Spawn a dedicated metrics tick task that updates rolling windows once per second.
        {
            let all_metrics: Vec<Arc<ProcessorMetrics>> =
                metrics_by_node.values().cloned().collect();
            let tick_token = self.cancel_token.child_token();
            let tick_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = tick_token.cancelled() => break,
                        _ = interval.tick() => {
                            for m in &all_metrics {
                                m.record_tick();
                            }
                        }
                    }
                }
            });
            self.task_handles.push(tick_handle);
        }

        self.running = true;
        tracing::info!(
            node_count = self.nodes.len(),
            connection_count = self.connections.len(),
            "Flow engine started"
        );

        Ok(())
    }

    /// Stop the engine gracefully — cancels all tasks and waits for them to complete.
    pub async fn stop(&mut self) {
        if !self.running {
            return;
        }

        tracing::info!("Stopping flow engine...");
        self.cancel_token.cancel();

        for handle in self.task_handles.drain(..) {
            let _ = handle.await;
        }

        self.running = false;
        tracing::info!("Flow engine stopped");
    }

    /// Check if the engine is running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Get the engine handle for API access. Available after `start()`.
    pub fn handle(&self) -> Option<&EngineHandle> {
        self.handle.as_ref()
    }

    /// Set the plugin types on the engine handle (called by server after discovery).
    pub fn set_plugin_types(&mut self, plugin_types: Vec<PluginTypeInfo>) {
        if let Some(handle) = &mut self.handle {
            handle.plugin_types = Arc::new(plugin_types);
        }
    }
}
