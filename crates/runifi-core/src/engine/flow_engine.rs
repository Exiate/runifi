use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::Processor;

use super::bulletin::BulletinBoard;
use super::handle::{
    ConnectionInfo, EngineHandle, PluginTypeInfo, ProcessorInfo, PropertyDescriptorInfo,
    RelationshipInfo,
};
use super::metrics::ProcessorMetrics;
use super::mutation::{MutationCommand, MutationError};
use super::processor_node::{
    ProcessorNode, SchedulingStrategy, SharedInputConnections, SharedInputNotifiers,
    SharedOutputConnections,
};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::flow_connection::FlowConnection;
use crate::error::{Result, RuniFiError};
use crate::id::IdGenerator;
use crate::registry::plugin_registry::PluginRegistry;
use crate::repository::content_repo::ContentRepository;

/// A unique identifier for a processor node in the engine.
pub type NodeId = usize;

/// A unique identifier for a connection in the engine.
pub type ConnId = usize;

/// The core flow engine - orchestrates a DAG of processors and connections.
///
/// While running, topology mutations are serialised through a `tokio::mpsc`
/// command channel drained by the `mutation_handler` task spawned during
/// `start()`. This keeps all topology writes sequential.
pub struct FlowEngine {
    flow_name: String,
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    cancel_token: CancellationToken,
    bulletin_board: Arc<BulletinBoard>,
    registry: Option<Arc<PluginRegistry>>,

    nodes: Vec<NodeBuilder>,
    connections: Vec<ConnBuilder>,
    next_node_id: NodeId,
    next_conn_id: ConnId,

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
            bulletin_board: Arc::new(BulletinBoard::default()),
            registry: None,
            nodes: Vec::new(),
            connections: Vec::new(),
            next_node_id: 0,
            next_conn_id: 0,
            task_handles: Vec::new(),
            running: false,
            handle: None,
        }
    }

    /// Provide a plugin registry for hot-add type validation.
    pub fn set_registry(&mut self, registry: Arc<PluginRegistry>) {
        self.registry = Some(registry);
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

    /// Start the engine - validates the DAG and spawns a task per processor.
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

        // First pass: collect static processor metadata (property descriptors and
        // relationships) and allocate per-processor state. Must happen before the
        // processor is consumed by ProcessorNode::new() below.
        let mut metrics_by_node: HashMap<NodeId, Arc<ProcessorMetrics>> = HashMap::new();
        let mut shared_props_by_node: HashMap<NodeId, Arc<RwLock<HashMap<String, String>>>> =
            HashMap::new();
        let mut static_meta_by_node: HashMap<
            NodeId,
            (Vec<PropertyDescriptorInfo>, Vec<RelationshipInfo>),
        > = HashMap::new();

        for node_builder in &self.nodes {
            let metrics = Arc::new(ProcessorMetrics::new());
            metrics_by_node.insert(node_builder.id, metrics.clone());
            let shared_props = Arc::new(RwLock::new(node_builder.properties.clone()));
            shared_props_by_node.insert(node_builder.id, shared_props);

            let (prop_descriptors, rels) = if let Some(ref proc) = node_builder.processor {
                let pds = proc
                    .property_descriptors()
                    .into_iter()
                    .map(|pd| PropertyDescriptorInfo {
                        name: pd.name.to_string(),
                        description: pd.description.to_string(),
                        required: pd.required,
                        default_value: pd.default_value.map(|v| v.to_string()),
                        sensitive: pd.sensitive,
                        allowed_values: pd
                            .allowed_values
                            .map(|av| av.iter().map(|v| v.to_string()).collect()),
                    })
                    .collect();
                let rs = proc
                    .relationships()
                    .into_iter()
                    .map(|r| RelationshipInfo {
                        name: r.name.to_string(),
                        description: r.description.to_string(),
                        auto_terminated: r.auto_terminated,
                    })
                    .collect();
                (pds, rs)
            } else {
                (Vec::new(), Vec::new())
            };
            static_meta_by_node.insert(node_builder.id, (prop_descriptors, rels));
        }

        // Build ConnectionInfo for the handle.
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

        // Build ProcessorNodes and collect shared handles for live_procs patching.
        let mut processor_nodes: Vec<ProcessorNode> = Vec::new();
        let mut proc_tokens: HashMap<String, CancellationToken> = HashMap::new();
        // Map processor name -> (input_connections, output_connections, input_notifiers)
        let mut node_conn_handles: HashMap<
            String,
            (
                SharedInputConnections,
                SharedOutputConnections,
                SharedInputNotifiers,
            ),
        > = HashMap::new();

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

            let shared_props = shared_props_by_node
                .get(&node_builder.id)
                .expect("shared_props must exist")
                .clone();

            let mut pn = ProcessorNode::new(
                node_builder.name.clone(),
                format!("node-{}", node_builder.id),
                processor,
                node_builder.scheduling.clone(),
                shared_props,
                self.content_repo.clone(),
                self.id_gen.clone(),
                child_token.clone(),
                metrics,
                self.bulletin_board.clone(),
            );

            for (src, rel, dst, fc) in &flow_connections {
                if *src == node_builder.id {
                    let relationships = pn.relationships();
                    if let Some(relationship) = relationships.into_iter().find(|r| r.name == *rel) {
                        pn.add_output(relationship, fc.clone());
                    }
                }
                if *dst == node_builder.id {
                    pn.add_input(fc.clone());
                }
            }

            // Grab the shared handles BEFORE moving pn into spawn.
            let input_h = pn.input_connections_handle();
            let output_h = pn.output_connections_handle();
            let notifiers_h = pn.input_notifiers_handle();
            node_conn_handles.insert(node_builder.name.clone(), (input_h, output_h, notifiers_h));

            proc_tokens.insert(node_builder.name.clone(), child_token);
            processor_nodes.push(pn);
        }

        // Build ProcessorInfo entries combining static metadata (from the first
        // pass) with the shared connection handles (from the second pass).
        let mut processor_infos: Vec<ProcessorInfo> = Vec::new();
        for node_builder in &self.nodes {
            let metrics = metrics_by_node
                .get(&node_builder.id)
                .expect("metrics must exist")
                .clone();
            let shared_props = shared_props_by_node
                .get(&node_builder.id)
                .expect("shared_props must exist")
                .clone();
            let (prop_descriptors, relationships) = static_meta_by_node
                .remove(&node_builder.id)
                .expect("static meta must exist for every node");
            let (input_h, output_h, notifiers_h) = node_conn_handles
                .get(&node_builder.name)
                .expect("conn handles must exist for every node")
                .clone();

            processor_infos.push(ProcessorInfo {
                name: node_builder.name.clone(),
                type_name: node_builder.type_name.clone(),
                scheduling: node_builder.scheduling.clone(),
                metrics,
                property_descriptors: prop_descriptors,
                relationships,
                properties: shared_props,
                input_connections: input_h,
                output_connections: output_h,
                input_notifiers: notifiers_h,
            });
        }

        // Wrap in shared mutable arcs visible from the handle.
        let live_procs = Arc::new(RwLock::new(processor_infos));
        let live_conns = Arc::new(RwLock::new(connection_infos));

        // Create the mutation command channel.
        let (mutation_tx, mutation_rx) = mpsc::channel::<MutationCommand>(64);

        // Build the EngineHandle.
        let engine_handle = EngineHandle {
            flow_name: self.flow_name.clone(),
            started_at: Instant::now(),
            processors: live_procs.clone(),
            connections: live_conns.clone(),
            plugin_types: Arc::new(Vec::new()),
            bulletin_board: self.bulletin_board.clone(),
            content_repo: self.content_repo.clone(),
            positions: Arc::new(DashMap::new()),
            mutation_tx,
        };
        self.handle = Some(engine_handle);

        // Spawn processor tasks.
        for node in processor_nodes {
            let handle = tokio::spawn(node.run());
            self.task_handles.push(handle);
        }

        // Spawn the metrics tick task.
        {
            let original_metrics: Vec<Arc<ProcessorMetrics>> =
                metrics_by_node.values().cloned().collect();
            let tick_token = self.cancel_token.child_token();
            let live_procs_tick = live_procs.clone();
            let tick_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = tick_token.cancelled() => break,
                        _ = interval.tick() => {
                            for m in &original_metrics {
                                m.record_tick();
                            }
                            let procs = live_procs_tick.read();
                            for p in procs.iter() {
                                let is_original = original_metrics
                                    .iter()
                                    .any(|m| Arc::ptr_eq(m, &p.metrics));
                                if !is_original {
                                    p.metrics.record_tick();
                                }
                            }
                        }
                    }
                }
            });
            self.task_handles.push(tick_handle);
        }

        // Spawn the mutation handler task.
        {
            let mutation_cancel = self.cancel_token.child_token();
            let content_repo = self.content_repo.clone();
            let id_gen = self.id_gen.clone();
            let bulletin_board = self.bulletin_board.clone();
            let registry = self.registry.clone();
            let parent_cancel = self.cancel_token.clone();

            let shared_tokens: Arc<parking_lot::Mutex<HashMap<String, CancellationToken>>> =
                Arc::new(parking_lot::Mutex::new(proc_tokens));

            let mutation_handle = tokio::spawn(run_mutation_handler(
                mutation_rx,
                mutation_cancel,
                live_procs,
                live_conns,
                content_repo,
                id_gen,
                bulletin_board,
                registry,
                parent_cancel,
                shared_tokens,
            ));
            self.task_handles.push(mutation_handle);
        }

        self.running = true;
        tracing::info!(
            node_count = self.nodes.len(),
            connection_count = self.connections.len(),
            "Flow engine started"
        );

        Ok(())
    }

    /// Stop the engine gracefully.
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

// ── Mutation handler ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_mutation_handler(
    mut rx: mpsc::Receiver<MutationCommand>,
    cancel: CancellationToken,
    live_procs: Arc<RwLock<Vec<ProcessorInfo>>>,
    live_conns: Arc<RwLock<Vec<ConnectionInfo>>>,
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    bulletin_board: Arc<BulletinBoard>,
    registry: Option<Arc<PluginRegistry>>,
    parent_cancel: CancellationToken,
    proc_tokens: Arc<parking_lot::Mutex<HashMap<String, CancellationToken>>>,
) {
    let mut runtime_conn_id: usize = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("Mutation handler cancelled");
                break;
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::debug!("Mutation channel closed");
                    break;
                };

                match cmd {
                    MutationCommand::AddProcessor {
                        name, type_name, properties,
                        scheduling_strategy, interval_ms, reply,
                    } => {
                        let result = handle_add_processor(
                            &name, &type_name, properties,
                            &scheduling_strategy, interval_ms,
                            &live_procs, &content_repo, &id_gen,
                            &bulletin_board, &registry,
                            &parent_cancel, &proc_tokens,
                        );
                        let _ = reply.send(result);
                    }

                    MutationCommand::RemoveProcessor { name, reply } => {
                        let result = handle_remove_processor(
                            &name, &live_procs, &live_conns, &proc_tokens,
                        ).await;
                        let _ = reply.send(result);
                    }

                    MutationCommand::AddConnection {
                        source_name, relationship, dest_name, config, reply,
                    } => {
                        runtime_conn_id += 1;
                        let conn_id = format!("runtime-conn-{}", runtime_conn_id);
                        let result = handle_add_connection(
                            conn_id, &source_name, &relationship, &dest_name,
                            config, &live_procs, &live_conns,
                        );
                        let _ = reply.send(result);
                    }

                    MutationCommand::RemoveConnection { id, force, reply } => {
                        let result = handle_remove_connection(&id, force, &live_conns);
                        let _ = reply.send(result);
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_add_processor(
    name: &str,
    type_name: &str,
    properties: HashMap<String, String>,
    scheduling_strategy: &str,
    interval_ms: u64,
    live_procs: &Arc<RwLock<Vec<ProcessorInfo>>>,
    content_repo: &Arc<dyn ContentRepository>,
    id_gen: &Arc<IdGenerator>,
    bulletin_board: &Arc<BulletinBoard>,
    registry: &Option<Arc<PluginRegistry>>,
    parent_cancel: &CancellationToken,
    proc_tokens: &Arc<parking_lot::Mutex<HashMap<String, CancellationToken>>>,
) -> std::result::Result<(), MutationError> {
    // Fix 5: validate scheduling_strategy strictly.
    if scheduling_strategy != "timer" && scheduling_strategy != "event" {
        return Err(MutationError::InvalidSchedulingStrategy(
            scheduling_strategy.to_string(),
        ));
    }

    let reg = registry
        .as_ref()
        .ok_or_else(|| MutationError::Internal("No plugin registry available".into()))?;

    let processor: Box<dyn Processor> = reg
        .create_processor(type_name)
        .ok_or_else(|| MutationError::UnknownType(type_name.to_string()))?;

    if live_procs.read().iter().any(|p| p.name == name) {
        return Err(MutationError::DuplicateName(name.to_string()));
    }

    let scheduling = if scheduling_strategy == "event" {
        SchedulingStrategy::EventDriven
    } else {
        SchedulingStrategy::TimerDriven { interval_ms }
    };

    let metrics = Arc::new(ProcessorMetrics::new());
    metrics
        .enabled
        .store(false, std::sync::atomic::Ordering::Relaxed);

    let prop_descriptors: Vec<PropertyDescriptorInfo> = processor
        .property_descriptors()
        .into_iter()
        .map(|pd| PropertyDescriptorInfo {
            name: pd.name.to_string(),
            description: pd.description.to_string(),
            required: pd.required,
            default_value: pd.default_value.map(|v| v.to_string()),
            sensitive: pd.sensitive,
            allowed_values: pd
                .allowed_values
                .map(|av| av.iter().map(|v| v.to_string()).collect()),
        })
        .collect();

    let relationships: Vec<RelationshipInfo> = processor
        .relationships()
        .into_iter()
        .map(|r| RelationshipInfo {
            name: r.name.to_string(),
            description: r.description.to_string(),
            auto_terminated: r.auto_terminated,
        })
        .collect();

    let shared_props = Arc::new(RwLock::new(properties));
    let child_token = parent_cancel.child_token();

    let pn = ProcessorNode::new(
        name.to_string(),
        format!("runtime-{}", name),
        processor,
        scheduling.clone(),
        shared_props.clone(),
        content_repo.clone(),
        id_gen.clone(),
        child_token.clone(),
        metrics.clone(),
        bulletin_board.clone(),
    );

    // Capture shared handles before moving pn into spawn.
    let input_h = pn.input_connections_handle();
    let output_h = pn.output_connections_handle();
    let notifiers_h = pn.input_notifiers_handle();

    proc_tokens.lock().insert(name.to_string(), child_token);
    tokio::spawn(pn.run());

    live_procs.write().push(ProcessorInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        scheduling,
        metrics,
        property_descriptors: prop_descriptors,
        relationships,
        properties: shared_props,
        input_connections: input_h,
        output_connections: output_h,
        input_notifiers: notifiers_h,
    });

    tracing::info!(name, type_name, "Hot-added processor");
    Ok(())
}

/// Remove a processor at runtime.
///
/// Fix 3 (TOCTOU): Acquire the write lock for the entire operation so that
/// concurrent lifecycle ops (start/stop) that check the same `live_procs` list
/// cannot race with removal. This means a concurrent `start_processor` reading
/// the processors list will either observe the processor as still present (before
/// the write lock is acquired here) or not at all (after retain completes).
async fn handle_remove_processor(
    name: &str,
    live_procs: &Arc<RwLock<Vec<ProcessorInfo>>>,
    live_conns: &Arc<RwLock<Vec<ConnectionInfo>>>,
    proc_tokens: &Arc<parking_lot::Mutex<HashMap<String, CancellationToken>>>,
) -> std::result::Result<(), MutationError> {
    // Check for active connections before acquiring the write lock.
    // This avoids holding the write lock while iterating connections.
    {
        let conns = live_conns.read();
        let conn_ids: Vec<String> = conns
            .iter()
            .filter(|c| c.source_name == name || c.dest_name == name)
            .map(|c| c.id.clone())
            .collect();
        if !conn_ids.is_empty() {
            return Err(MutationError::ProcessorHasConnections(conn_ids.join(", ")));
        }
    }

    // Acquire the write lock for the remainder of the operation. This serialises
    // removal with any concurrent lifecycle operation (start/stop/pause/resume)
    // that holds a read lock while setting atomics.
    let mut procs = live_procs.write();

    let info = procs
        .iter()
        .find(|p| p.name == name)
        .ok_or_else(|| MutationError::ProcessorNotFound(name.to_string()))?;

    let enabled = info
        .metrics
        .enabled
        .load(std::sync::atomic::Ordering::Relaxed);
    let active = info
        .metrics
        .active
        .load(std::sync::atomic::Ordering::Relaxed);
    if enabled || active {
        return Err(MutationError::ProcessorNotStopped);
    }

    if let Some(token) = proc_tokens.lock().remove(name) {
        token.cancel();
    }

    procs.retain(|p| p.name != name);

    tracing::info!(name, "Hot-removed processor");
    Ok(())
}

/// Add a connection at runtime and wire it into the processor data paths.
///
/// Fix 1 (phantom connections): in addition to updating `live_conns`, push the
/// new `FlowConnection` into:
///   - the source processor's `output_connections` (SharedOutputConnections), and
///   - the destination processor's `input_connections` (SharedInputConnections)
///     and `input_notifiers` (SharedInputNotifiers).
///
/// These shared `Arc<RwLock<Vec<...>>>` are held inside the running
/// `ProcessorNode` task, so writes here are immediately visible to the task's
/// next invocation without restarting it.
fn handle_add_connection(
    conn_id: String,
    source_name: &str,
    relationship: &str,
    dest_name: &str,
    config: BackPressureConfig,
    live_procs: &Arc<RwLock<Vec<ProcessorInfo>>>,
    live_conns: &Arc<RwLock<Vec<ConnectionInfo>>>,
) -> std::result::Result<String, MutationError> {
    // Validate processors/relationship and collect the shared handles we'll
    // mutate. Clone the Arcs so we can drop the read lock before writing.
    let (src_output_h, src_rel, dst_input_h, dst_notifiers_h) = {
        let procs = live_procs.read();

        let src = procs
            .iter()
            .find(|p| p.name == source_name)
            .ok_or_else(|| MutationError::ProcessorNotFound(source_name.to_string()))?;

        // Find the matching RelationshipInfo to reconstruct a Relationship value.
        let src_rel_info = src
            .relationships
            .iter()
            .find(|r| r.name == relationship)
            .ok_or_else(|| {
                MutationError::UnknownRelationship(
                    relationship.to_string(),
                    source_name.to_string(),
                )
            })?;

        let dst = procs
            .iter()
            .find(|p| p.name == dest_name)
            .ok_or_else(|| MutationError::ProcessorNotFound(dest_name.to_string()))?;

        // Relationship uses &'static str; we must leak the strings for runtime-
        // allocated names. These strings live for the lifetime of the process,
        // which is acceptable because connection/processor names are bounded by
        // the configured max (128 chars) and the total number of hot-added
        // connections is expected to be small.
        use runifi_plugin_api::relationship::Relationship;
        let rel = Relationship {
            name: Box::leak(src_rel_info.name.clone().into_boxed_str()),
            description: Box::leak(src_rel_info.description.clone().into_boxed_str()),
            auto_terminated: src_rel_info.auto_terminated,
        };

        (
            Arc::clone(&src.output_connections),
            rel,
            Arc::clone(&dst.input_connections),
            Arc::clone(&dst.input_notifiers),
        )
    };

    // Check for duplicate connection.
    {
        let conns = live_conns.read();
        if conns.iter().any(|c| {
            c.source_name == source_name
                && c.relationship == relationship
                && c.dest_name == dest_name
        }) {
            return Err(MutationError::DuplicateConnection(
                source_name.to_string(),
                relationship.to_string(),
                dest_name.to_string(),
            ));
        }
    }

    let fc = Arc::new(FlowConnection::new(conn_id.clone(), config));

    // Wire into destination processor's input list and notifier list.
    // The running ProcessorNode task holds the same Arc<RwLock<...>>, so writes
    // here are visible on its next `input_connections.read()` / `input_notifiers.read()`.
    let notifier = fc.notifier();
    dst_input_h.write().push(Arc::clone(&fc));
    dst_notifiers_h.write().push(notifier);

    // Wire into source processor's output list.
    src_output_h.write().push((src_rel, Arc::clone(&fc)));

    // Record in live_conns for API visibility.
    live_conns.write().push(ConnectionInfo {
        id: conn_id.clone(),
        source_name: source_name.to_string(),
        relationship: relationship.to_string(),
        dest_name: dest_name.to_string(),
        connection: fc,
    });

    tracing::info!(
        id = %conn_id,
        source = source_name,
        relationship,
        destination = dest_name,
        "Hot-added connection"
    );
    Ok(conn_id)
}

fn handle_remove_connection(
    id: &str,
    force: bool,
    live_conns: &Arc<RwLock<Vec<ConnectionInfo>>>,
) -> std::result::Result<(), MutationError> {
    let queue_count = {
        let conns = live_conns.read();
        let info = conns
            .iter()
            .find(|c| c.id == id)
            .ok_or_else(|| MutationError::ConnectionNotFound(id.to_string()))?;
        info.connection.count()
    };

    if queue_count > 0 && !force {
        return Err(MutationError::QueueNotEmpty(queue_count));
    }

    if queue_count > 0 {
        let conns = live_conns.read();
        if let Some(info) = conns.iter().find(|c| c.id == id) {
            let removed = info.connection.clear_queue();
            tracing::warn!(
                connection_id = %id,
                discarded = removed,
                "Force-removing connection with non-empty queue - FlowFiles discarded"
            );
        }
    }

    live_conns.write().retain(|c| c.id != id);

    tracing::info!(id, "Hot-removed connection");
    Ok(())
}
