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
use super::mutation::MutationCommand;
use super::mutation_handler::DefaultMutationHandler;
use super::persistence::FlowPersistence;
use super::processor_node::{
    ProcessorNode, SchedulingStrategy, SharedInputConnections, SharedInputNotifiers,
    SharedOutputConnections,
};
use crate::audit::{AuditAction, AuditEvent, AuditLogger, AuditTarget, NullAuditLogger};
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::flow_connection::{FlowConnection, QueuePriority};
use crate::connection::query::FlowConnectionQuery;
use crate::error::{Result, RuniFiError};
use crate::id::IdGenerator;
use crate::registry::plugin_registry::PluginRegistry;
use crate::registry::service_registry::SharedServiceRegistry;
use crate::repository::content_repo::ContentRepository;
use crate::repository::flowfile_repo::FlowFileRepository;
use crate::repository::provenance_repo::{
    InMemoryProvenanceRepository, SharedProvenanceRepository,
};
use crate::repository::state_provider::SharedLocalStateProvider;

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
    flowfile_repo: Arc<dyn FlowFileRepository>,
    persistence: Option<FlowPersistence>,
    audit_logger: Arc<dyn AuditLogger>,
    service_registry: SharedServiceRegistry,
    provenance_repo: SharedProvenanceRepository,
    state_provider: Option<SharedLocalStateProvider>,

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
    expiration: Option<Duration>,
    priority: QueuePriority,
    load_balance: Option<crate::cluster::load_balance::LoadBalanceConfig>,
}

impl FlowEngine {
    pub fn new(
        flow_name: impl Into<String>,
        content_repo: Arc<dyn ContentRepository>,
        flowfile_repo: Arc<dyn FlowFileRepository>,
    ) -> Self {
        Self {
            flow_name: flow_name.into(),
            content_repo,
            id_gen: Arc::new(IdGenerator::new()),
            cancel_token: CancellationToken::new(),
            bulletin_board: Arc::new(BulletinBoard::default()),
            registry: None,
            flowfile_repo,
            persistence: None,
            audit_logger: Arc::new(NullAuditLogger),
            service_registry: SharedServiceRegistry::new(),
            provenance_repo: Arc::new(InMemoryProvenanceRepository::new()),
            state_provider: None,
            nodes: Vec::new(),
            connections: Vec::new(),
            next_node_id: 0,
            next_conn_id: 0,
            task_handles: Vec::new(),
            running: false,
            handle: None,
        }
    }

    /// Set the audit logger for compliance event tracking.
    pub fn set_audit_logger(&mut self, logger: Arc<dyn AuditLogger>) {
        self.audit_logger = logger;
    }

    /// Provide a plugin registry for hot-add type validation.
    pub fn set_registry(&mut self, registry: Arc<PluginRegistry>) {
        self.registry = Some(registry);
    }

    /// Set the flow persistence layer for runtime state saving.
    pub fn set_persistence(&mut self, persistence: FlowPersistence) {
        self.persistence = Some(persistence);
    }

    /// Set the provenance repository for FlowFile lineage tracking.
    pub fn set_provenance_repo(&mut self, repo: SharedProvenanceRepository) {
        self.provenance_repo = repo;
    }

    /// Set the local state provider for processor state persistence.
    pub fn set_state_provider(&mut self, provider: SharedLocalStateProvider) {
        self.state_provider = Some(provider);
    }

    /// Get a reference to the provenance repository.
    pub fn provenance_repo(&self) -> &SharedProvenanceRepository {
        &self.provenance_repo
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
        self.connect_with_options(
            source_id,
            relationship,
            dest_id,
            config,
            None,
            QueuePriority::Fifo,
        )
    }

    /// Connect two processors with full options (expiration, priority).
    pub fn connect_with_options(
        &mut self,
        source_id: NodeId,
        relationship: &'static str,
        dest_id: NodeId,
        config: BackPressureConfig,
        expiration: Option<Duration>,
        priority: QueuePriority,
    ) -> ConnId {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections.push(ConnBuilder {
            _id: id,
            source_node: source_id,
            relationship,
            dest_node: dest_id,
            config,
            expiration,
            priority,
            load_balance: None,
        });
        id
    }

    /// Connect two processors with load balance configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_with_load_balance(
        &mut self,
        source_id: NodeId,
        relationship: &'static str,
        dest_id: NodeId,
        config: BackPressureConfig,
        expiration: Option<Duration>,
        priority: QueuePriority,
        load_balance: Option<crate::cluster::load_balance::LoadBalanceConfig>,
    ) -> ConnId {
        let id = self.next_conn_id;
        self.next_conn_id += 1;
        self.connections.push(ConnBuilder {
            _id: id,
            source_node: source_id,
            relationship,
            dest_node: dest_id,
            config,
            expiration,
            priority,
            load_balance,
        });
        id
    }

    /// Start the engine - validates the DAG and spawns a task per processor.
    pub async fn start(&mut self) -> Result<()> {
        if self.running {
            return Err(RuniFiError::EngineAlreadyRunning);
        }

        // ── WAL recovery ────────────────────────────────────────────────
        let recovery = self.flowfile_repo.recover().map_err(|e| {
            RuniFiError::Config(format!("FlowFile repository recovery failed: {e}"))
        })?;

        if recovery.max_id > 0 {
            self.id_gen.reset_to(recovery.max_id + 1);
            tracing::info!(
                max_id = recovery.max_id,
                recovered_queues = recovery.queued.len(),
                recovered_flowfiles = recovery.queued.values().map(|v| v.len()).sum::<usize>(),
                "Recovered FlowFiles from WAL"
            );
        }

        // Build FlowConnections.
        let mut flow_connections: Vec<(usize, &'static str, usize, Arc<FlowConnection>)> =
            Vec::new();
        for (idx, conn) in self.connections.iter().enumerate() {
            let fc = if let Some(ref lb_config) = conn.load_balance {
                Arc::new(FlowConnection::with_load_balance(
                    format!("conn-{}", idx),
                    conn.config,
                    lb_config.clone(),
                ))
            } else {
                Arc::new(FlowConnection::with_options(
                    format!("conn-{}", idx),
                    conn.config,
                    conn.expiration,
                    conn.priority.clone(),
                ))
            };
            flow_connections.push((conn.source_node, conn.relationship, conn.dest_node, fc));
        }

        // Restore recovered FlowFiles to their connection queues.
        if !recovery.queued.is_empty() {
            for (queue_id, flowfiles) in &recovery.queued {
                let target_conn = flow_connections
                    .iter()
                    .find(|(_, _, _, fc)| fc.id == *queue_id);
                if let Some((_, _, _, fc)) = target_conn {
                    for ff in flowfiles {
                        if fc.try_send(ff.clone()).is_err() {
                            tracing::warn!(
                                queue_id = %queue_id,
                                flowfile_id = ff.id,
                                "Failed to restore FlowFile — connection full"
                            );
                        }
                    }
                } else {
                    tracing::warn!(
                        queue_id = %queue_id,
                        count = flowfiles.len(),
                        "No matching connection for recovered FlowFiles — discarded"
                    );
                }
            }
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
            .map(|(src, rel, dst, fc)| {
                let src_name = node_names.get(src).cloned().unwrap_or_default();
                let dst_name = node_names.get(dst).cloned().unwrap_or_default();
                let rel_str = rel.to_string();
                let query = Arc::new(FlowConnectionQuery::new(
                    src_name.clone(),
                    rel_str.clone(),
                    dst_name.clone(),
                    fc.clone(),
                ));
                ConnectionInfo {
                    id: fc.id.clone(),
                    source_name: src_name,
                    relationship: rel_str,
                    dest_name: dst_name,
                    connection: query,
                }
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
                self.flowfile_repo.clone(),
            );
            pn.set_service_registry(self.service_registry.clone());
            pn.set_provenance_repo(self.provenance_repo.clone());
            pn.set_type_name(node_builder.type_name.clone());

            // Set sensitive property names for bulletin redaction.
            if let Some((descriptors, _)) = static_meta_by_node.get(&node_builder.id) {
                let sensitive_names: Vec<String> = descriptors
                    .iter()
                    .filter(|d| d.sensitive)
                    .map(|d| d.name.clone())
                    .collect();
                if !sensitive_names.is_empty() {
                    pn.set_sensitive_property_names(sensitive_names);
                }
            }
            if let Some(ref provider) = self.state_provider {
                pn.set_state_provider(provider.clone());
            }

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
                scheduling_display: scheduling_display(&node_builder.scheduling),
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

        // Shared position store.
        let positions = Arc::new(DashMap::new());

        // Shared label store.
        let labels = Arc::new(RwLock::new(Vec::new()));

        // Shared process group store.
        let process_groups = Arc::new(RwLock::new(Vec::new()));

        // Build the EngineHandle.
        let engine_handle = EngineHandle {
            flow_name: self.flow_name.clone(),
            started_at: Instant::now(),
            processors: live_procs.clone(),
            connections: live_conns.clone(),
            plugin_types: Arc::new(Vec::new()),
            bulletin_board: self.bulletin_board.clone(),
            content_repo: self.content_repo.clone(),
            positions: positions.clone(),
            audit_logger: self.audit_logger.clone(),
            service_registry: self.service_registry.clone(),
            labels: labels.clone(),
            process_groups: process_groups.clone(),
            mutation_tx,
            persistence: self.persistence.clone(),
            provenance_repo: self.provenance_repo.clone(),
            state_provider: self.state_provider.clone(),
        };

        // Wire persistence: pass only the data collections it needs for
        // snapshotting (not the full EngineHandle) to break the Arc cycle.
        if let Some(ref persistence) = self.persistence {
            persistence.set_source(
                self.flow_name.clone(),
                live_procs.clone(),
                live_conns.clone(),
                positions,
                self.service_registry.clone(),
                labels,
                process_groups,
            );
            let persist_token = self.cancel_token.child_token();
            let persist_clone = persistence.clone();
            let persist_handle = tokio::spawn(async move {
                persist_clone.run(persist_token).await;
            });
            self.task_handles.push(persist_handle);
        }

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

        // Spawn the WAL checkpoint task.
        {
            let ckpt_token = self.cancel_token.child_token();
            let ckpt_repo = self.flowfile_repo.clone();
            let ckpt_handle = tokio::spawn(async move {
                // Default to 120s if the repo doesn't expose an interval.
                let interval_secs = 120u64;
                let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
                // Skip the first immediate tick.
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = ckpt_token.cancelled() => break,
                        _ = interval.tick() => {
                            if let Err(e) = ckpt_repo.checkpoint() {
                                tracing::error!(error = %e, "WAL checkpoint failed");
                            } else {
                                tracing::debug!("WAL checkpoint completed");
                            }
                        }
                    }
                }
            });
            self.task_handles.push(ckpt_handle);
        }

        // Spawn the FlowFile expiration background task.
        // Checks all connections with configured expiration and drops expired FlowFiles.
        {
            let expire_token = self.cancel_token.child_token();
            let expire_conns: Vec<Arc<FlowConnection>> = flow_connections
                .iter()
                .filter(|(_, _, _, fc)| fc.expiration().is_some())
                .map(|(_, _, _, fc)| fc.clone())
                .collect();

            if !expire_conns.is_empty() {
                let num_expire_conns = expire_conns.len();
                let expire_handle = tokio::spawn(async move {
                    // Check every 5 seconds.
                    let mut interval = tokio::time::interval(Duration::from_secs(5));
                    // Skip the first immediate tick.
                    interval.tick().await;
                    loop {
                        tokio::select! {
                            _ = expire_token.cancelled() => break,
                            _ = interval.tick() => {
                                let now_nanos = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as u64;
                                for conn in &expire_conns {
                                    let expired = conn.expire_flowfiles(now_nanos);
                                    if !expired.is_empty() {
                                        tracing::info!(
                                            connection_id = %conn.id,
                                            expired_count = expired.len(),
                                            "Expired FlowFiles removed from queue"
                                        );
                                    }
                                }
                            }
                        }
                    }
                });
                self.task_handles.push(expire_handle);
                tracing::info!(
                    connections_with_expiration = num_expire_conns,
                    "FlowFile expiration task started"
                );
            }
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

            let flowfile_repo_mutation = self.flowfile_repo.clone();
            let audit_logger = self.audit_logger.clone();
            let provenance_repo_mutation = self.provenance_repo.clone();
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
                flowfile_repo_mutation,
                audit_logger,
                provenance_repo_mutation,
            ));
            self.task_handles.push(mutation_handle);
        }

        self.running = true;
        tracing::info!(
            node_count = self.nodes.len(),
            connection_count = self.connections.len(),
            "Flow engine started"
        );

        self.audit_logger.log(&AuditEvent::success_with_details(
            AuditAction::EngineStarted,
            AuditTarget::system(),
            format!(
                "nodes={}, connections={}",
                self.nodes.len(),
                self.connections.len()
            ),
        ));

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

        // Best-effort final checkpoint before shutdown.
        if let Err(e) = self.flowfile_repo.checkpoint() {
            tracing::error!(error = %e, "Final WAL checkpoint failed");
        }
        self.flowfile_repo.shutdown();

        self.running = false;
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::EngineShutdown,
            AuditTarget::system(),
        ));
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

    /// Get a reference to the shared service registry for pre-start configuration.
    pub fn service_registry(&self) -> &SharedServiceRegistry {
        &self.service_registry
    }

    /// Set the plugin types on the engine handle (called by server after discovery).
    pub fn set_plugin_types(&mut self, plugin_types: Vec<PluginTypeInfo>) {
        if let Some(handle) = &mut self.handle {
            handle.plugin_types = Arc::new(plugin_types);
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Format a `SchedulingStrategy` as a human-readable display string.
///
/// Keeps the concrete enum internal to the engine — callers receive a `String`
/// so they do not need to import or match on `SchedulingStrategy`.
pub(crate) fn scheduling_display(strategy: &SchedulingStrategy) -> String {
    match strategy {
        SchedulingStrategy::TimerDriven { interval_ms } => {
            format!("timer-driven ({}ms)", interval_ms)
        }
        SchedulingStrategy::EventDriven => "event-driven".to_string(),
        SchedulingStrategy::CronDriven { expression } => {
            format!("cron-driven ({})", expression)
        }
    }
}

// ── Mutation handler ──────────────────────────────────────────────────────────

/// Drive the mutation command loop, delegating each command to `DefaultMutationHandler`.
///
/// Topology mutations are kept sequential (one command at a time) by this loop
/// so that `DefaultMutationHandler` methods never run concurrently.
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
    flowfile_repo: Arc<dyn FlowFileRepository>,
    audit_logger: Arc<dyn AuditLogger>,
    provenance_repo: SharedProvenanceRepository,
) {
    let mut handler = DefaultMutationHandler {
        live_procs,
        live_conns,
        content_repo,
        id_gen,
        bulletin_board,
        registry,
        parent_cancel,
        proc_tokens,
        runtime_conn_id: 0,
        flowfile_repo,
        audit_logger,
        provenance_repo,
    };

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
                        scheduling_strategy, interval_ms, cron_expression, reply,
                    } => {
                        let result = handler.handle_add_processor(
                            &name, &type_name, properties,
                            &scheduling_strategy, interval_ms,
                            cron_expression.as_deref(),
                        );
                        let _ = reply.send(result);
                    }

                    MutationCommand::RemoveProcessor { name, reply } => {
                        let result = handler.handle_remove_processor(&name).await;
                        let _ = reply.send(result);
                    }

                    MutationCommand::AddConnection {
                        source_name, relationship, dest_name, config, load_balance, reply,
                    } => {
                        handler.runtime_conn_id += 1;
                        let conn_id = format!("runtime-conn-{}", handler.runtime_conn_id);
                        let result = handler.handle_add_connection(
                            conn_id, &source_name, &relationship, &dest_name, config, load_balance,
                        );
                        let _ = reply.send(result);
                    }

                    MutationCommand::RemoveConnection { id, force, reply } => {
                        let result = handler.handle_remove_connection(&id, force);
                        let _ = reply.send(result);
                    }
                }
            }
        }
    }
}
