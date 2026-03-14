use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::property::PropertyValue;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::service::ServiceLookup;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::state::StateManager;
use runifi_plugin_api::{FlowFile, Processor};

use super::bulletin::{BulletinBoard, BulletinSeverity};
use super::metrics::ProcessorMetrics;
use super::supervisor::{InvocationResult, ProcessorSupervisor};
use crate::cluster::load_balance::{LoadBalanceStrategy, LoadBalancer};
use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::registry::service_registry::{RegistryServiceLookup, SharedServiceRegistry};
use crate::repository::content_repo::ContentRepository;
use crate::repository::flowfile_repo::{FlowFileOp, FlowFileRepository};
use crate::repository::provenance_repo::SharedProvenanceRepository;
use crate::repository::state_provider::SharedLocalStateProvider;
use crate::session::process_session::CoreProcessSession;

/// Scheduling strategy for a processor node.
#[derive(Debug, Clone)]
pub enum SchedulingStrategy {
    /// Trigger on a fixed interval.
    TimerDriven { interval_ms: u64 },
    /// Trigger when input data is available.
    EventDriven,
    /// Trigger based on a CRON expression.
    CronDriven { expression: String },
}

/// Runtime context for a single processor instance.
struct NodeProcessContext {
    name: String,
    id: String,
    properties: HashMap<String, String>,
    yield_duration_ms: u64,
    service_lookup: Option<Box<dyn ServiceLookup>>,
    state_manager: Option<Box<dyn StateManager>>,
}

impl runifi_plugin_api::context::ProcessContext for NodeProcessContext {
    fn get_property(&self, name: &str) -> PropertyValue {
        match self.properties.get(name) {
            Some(v) => PropertyValue::String(v.clone()),
            None => PropertyValue::Unset,
        }
    }

    fn property_names(&self) -> Vec<String> {
        self.properties.keys().cloned().collect()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn yield_duration_ms(&self) -> u64 {
        self.yield_duration_ms
    }

    fn service_lookup(&self) -> Option<&dyn ServiceLookup> {
        self.service_lookup.as_deref()
    }

    fn state_manager(&self) -> Option<&dyn StateManager> {
        self.state_manager.as_deref()
    }
}

/// Shared, mutable list of input connections.
///
/// `parking_lot::RwLock` is used (not tokio) because the list is also accessed
/// from synchronous `on_trigger` contexts via `CoreProcessSession`. Cloning the
/// `Arc` gives the mutation handler a handle into the live task's data.
pub type SharedInputConnections = Arc<RwLock<Vec<Arc<FlowConnection>>>>;

/// Shared, mutable list of output connections per relationship.
pub type SharedOutputConnections = Arc<RwLock<Vec<(Relationship, Arc<FlowConnection>)>>>;

/// Shared, mutable list of input notifiers for event-driven wakeup.
///
/// The mutation handler pushes a notifier from each new input connection so
/// that `EventDriven` processors wake up when data arrives on hot-added inputs.
pub type SharedInputNotifiers = Arc<RwLock<Vec<Arc<Notify>>>>;

/// A runtime wrapper for a single processor instance.
///
/// Manages the processor's lifecycle, connections, scheduling, and fault isolation.
pub struct ProcessorNode {
    pub name: String,
    pub id: String,
    /// Processor type name (e.g. "GenerateFlowFile").
    pub type_name: String,
    pub scheduling: SchedulingStrategy,
    pub properties: Arc<RwLock<HashMap<String, String>>>,
    supervisor: ProcessorSupervisor,
    /// Shared so the mutation handler can wire hot-added connections to the
    /// running task without restarting it.
    input_connections: SharedInputConnections,
    /// Shared so the mutation handler can wire hot-added output connections.
    output_connections: SharedOutputConnections,
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    cancel_token: CancellationToken,
    /// Shared so the mutation handler can register notifiers from new inputs.
    input_notifiers: SharedInputNotifiers,
    metrics: Arc<ProcessorMetrics>,
    bulletin_board: Arc<BulletinBoard>,
    flowfile_repo: Arc<dyn FlowFileRepository>,
    service_registry: Option<SharedServiceRegistry>,
    provenance_repo: SharedProvenanceRepository,
    /// Local state provider for processor state persistence.
    state_provider: Option<SharedLocalStateProvider>,
    /// Penalty duration in milliseconds for penalized FlowFiles (default 30s).
    penalty_duration_ms: u64,
    /// Lazily initialized load balancer for distributing FlowFiles across
    /// multiple output connections on the same relationship.
    load_balancer: OnceLock<LoadBalancer>,
}

impl ProcessorNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        id: String,
        processor: Box<dyn Processor>,
        scheduling: SchedulingStrategy,
        properties: Arc<RwLock<HashMap<String, String>>>,
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        cancel_token: CancellationToken,
        metrics: Arc<ProcessorMetrics>,
        bulletin_board: Arc<BulletinBoard>,
        flowfile_repo: Arc<dyn FlowFileRepository>,
    ) -> Self {
        Self {
            name,
            id,
            type_name: String::new(),
            scheduling,
            properties,
            supervisor: ProcessorSupervisor::new(processor),
            input_connections: Arc::new(RwLock::new(Vec::new())),
            output_connections: Arc::new(RwLock::new(Vec::new())),
            content_repo,
            id_gen,
            cancel_token,
            input_notifiers: Arc::new(RwLock::new(Vec::new())),
            metrics,
            bulletin_board,
            flowfile_repo,
            service_registry: None,
            provenance_repo: Arc::new(crate::repository::provenance_repo::NullProvenanceRepository),
            state_provider: None,
            penalty_duration_ms: crate::session::process_session::DEFAULT_PENALTY_DURATION_MS,
            load_balancer: OnceLock::new(),
        }
    }

    /// Set the service registry for service lookup during processing.
    pub fn set_service_registry(&mut self, registry: SharedServiceRegistry) {
        self.service_registry = Some(registry);
    }

    /// Set the provenance repository for lineage tracking.
    pub fn set_provenance_repo(&mut self, repo: SharedProvenanceRepository) {
        self.provenance_repo = repo;
    }

    /// Set the processor type name for provenance events.
    pub fn set_type_name(&mut self, type_name: String) {
        self.type_name = type_name;
    }

    /// Set the local state provider for processor state persistence.
    pub fn set_state_provider(&mut self, provider: SharedLocalStateProvider) {
        self.state_provider = Some(provider);
    }

    /// Add an input connection and wire its notifier for event-driven wakeup.
    pub fn add_input(&mut self, connection: Arc<FlowConnection>) {
        let notifier = connection.notifier();
        self.input_notifiers.write().push(notifier);
        self.input_connections.write().push(connection);
    }

    /// Add an output connection for a specific relationship.
    pub fn add_output(&mut self, relationship: Relationship, connection: Arc<FlowConnection>) {
        self.output_connections
            .write()
            .push((relationship, connection));
    }

    /// Get the processor's supported relationships.
    pub fn relationships(&self) -> Vec<Relationship> {
        self.supervisor.relationships()
    }

    /// Check if the circuit breaker is open.
    pub fn is_circuit_open(&self) -> bool {
        self.supervisor.is_circuit_open()
    }

    /// Return a clone of the shared input connection list handle.
    ///
    /// The mutation handler stores this so it can wire hot-added connections
    /// into the running processor task after `tokio::spawn(node.run())`.
    pub fn input_connections_handle(&self) -> SharedInputConnections {
        Arc::clone(&self.input_connections)
    }

    /// Return a clone of the shared output connection list handle.
    pub fn output_connections_handle(&self) -> SharedOutputConnections {
        Arc::clone(&self.output_connections)
    }

    /// Return a clone of the shared input notifiers handle.
    ///
    /// The mutation handler pushes notifiers from new input connections so that
    /// `EventDriven` processors wake up when data arrives on hot-added queues.
    pub fn input_notifiers_handle(&self) -> SharedInputNotifiers {
        Arc::clone(&self.input_notifiers)
    }

    /// Run the processor lifecycle until the cancellation token fires.
    ///
    /// This is spawned as a tokio task by the engine.
    /// The outer loop handles stop/start transitions: when `enabled` is set to
    /// false, the inner processing loop breaks cleanly, and the task waits
    /// until `enabled` is set back to true (or the cancellation token fires).
    pub async fn run(mut self) {
        // Last context, kept for on_stopped after lifecycle loop exits.
        let make_service_lookup = || -> Option<Box<dyn ServiceLookup>> {
            self.service_registry
                .as_ref()
                .map(|r| -> Box<dyn ServiceLookup> {
                    Box::new(RegistryServiceLookup::new(r.clone()))
                })
        };

        let make_state_manager = || -> Option<Box<dyn StateManager>> {
            self.state_provider.as_ref().map(|p| {
                Box::new(crate::repository::state_provider::CoreStateManager::new(
                    p.clone(),
                    self.name.clone(),
                )) as Box<dyn StateManager>
            })
        };

        #[allow(unused_assignments)]
        let mut last_ctx = NodeProcessContext {
            name: self.name.clone(),
            id: self.id.clone(),
            properties: self.properties.read().clone(),
            yield_duration_ms: 1000,
            service_lookup: make_service_lookup(),
            state_manager: make_state_manager(),
        };

        'lifecycle: loop {
            // Re-read properties from shared store on each lifecycle iteration.
            // This picks up any config changes made via the API while stopped.
            let ctx = NodeProcessContext {
                name: self.name.clone(),
                id: self.id.clone(),
                properties: self.properties.read().clone(),
                yield_duration_ms: 1000,
                service_lookup: make_service_lookup(),
                state_manager: make_state_manager(),
            };
            last_ctx = NodeProcessContext {
                name: ctx.name.clone(),
                id: ctx.id.clone(),
                properties: ctx.properties.clone(),
                yield_duration_ms: ctx.yield_duration_ms,
                service_lookup: make_service_lookup(),
                state_manager: make_state_manager(),
            };
            // Wait until the processor is enabled and not disabled (or cancelled).
            loop {
                let enabled = self.metrics.enabled.load(Ordering::Relaxed);
                let disabled = self.metrics.disabled.load(Ordering::Relaxed);
                if enabled && !disabled {
                    break;
                }
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        tracing::info!(processor = %self.name, "Processor exiting (cancelled while stopped)");
                        return;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
            }

            // Call on_scheduled.
            if let Err(e) = self.supervisor.on_scheduled(&ctx) {
                tracing::error!(processor = %self.name, error = %e, "on_scheduled failed");
                self.bulletin_board.add(
                    &self.name,
                    BulletinSeverity::Error,
                    format!("on_scheduled failed: {e}"),
                );
                return;
            }

            self.metrics.active.store(true, Ordering::Relaxed);
            tracing::info!(processor = %self.name, "Processor started");

            loop {
                // Check per-processor enabled flag — break cleanly on stop.
                if !self.metrics.enabled.load(Ordering::Relaxed) {
                    tracing::info!(processor = %self.name, "Processor stopping (stopped)");
                    break;
                }

                // Check disabled flag — break cleanly on disable.
                if self.metrics.disabled.load(Ordering::Relaxed) {
                    tracing::info!(processor = %self.name, "Processor stopping (disabled)");
                    break;
                }

                // Check per-processor paused flag — skip invocation, sleep, continue.
                if self.metrics.paused.load(Ordering::Relaxed) {
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            tracing::info!(processor = %self.name, "Processor stopping (cancelled while paused)");
                            break 'lifecycle;
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    }
                    continue;
                }

                // Wait for trigger based on scheduling strategy.
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        tracing::info!(processor = %self.name, "Processor stopping (cancelled)");
                        break 'lifecycle;
                    }
                    _ = self.wait_for_trigger() => {}
                }

                // Re-check enabled/disabled after waking from trigger wait.
                if !self.metrics.enabled.load(Ordering::Relaxed)
                    || self.metrics.disabled.load(Ordering::Relaxed)
                {
                    tracing::info!(processor = %self.name, "Processor stopping (stopped/disabled)");
                    break;
                }

                // Check if the API requested a circuit reset.
                if self.metrics.reset_requested.swap(false, Ordering::Relaxed) {
                    self.supervisor.reset_circuit();
                    tracing::info!(processor = %self.name, "Circuit breaker reset via API");
                }

                // Skip if circuit breaker is open.
                if self.supervisor.is_circuit_open() {
                    tracing::warn!(processor = %self.name, "Circuit breaker open, skipping trigger");
                    self.bulletin_board.add(
                        &self.name,
                        BulletinSeverity::Warn,
                        "Circuit breaker open, skipping trigger".to_string(),
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }

                // Check back-pressure on output connections.
                if self.any_output_back_pressured() {
                    tracing::debug!(processor = %self.name, "Output back-pressured, yielding");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }

                // Record trigger timestamp.
                let now_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                self.metrics
                    .last_trigger_nanos
                    .store(now_nanos, Ordering::Relaxed);

                // Snapshot the current input connections for this session.
                // We snapshot rather than holding the lock across session lifetime
                // so the mutation handler is never blocked on the lock.
                let input_conns_snapshot: Vec<Arc<FlowConnection>> =
                    self.input_connections.read().clone();

                // Create session directly — CoreProcessSession handles content repo,
                // ID generation, connection routing, and WAL tracking.
                let mut session = CoreProcessSession::new(
                    self.content_repo.clone(),
                    self.id_gen.clone(),
                    input_conns_snapshot,
                    ctx.yield_duration_ms,
                    self.penalty_duration_ms,
                );
                session.set_provenance(
                    self.provenance_repo.clone(),
                    self.name.clone(),
                    self.type_name.clone(),
                );

                // Invoke with fault isolation via spawn_blocking.
                let result = {
                    self.supervisor
                        .invoke(&ctx, &mut session as &mut dyn ProcessSession)
                };

                // Sync supervisor metrics to shared atomics.
                self.metrics.sync_from_supervisor(
                    self.supervisor.total_invocations(),
                    self.supervisor.total_failures(),
                    self.supervisor.consecutive_failures(),
                    self.supervisor.is_circuit_open(),
                );

                // Track input metrics.
                let acquired = session.acquired_count();
                let acquired_bytes = session.acquired_bytes();
                if acquired > 0 {
                    self.metrics
                        .flowfiles_in
                        .fetch_add(acquired as u64, Ordering::Relaxed);
                    self.metrics
                        .bytes_in
                        .fetch_add(acquired_bytes, Ordering::Relaxed);
                }

                match &result {
                    InvocationResult::Success => {
                        if session.is_committed() {
                            let (ff_out, bytes_out, routed) = self.route_transfers(&mut session);
                            self.metrics
                                .flowfiles_out
                                .fetch_add(ff_out, Ordering::Relaxed);
                            self.metrics
                                .bytes_out
                                .fetch_add(bytes_out, Ordering::Relaxed);

                            // Build WAL batch: Upsert for routed, Delete for removed.
                            let remove_ids = session.take_committed_remove_ids();
                            if !routed.is_empty() || !remove_ids.is_empty() {
                                let mut ops: Vec<FlowFileOp<'_>> = Vec::new();
                                for (ff, conn_id) in &routed {
                                    ops.push(FlowFileOp::Upsert {
                                        flowfile: ff,
                                        queue_id: conn_id,
                                    });
                                }
                                for id in &remove_ids {
                                    ops.push(FlowFileOp::Delete { id: *id });
                                }
                                if let Err(e) = self.flowfile_repo.commit_batch(&ops) {
                                    tracing::error!(
                                        processor = %self.name,
                                        error = %e,
                                        "WAL commit_batch failed"
                                    );
                                }
                            }
                        }
                    }
                    InvocationResult::Yield => {
                        // Yield is not an error — commit the session and sleep
                        // for the configured yield duration before re-triggering.
                        if session.is_committed() {
                            let (ff_out, bytes_out, routed) = self.route_transfers(&mut session);
                            self.metrics
                                .flowfiles_out
                                .fetch_add(ff_out, Ordering::Relaxed);
                            self.metrics
                                .bytes_out
                                .fetch_add(bytes_out, Ordering::Relaxed);

                            let remove_ids = session.take_committed_remove_ids();
                            if !routed.is_empty() || !remove_ids.is_empty() {
                                let mut ops: Vec<FlowFileOp<'_>> = Vec::new();
                                for (ff, conn_id) in &routed {
                                    ops.push(FlowFileOp::Upsert {
                                        flowfile: ff,
                                        queue_id: conn_id,
                                    });
                                }
                                for id in &remove_ids {
                                    ops.push(FlowFileOp::Delete { id: *id });
                                }
                                if let Err(e) = self.flowfile_repo.commit_batch(&ops) {
                                    tracing::error!(
                                        processor = %self.name,
                                        error = %e,
                                        "WAL commit_batch failed"
                                    );
                                }
                            }
                        }
                        tracing::debug!(
                            processor = %self.name,
                            yield_ms = ctx.yield_duration_ms,
                            "Processor yielded, sleeping"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(ctx.yield_duration_ms))
                            .await;
                    }
                    InvocationResult::Failed(e) => {
                        tracing::warn!(
                            processor = %self.name,
                            error = %e,
                            consecutive = self.supervisor.consecutive_failures(),
                            "Processor failed"
                        );
                        self.bulletin_board.add(
                            &self.name,
                            BulletinSeverity::Warn,
                            format!(
                                "Processor failed (consecutive: {}): {}",
                                self.supervisor.consecutive_failures(),
                                e
                            ),
                        );
                        session.rollback();
                        let backoff = self.supervisor.current_backoff();
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                    }
                    InvocationResult::Panic(msg) => {
                        tracing::error!(
                            processor = %self.name,
                            panic = %msg,
                            consecutive = self.supervisor.consecutive_failures(),
                            "Processor panicked"
                        );
                        self.bulletin_board.add(
                            &self.name,
                            BulletinSeverity::Error,
                            format!(
                                "Processor panicked (consecutive: {}): {}",
                                self.supervisor.consecutive_failures(),
                                msg
                            ),
                        );
                        // Session is automatically rolled back on drop.
                    }
                }
            }

            // Inner loop broken — processor is stopped.
            self.metrics.active.store(false, Ordering::Relaxed);
            self.supervisor.on_stopped(&ctx);
            tracing::info!(processor = %self.name, "Processor stopped");

            // Continue the lifecycle loop — will wait for re-enable or cancellation.
        }

        // Only reached when breaking out of 'lifecycle (cancellation).
        self.metrics.active.store(false, Ordering::Relaxed);
        self.supervisor.on_stopped(&last_ctx);
        tracing::info!(processor = %self.name, "Processor stopped");
    }

    async fn wait_for_trigger(&self) {
        match &self.scheduling {
            SchedulingStrategy::TimerDriven { interval_ms } => {
                tokio::time::sleep(std::time::Duration::from_millis(*interval_ms)).await;
            }
            SchedulingStrategy::EventDriven => {
                // Snapshot the notifiers under a brief lock, then await outside
                // the lock so we never hold parking_lot across an await point.
                let notifiers: Vec<Arc<Notify>> = self.input_notifiers.read().clone();
                if notifiers.is_empty() {
                    // No inputs wired -- suspend forever (cancel token will break the loop).
                    std::future::pending::<()>().await;
                } else {
                    // Race all input connection notifiers -- wake on ANY data arrival.
                    let futures: Vec<_> =
                        notifiers.iter().map(|n| Box::pin(n.notified())).collect();
                    futures::future::select_all(futures).await;
                }
            }
            SchedulingStrategy::CronDriven { expression } => {
                use std::str::FromStr;
                match cron::Schedule::from_str(expression) {
                    Ok(schedule) => {
                        let now = chrono::Utc::now();
                        if let Some(next) = schedule.upcoming(chrono::Utc).next() {
                            let delay = (next - now).to_std().unwrap_or_default();
                            tracing::debug!(
                                processor = %self.name,
                                next_fire = %next,
                                delay_ms = delay.as_millis(),
                                "CRON: waiting for next fire time"
                            );
                            tokio::time::sleep(delay).await;
                        } else {
                            // No upcoming fire time -- sleep for a long time.
                            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            processor = %self.name,
                            expression = %expression,
                            error = %e,
                            "Invalid CRON expression, falling back to 60s interval"
                        );
                        self.bulletin_board.add(
                            &self.name,
                            BulletinSeverity::Error,
                            format!("Invalid CRON expression '{}': {}", expression, e),
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    }
                }
            }
        }
    }

    fn any_output_back_pressured(&self) -> bool {
        self.output_connections
            .read()
            .iter()
            .any(|(_, conn)| conn.is_back_pressured())
    }

    /// Route transfers and return (flowfiles_out, bytes_out, routed_flowfiles).
    ///
    /// The third element contains `(FlowFile, connection_id)` for each
    /// successfully routed FlowFile, used by the WAL.
    ///
    /// When multiple connections exist for the same relationship, uses the
    /// load balance strategy (configured per-connection) to select the target:
    /// - `DoNotLoadBalance`: sends to the first matching connection (default)
    /// - `RoundRobin`: distributes evenly across all connections for the relationship
    /// - `PartitionByAttribute`: routes based on a hash of the specified attribute
    /// - `SingleNode`: sends all FlowFiles to the first connection
    fn route_transfers(
        &self,
        session: &mut CoreProcessSession,
    ) -> (u64, u64, Vec<(FlowFile, String)>) {
        let mut ff_out: u64 = 0;
        let mut bytes_out: u64 = 0;
        let mut routed_list: Vec<(FlowFile, String)> = Vec::new();
        // Snapshot output connections for routing; brief lock, then released.
        let output_connections: Vec<(Relationship, Arc<FlowConnection>)> =
            self.output_connections.read().clone();
        for (flowfile, rel_name) in session.take_transfers() {
            let size = flowfile.size;

            // Collect all connections for this relationship.
            let matching_conns: Vec<&Arc<FlowConnection>> = output_connections
                .iter()
                .filter(|(rel, _)| rel.name == rel_name)
                .map(|(_, conn)| conn)
                .collect();

            if matching_conns.is_empty() {
                // Check if the relationship is auto-terminated.
                let is_auto_term = output_connections
                    .iter()
                    .any(|(rel, _)| rel.name == rel_name && rel.auto_terminated);
                if !is_auto_term {
                    tracing::debug!(
                        processor = %self.name,
                        relationship = rel_name,
                        "No connection for relationship (auto-terminated or unconnected)"
                    );
                }
                continue;
            }

            // Select target connection using load balance strategy.
            let target_idx = if matching_conns.len() == 1 {
                0
            } else {
                self.select_load_balanced_connection(&matching_conns, &flowfile)
            };

            let conn = matching_conns[target_idx];
            let conn_id = conn.id.clone();
            if let Err(_ff) = conn.try_send(flowfile.clone()) {
                tracing::warn!(
                    processor = %self.name,
                    relationship = rel_name,
                    connection_id = %conn_id,
                    "Failed to route FlowFile -- connection full"
                );
                self.bulletin_board.add(
                    &self.name,
                    BulletinSeverity::Warn,
                    format!(
                        "Failed to route FlowFile on relationship '{}' -- connection full",
                        rel_name
                    ),
                );
            } else {
                ff_out += 1;
                bytes_out += size;
                routed_list.push((flowfile, conn_id));
            }
        }
        (ff_out, bytes_out, routed_list)
    }

    /// Select a target connection index using the load balance strategy
    /// configured on the connections.
    ///
    /// If any connection has a load balance config, its strategy is used.
    /// Otherwise, defaults to the first connection (DoNotLoadBalance behavior).
    fn select_load_balanced_connection(
        &self,
        connections: &[&Arc<FlowConnection>],
        flowfile: &FlowFile,
    ) -> usize {
        // Find the first connection with a load balance config to determine strategy.
        let lb_config = connections
            .iter()
            .find_map(|conn| conn.load_balance_config());

        let Some(config) = lb_config else {
            return 0; // No load balancing configured — use first connection.
        };

        match &config.strategy {
            LoadBalanceStrategy::DoNotLoadBalance => 0,

            LoadBalanceStrategy::RoundRobin => {
                // Use the shared load balancer for round-robin distribution.
                let lb = self
                    .load_balancer
                    .get_or_init(|| LoadBalancer::new(LoadBalanceStrategy::RoundRobin));
                let nodes: Vec<String> = connections.iter().map(|c| c.id.clone()).collect();
                let local = nodes[0].clone();
                if let Some(target_id) = lb.select_node(&local, &nodes, None) {
                    nodes.iter().position(|n| *n == target_id).unwrap_or(0)
                } else {
                    0
                }
            }

            LoadBalanceStrategy::PartitionByAttribute { attribute } => {
                // Hash the attribute value to select a consistent target.
                let attr_value = flowfile
                    .attributes
                    .iter()
                    .find(|(k, _)| k.as_ref() == attribute.as_str())
                    .map(|(_, v)| v.as_ref() as &str);

                let lb = self.load_balancer.get_or_init(|| {
                    LoadBalancer::new(LoadBalanceStrategy::PartitionByAttribute {
                        attribute: attribute.clone(),
                    })
                });
                let nodes: Vec<String> = connections.iter().map(|c| c.id.clone()).collect();
                let local = nodes[0].clone();
                if let Some(target_id) = lb.select_node(&local, &nodes, attr_value) {
                    nodes.iter().position(|n| *n == target_id).unwrap_or(0)
                } else {
                    0
                }
            }

            LoadBalanceStrategy::SingleNode { .. } => 0,
        }
    }
}
