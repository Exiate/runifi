use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::Processor;
use runifi_plugin_api::property::PropertyValue;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::session::ProcessSession;

use super::metrics::ProcessorMetrics;
use super::supervisor::{InvocationResult, ProcessorSupervisor};
use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;
use crate::session::process_session::CoreProcessSession;

/// Scheduling strategy for a processor node.
#[derive(Debug, Clone)]
pub enum SchedulingStrategy {
    /// Trigger on a fixed interval.
    TimerDriven { interval_ms: u64 },
    /// Trigger when input data is available.
    EventDriven,
}

/// Runtime context for a single processor instance.
struct NodeProcessContext {
    name: String,
    id: String,
    properties: HashMap<String, String>,
    yield_duration_ms: u64,
}

impl runifi_plugin_api::context::ProcessContext for NodeProcessContext {
    fn get_property(&self, name: &str) -> PropertyValue {
        match self.properties.get(name) {
            Some(v) => PropertyValue::String(v.clone()),
            None => PropertyValue::Unset,
        }
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
}

/// A runtime wrapper for a single processor instance.
///
/// Manages the processor's lifecycle, connections, scheduling, and fault isolation.
pub struct ProcessorNode {
    pub name: String,
    pub id: String,
    pub scheduling: SchedulingStrategy,
    pub properties: HashMap<String, String>,
    supervisor: ProcessorSupervisor,
    input_connections: Vec<Arc<FlowConnection>>,
    output_connections: Vec<(Relationship, Arc<FlowConnection>)>,
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    cancel_token: CancellationToken,
    input_notifiers: Vec<Arc<Notify>>,
    metrics: Arc<ProcessorMetrics>,
}

impl ProcessorNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        id: String,
        processor: Box<dyn Processor>,
        scheduling: SchedulingStrategy,
        properties: HashMap<String, String>,
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        cancel_token: CancellationToken,
        metrics: Arc<ProcessorMetrics>,
    ) -> Self {
        Self {
            name,
            id,
            scheduling,
            properties,
            supervisor: ProcessorSupervisor::new(processor),
            input_connections: Vec::new(),
            output_connections: Vec::new(),
            content_repo,
            id_gen,
            cancel_token,
            input_notifiers: Vec::new(),
            metrics,
        }
    }

    /// Add an input connection and wire its notifier for event-driven wakeup.
    pub fn add_input(&mut self, connection: Arc<FlowConnection>) {
        self.input_notifiers.push(connection.notifier());
        self.input_connections.push(connection);
    }

    /// Add an output connection for a specific relationship.
    pub fn add_output(&mut self, relationship: Relationship, connection: Arc<FlowConnection>) {
        self.output_connections.push((relationship, connection));
    }

    /// Get the processor's supported relationships.
    pub fn relationships(&self) -> Vec<Relationship> {
        self.supervisor.relationships()
    }

    /// Check if the circuit breaker is open.
    pub fn is_circuit_open(&self) -> bool {
        self.supervisor.is_circuit_open()
    }

    /// Run the processor loop until cancelled.
    ///
    /// This is spawned as a tokio task by the engine.
    pub async fn run(mut self) {
        let ctx = NodeProcessContext {
            name: self.name.clone(),
            id: self.id.clone(),
            properties: self.properties.clone(),
            yield_duration_ms: 1000,
        };

        // Call on_scheduled.
        if let Err(e) = self.supervisor.on_scheduled(&ctx) {
            tracing::error!(processor = %self.name, error = %e, "on_scheduled failed");
            return;
        }

        self.metrics.active.store(true, Ordering::Relaxed);
        tracing::info!(processor = %self.name, "Processor started");

        loop {
            // Wait for trigger based on scheduling strategy.
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!(processor = %self.name, "Processor stopping (cancelled)");
                    break;
                }
                _ = self.wait_for_trigger() => {}
            }

            // Check if the API requested a circuit reset.
            if self.metrics.reset_requested.swap(false, Ordering::Relaxed) {
                self.supervisor.reset_circuit();
                tracing::info!(processor = %self.name, "Circuit breaker reset via API");
            }

            // Skip if circuit breaker is open.
            if self.supervisor.is_circuit_open() {
                tracing::warn!(processor = %self.name, "Circuit breaker open, skipping trigger");
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

            // Build session and invoke processor in a blocking thread.
            let mut session = CoreProcessSession::new(
                self.content_repo.clone(),
                self.id_gen.clone(),
                self.input_connections.clone(),
                ctx.yield_duration_ms,
            );

            // Invoke with fault isolation via spawn_blocking.
            let result = {
                // We need to pass mutable references into spawn_blocking.
                // Since the processor is !Send across await points in some cases,
                // we do the invocation inline here (it's synchronous anyway).
                self.supervisor.invoke(&ctx, &mut session)
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
                        let (ff_out, bytes_out) = self.route_transfers(&mut session);
                        self.metrics
                            .flowfiles_out
                            .fetch_add(ff_out, Ordering::Relaxed);
                        self.metrics
                            .bytes_out
                            .fetch_add(bytes_out, Ordering::Relaxed);
                    }
                }
                InvocationResult::Failed(e) => {
                    tracing::warn!(
                        processor = %self.name,
                        error = %e,
                        consecutive = self.supervisor.consecutive_failures(),
                        "Processor failed"
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
                    // Session is automatically rolled back on drop.
                }
            }
        }

        self.metrics.active.store(false, Ordering::Relaxed);
        self.supervisor.on_stopped(&ctx);
        tracing::info!(processor = %self.name, "Processor stopped");
    }

    async fn wait_for_trigger(&self) {
        match &self.scheduling {
            SchedulingStrategy::TimerDriven { interval_ms } => {
                tokio::time::sleep(std::time::Duration::from_millis(*interval_ms)).await;
            }
            SchedulingStrategy::EventDriven => {
                if self.input_notifiers.is_empty() {
                    // No inputs wired — suspend forever (cancel token will break the loop).
                    std::future::pending::<()>().await;
                } else {
                    // Race all input connection notifiers — wake on ANY data arrival.
                    let futures: Vec<_> = self
                        .input_notifiers
                        .iter()
                        .map(|n| Box::pin(n.notified()))
                        .collect();
                    futures::future::select_all(futures).await;
                }
            }
        }
    }

    fn any_output_back_pressured(&self) -> bool {
        self.output_connections
            .iter()
            .any(|(_, conn)| conn.is_back_pressured())
    }

    /// Route transfers and return (flowfiles_out, bytes_out).
    fn route_transfers(&self, session: &mut CoreProcessSession) -> (u64, u64) {
        let mut ff_out: u64 = 0;
        let mut bytes_out: u64 = 0;
        for (flowfile, rel_name) in session.take_transfers() {
            let size = flowfile.size;
            let mut routed = false;
            for (rel, conn) in &self.output_connections {
                if rel.name == rel_name {
                    if let Err(_ff) = conn.try_send(flowfile.clone()) {
                        tracing::warn!(
                            processor = %self.name,
                            relationship = rel_name,
                            "Failed to route FlowFile — connection full"
                        );
                    } else {
                        ff_out += 1;
                        bytes_out += size;
                    }
                    routed = true;
                    break;
                }
            }
            if !routed {
                // Check if the relationship is auto-terminated.
                let is_auto_term = self
                    .output_connections
                    .iter()
                    .any(|(rel, _)| rel.name == rel_name && rel.auto_terminated);
                if !is_auto_term {
                    tracing::debug!(
                        processor = %self.name,
                        relationship = rel_name,
                        "No connection for relationship (auto-terminated or unconnected)"
                    );
                }
            }
        }
        (ff_out, bytes_out)
    }
}
