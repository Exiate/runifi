use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::property::{PropertyDescriptor, PropertyValue};
use runifi_plugin_api::reporting::{
    BulletinSnapshot, ConnectionStatus, ProcessorStatus, ReportingContext, ReportingTask,
};
use runifi_plugin_api::state::StateManager;

use super::bulletin::BulletinBoard;
use super::handle::{ConnectionInfo, ProcessorInfo, PropertyDescriptorInfo};
use super::reporting_supervisor::ReportingTaskSupervisor;
use super::supervisor::InvocationResult;
use crate::repository::state_provider::{CoreStateManager, SharedLocalStateProvider};

/// Error type for reporting task operations.
#[derive(Debug, thiserror::Error)]
pub enum ReportingTaskError {
    #[error("Reporting task not found: {0}")]
    NotFound(String),
    #[error("Reporting task already exists: {0}")]
    DuplicateName(String),
    #[error("Reporting task '{name}': {message}")]
    InvalidState { name: String, message: String },
    #[error("Unknown reporting task type: {0}")]
    UnknownType(String),
}

/// Lifecycle state of a reporting task instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportingTaskState {
    Stopped,
    Running,
}

impl ReportingTaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReportingTaskState::Stopped => "STOPPED",
            ReportingTaskState::Running => "RUNNING",
        }
    }
}

/// Shared metrics for a reporting task (atomics for API reads).
pub struct ReportingTaskMetrics {
    pub total_invocations: AtomicU64,
    pub total_failures: AtomicU64,
    pub consecutive_failures: AtomicU64,
    pub circuit_open: AtomicBool,
    pub last_trigger_nanos: AtomicU64,
}

impl ReportingTaskMetrics {
    pub fn new() -> Self {
        Self {
            total_invocations: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            circuit_open: AtomicBool::new(false),
            last_trigger_nanos: AtomicU64::new(0),
        }
    }

    pub fn sync_from_supervisor(&self, supervisor: &ReportingTaskSupervisor) {
        self.total_invocations
            .store(supervisor.total_invocations(), Ordering::Relaxed);
        self.total_failures
            .store(supervisor.total_failures(), Ordering::Relaxed);
        self.consecutive_failures
            .store(supervisor.consecutive_failures() as u64, Ordering::Relaxed);
        self.circuit_open
            .store(supervisor.is_circuit_open(), Ordering::Relaxed);
    }
}

impl Default for ReportingTaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a reporting task instance, visible to the API.
#[derive(Clone)]
pub struct ReportingTaskInfo {
    pub name: String,
    pub type_name: String,
    pub state: ReportingTaskState,
    pub scheduling_display: String,
    pub properties: HashMap<String, String>,
    pub property_descriptors: Vec<PropertyDescriptorInfo>,
    pub metrics: Arc<ReportingTaskMetrics>,
}

/// Scheduling mode for a reporting task.
#[derive(Debug, Clone)]
pub enum ReportingTaskSchedule {
    Timer { interval_ms: u64 },
    Cron { expression: String },
}

impl ReportingTaskSchedule {
    pub fn display(&self) -> String {
        match self {
            ReportingTaskSchedule::Timer { interval_ms } => {
                format!("timer-driven ({}ms)", interval_ms)
            }
            ReportingTaskSchedule::Cron { expression } => {
                format!("cron-driven ({})", expression)
            }
        }
    }
}

/// A managed reporting task instance.
struct ManagedReportingTask {
    type_name: String,
    task: Option<Box<dyn ReportingTask>>,
    schedule: ReportingTaskSchedule,
    properties: HashMap<String, String>,
    property_descriptors: Vec<PropertyDescriptorInfo>,
    metrics: Arc<ReportingTaskMetrics>,
    state: ReportingTaskState,
    cancel_token: Option<CancellationToken>,
    task_handle: Option<tokio::task::JoinHandle<Box<dyn ReportingTask>>>,
}

/// Manages the lifecycle of reporting task instances.
pub struct ReportingTaskManager {
    tasks: HashMap<String, ManagedReportingTask>,
    bulletin_board: Arc<BulletinBoard>,
    state_provider: Option<SharedLocalStateProvider>,
}

/// Thread-safe wrapper for the ReportingTaskManager.
pub type SharedReportingTaskManager = Arc<RwLock<ReportingTaskManager>>;

impl ReportingTaskManager {
    pub fn new(
        bulletin_board: Arc<BulletinBoard>,
        state_provider: Option<SharedLocalStateProvider>,
    ) -> Self {
        Self {
            tasks: HashMap::new(),
            bulletin_board,
            state_provider,
        }
    }

    /// Add a new reporting task in Stopped state.
    pub fn add_task(
        &mut self,
        name: String,
        type_name: String,
        task: Box<dyn ReportingTask>,
        schedule: ReportingTaskSchedule,
        properties: HashMap<String, String>,
    ) -> Result<(), ReportingTaskError> {
        if self.tasks.contains_key(&name) {
            return Err(ReportingTaskError::DuplicateName(name));
        }

        let descriptors: Vec<PropertyDescriptorInfo> = task
            .property_descriptors()
            .iter()
            .map(descriptor_to_info)
            .collect();

        let managed = ManagedReportingTask {
            type_name,
            task: Some(task),
            schedule,
            properties,
            property_descriptors: descriptors,
            metrics: Arc::new(ReportingTaskMetrics::new()),
            state: ReportingTaskState::Stopped,
            cancel_token: None,
            task_handle: None,
        };

        self.tasks.insert(name, managed);
        Ok(())
    }

    /// Update the configuration of a stopped reporting task.
    pub fn configure_task(
        &mut self,
        name: &str,
        properties: HashMap<String, String>,
    ) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state == ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "must be stopped before reconfiguring".to_string(),
            });
        }

        managed.properties = properties;
        Ok(())
    }

    /// Update the scheduling of a stopped reporting task.
    pub fn configure_schedule(
        &mut self,
        name: &str,
        schedule: ReportingTaskSchedule,
    ) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state == ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "must be stopped before changing schedule".to_string(),
            });
        }

        managed.schedule = schedule;
        Ok(())
    }

    /// Start a reporting task's scheduling loop.
    pub fn start_task(
        &mut self,
        name: &str,
        processors: Arc<RwLock<Vec<ProcessorInfo>>>,
        connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    ) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state == ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            });
        }

        let task = managed
            .task
            .take()
            .ok_or_else(|| ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "task instance not available (already consumed)".to_string(),
            })?;

        let cancel_token = CancellationToken::new();
        let metrics = managed.metrics.clone();
        let schedule = managed.schedule.clone();
        let properties = managed.properties.clone();
        let task_name = name.to_string();
        let task_id = format!("reporting-task-{}", name);
        let bulletin_board = self.bulletin_board.clone();

        let state_manager: Option<Box<dyn StateManager>> =
            self.state_provider
                .as_ref()
                .map(|provider| -> Box<dyn StateManager> {
                    Box::new(CoreStateManager::new(provider.clone(), task_id.clone()))
                });

        let token = cancel_token.clone();
        let handle = tokio::spawn(run_reporting_task(
            task,
            schedule,
            properties,
            task_name,
            task_id,
            token,
            metrics.clone(),
            bulletin_board,
            processors,
            connections,
            state_manager,
        ));

        managed.cancel_token = Some(cancel_token);
        managed.task_handle = Some(handle);
        managed.state = ReportingTaskState::Running;

        Ok(())
    }

    /// Prepare to stop a running reporting task by extracting the cancel token
    /// and task handle. This allows callers to cancel and await the task without
    /// holding the manager lock across an `.await` boundary.
    #[allow(clippy::type_complexity)]
    pub fn prepare_stop(
        &mut self,
        name: &str,
    ) -> Result<
        (
            Option<CancellationToken>,
            Option<tokio::task::JoinHandle<Box<dyn ReportingTask>>>,
        ),
        ReportingTaskError,
    > {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state != ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            });
        }

        let cancel_token = managed.cancel_token.take();
        let task_handle = managed.task_handle.take();
        // Mark as Stopping so no new start can race.
        managed.state = ReportingTaskState::Stopped;
        Ok((cancel_token, task_handle))
    }

    /// Finalize stop by restoring the recovered task instance.
    pub fn finalize_stop(
        &mut self,
        name: &str,
        recovered_task: Option<Box<dyn ReportingTask>>,
    ) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if let Some(task) = recovered_task {
            managed.task = Some(task);
        }
        managed.state = ReportingTaskState::Stopped;
        Ok(())
    }

    /// Stop a running reporting task.
    pub async fn stop_task(&mut self, name: &str) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get_mut(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state != ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            });
        }

        if let Some(token) = managed.cancel_token.take() {
            token.cancel();
        }

        if let Some(handle) = managed.task_handle.take() {
            match handle.await {
                Ok(task) => {
                    managed.task = Some(task);
                }
                Err(e) => {
                    tracing::error!(
                        task = name,
                        error = %e,
                        "Reporting task join error"
                    );
                }
            }
        }

        managed.state = ReportingTaskState::Stopped;
        Ok(())
    }

    /// Remove a stopped reporting task.
    pub fn remove_task(&mut self, name: &str) -> Result<(), ReportingTaskError> {
        let managed = self
            .tasks
            .get(name)
            .ok_or_else(|| ReportingTaskError::NotFound(name.to_string()))?;

        if managed.state == ReportingTaskState::Running {
            return Err(ReportingTaskError::InvalidState {
                name: name.to_string(),
                message: "must be stopped before removing".to_string(),
            });
        }

        self.tasks.remove(name);
        Ok(())
    }

    /// List all reporting tasks.
    pub fn list_tasks(&self) -> Vec<ReportingTaskInfo> {
        self.tasks
            .iter()
            .map(|(name, managed)| ReportingTaskInfo {
                name: name.clone(),
                type_name: managed.type_name.clone(),
                state: managed.state,
                scheduling_display: managed.schedule.display(),
                properties: managed.properties.clone(),
                property_descriptors: managed.property_descriptors.clone(),
                metrics: managed.metrics.clone(),
            })
            .collect()
    }

    /// Get info about a specific reporting task.
    pub fn get_task(&self, name: &str) -> Option<ReportingTaskInfo> {
        self.tasks.get(name).map(|managed| ReportingTaskInfo {
            name: name.to_string(),
            type_name: managed.type_name.clone(),
            state: managed.state,
            scheduling_display: managed.schedule.display(),
            properties: managed.properties.clone(),
            property_descriptors: managed.property_descriptors.clone(),
            metrics: managed.metrics.clone(),
        })
    }

    /// Stop all running tasks (for engine shutdown).
    pub async fn stop_all(&mut self) {
        let running: Vec<String> = self
            .tasks
            .iter()
            .filter(|(_, m)| m.state == ReportingTaskState::Running)
            .map(|(name, _)| name.clone())
            .collect();

        for name in running {
            if let Err(e) = self.stop_task(&name).await {
                tracing::error!(task = %name, error = %e, "Failed to stop reporting task");
            }
        }
    }
}

fn descriptor_to_info(pd: &PropertyDescriptor) -> PropertyDescriptorInfo {
    PropertyDescriptorInfo {
        name: pd.name.to_string(),
        description: pd.description.to_string(),
        required: pd.required,
        default_value: pd.default_value.map(|v| v.to_string()),
        sensitive: pd.sensitive,
        allowed_values: pd
            .allowed_values
            .map(|vs| vs.iter().map(|v| v.to_string()).collect()),
    }
}

/// Runtime context implementation for reporting tasks.
struct CoreReportingContext {
    name: String,
    id: String,
    properties: HashMap<String, String>,
    processors: Arc<RwLock<Vec<ProcessorInfo>>>,
    connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    bulletin_board: Arc<BulletinBoard>,
    state_manager: Option<Box<dyn StateManager>>,
}

impl ReportingContext for CoreReportingContext {
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

    fn state_manager(&self) -> Option<&dyn StateManager> {
        self.state_manager.as_deref()
    }

    fn processor_statuses(&self) -> Vec<ProcessorStatus> {
        let procs = self.processors.read();
        procs
            .iter()
            .map(|p| {
                let snapshot = p.metrics.snapshot();
                ProcessorStatus {
                    name: p.name.clone(),
                    type_name: p.type_name.clone(),
                    state: snapshot.state.as_str().to_string(),
                    total_invocations: snapshot.total_invocations,
                    total_failures: snapshot.total_failures,
                    flowfiles_in: snapshot.flowfiles_in,
                    flowfiles_out: snapshot.flowfiles_out,
                    bytes_in: snapshot.bytes_in,
                    bytes_out: snapshot.bytes_out,
                    active: snapshot.active,
                }
            })
            .collect()
    }

    fn connection_statuses(&self) -> Vec<ConnectionStatus> {
        let conns = self.connections.read();
        conns
            .iter()
            .map(|c| ConnectionStatus {
                id: c.id.clone(),
                source_name: c.source_name.clone(),
                destination_name: c.dest_name.clone(),
                queued_count: c.connection.queue_count(),
                queued_bytes: c.connection.queue_size_bytes(),
                back_pressured: c.connection.is_back_pressured(),
            })
            .collect()
    }

    fn bulletins(&self) -> Vec<BulletinSnapshot> {
        self.bulletin_board
            .get_all(None, None)
            .into_iter()
            .map(|b| BulletinSnapshot {
                id: b.id,
                timestamp_ms: b.timestamp_ms,
                severity: b.severity.as_str().to_string(),
                source_name: b.processor_name.clone(),
                message: b.message.clone(),
            })
            .collect()
    }
}

/// Async loop that runs a reporting task on schedule.
#[allow(clippy::too_many_arguments)]
async fn run_reporting_task(
    task: Box<dyn ReportingTask>,
    schedule: ReportingTaskSchedule,
    properties: HashMap<String, String>,
    name: String,
    id: String,
    cancel_token: CancellationToken,
    metrics: Arc<ReportingTaskMetrics>,
    bulletin_board: Arc<BulletinBoard>,
    processors: Arc<RwLock<Vec<ProcessorInfo>>>,
    connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    state_manager: Option<Box<dyn StateManager>>,
) -> Box<dyn ReportingTask> {
    let context = Arc::new(CoreReportingContext {
        name: name.clone(),
        id,
        properties,
        processors,
        connections,
        bulletin_board: bulletin_board.clone(),
        state_manager,
    });

    let supervisor = Arc::new(Mutex::new(ReportingTaskSupervisor::new(task)));

    // Call on_scheduled.
    {
        let mut sup = supervisor.lock();
        if let Err(e) = sup.on_scheduled(context.as_ref()) {
            tracing::error!(
                task = %name,
                error = %e,
                "Reporting task on_scheduled failed"
            );
            bulletin_board.add(
                &name,
                super::bulletin::BulletinSeverity::Error,
                format!("on_scheduled failed: {}", e),
            );
        }
    }

    loop {
        // Wait for the schedule interval or cancellation.
        let wait = match &schedule {
            ReportingTaskSchedule::Timer { interval_ms } => {
                tokio::time::Duration::from_millis(*interval_ms)
            }
            ReportingTaskSchedule::Cron { .. } => {
                // Simplified: fall back to 60s for CRON (full CRON parsing is out of scope).
                tokio::time::Duration::from_secs(60)
            }
        };

        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }
            _ = tokio::time::sleep(wait) => {}
        }

        if cancel_token.is_cancelled() {
            break;
        }

        // Apply backoff if needed.
        let backoff = supervisor.lock().current_backoff();
        if !backoff.is_zero() {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                _ = tokio::time::sleep(backoff) => {}
            }
            if cancel_token.is_cancelled() {
                break;
            }
        }

        // Run the task in spawn_blocking for fault isolation.
        let sup = supervisor.clone();
        let ctx = context.clone();
        let task_name = name.clone();
        let bb = bulletin_board.clone();

        let result = tokio::task::spawn_blocking(move || {
            let mut s = sup.lock();
            let result = s.invoke(ctx.as_ref());
            (
                result,
                s.total_invocations(),
                s.total_failures(),
                s.consecutive_failures(),
                s.is_circuit_open(),
            )
        })
        .await;

        match result {
            Ok((invocation_result, total_inv, total_fail, consec_fail, circuit)) => {
                metrics
                    .total_invocations
                    .store(total_inv, Ordering::Relaxed);
                metrics.total_failures.store(total_fail, Ordering::Relaxed);
                metrics
                    .consecutive_failures
                    .store(consec_fail as u64, Ordering::Relaxed);
                metrics.circuit_open.store(circuit, Ordering::Relaxed);
                metrics.last_trigger_nanos.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64,
                    Ordering::Relaxed,
                );

                match invocation_result {
                    InvocationResult::Success | InvocationResult::Yield => {}
                    InvocationResult::Failed(msg) => {
                        tracing::warn!(task = %task_name, error = %msg, "Reporting task failed");
                        bb.add(
                            &task_name,
                            super::bulletin::BulletinSeverity::Warn,
                            format!("Reporting task failed: {}", msg),
                        );
                    }
                    InvocationResult::Panic(msg) => {
                        tracing::error!(task = %task_name, error = %msg, "Reporting task panicked");
                        bb.add(
                            &task_name,
                            super::bulletin::BulletinSeverity::Error,
                            format!("Reporting task panicked: {}", msg),
                        );
                    }
                }

                if circuit {
                    tracing::error!(task = %task_name, "Reporting task circuit breaker open — sleeping");
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                    }
                }
            }
            Err(e) => {
                tracing::error!(task = %task_name, error = %e, "Reporting task spawn_blocking join error");
            }
        }
    }

    // Call on_stopped then recover the task instance for potential restart.
    {
        let mut sup = supervisor.lock();
        sup.on_stopped(context.as_ref());
    }

    // Extract the task from the supervisor so it can be re-started.
    match Arc::try_unwrap(supervisor) {
        Ok(mutex) => mutex.into_inner().into_inner(),
        Err(_arc) => {
            tracing::error!(
                task = %name,
                "Could not recover reporting task — Arc still has references"
            );
            // Fallback: create a no-op placeholder. The task cannot be restarted
            // without being removed and re-created.
            Box::new(NoOpReportingTask)
        }
    }
}

struct NoOpReportingTask;
impl ReportingTask for NoOpReportingTask {
    fn on_trigger(
        &mut self,
        _context: &dyn ReportingContext,
    ) -> runifi_plugin_api::result::ProcessResult {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTask;
    impl ReportingTask for TestTask {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ReportingContext,
        ) -> runifi_plugin_api::result::ProcessResult {
            Ok(())
        }
        fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
            vec![PropertyDescriptor::new("Test Prop", "A test property")]
        }
    }

    fn make_manager() -> ReportingTaskManager {
        ReportingTaskManager::new(Arc::new(BulletinBoard::new(100)), None)
    }

    #[test]
    fn add_and_list_tasks() {
        let mut mgr = make_manager();
        mgr.add_task(
            "test-task".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        )
        .unwrap();

        let tasks = mgr.list_tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].name, "test-task");
        assert_eq!(tasks[0].type_name, "TestTask");
        assert_eq!(tasks[0].state, ReportingTaskState::Stopped);
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut mgr = make_manager();
        mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        )
        .unwrap();

        let result = mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        );
        assert!(matches!(result, Err(ReportingTaskError::DuplicateName(_))));
    }

    #[test]
    fn remove_stopped_task() {
        let mut mgr = make_manager();
        mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        )
        .unwrap();

        mgr.remove_task("task-1").unwrap();
        assert!(mgr.list_tasks().is_empty());
    }

    #[test]
    fn remove_nonexistent_fails() {
        let mut mgr = make_manager();
        let result = mgr.remove_task("nope");
        assert!(matches!(result, Err(ReportingTaskError::NotFound(_))));
    }

    #[test]
    fn configure_stopped_task() {
        let mut mgr = make_manager();
        mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        )
        .unwrap();

        let mut props = HashMap::new();
        props.insert("Test Prop".to_string(), "value".to_string());
        mgr.configure_task("task-1", props.clone()).unwrap();

        let info = mgr.get_task("task-1").unwrap();
        assert_eq!(info.properties.get("Test Prop").unwrap(), "value");
    }

    #[test]
    fn get_task_returns_none_for_unknown() {
        let mgr = make_manager();
        assert!(mgr.get_task("nope").is_none());
    }

    #[test]
    fn property_descriptors_populated() {
        let mut mgr = make_manager();
        mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer { interval_ms: 5000 },
            HashMap::new(),
        )
        .unwrap();

        let info = mgr.get_task("task-1").unwrap();
        assert_eq!(info.property_descriptors.len(), 1);
        assert_eq!(info.property_descriptors[0].name, "Test Prop");
    }

    #[tokio::test]
    async fn start_and_stop_task() {
        let mut mgr = make_manager();
        mgr.add_task(
            "task-1".to_string(),
            "TestTask".to_string(),
            Box::new(TestTask),
            ReportingTaskSchedule::Timer {
                interval_ms: 60_000,
            },
            HashMap::new(),
        )
        .unwrap();

        let processors = Arc::new(RwLock::new(Vec::new()));
        let connections = Arc::new(RwLock::new(Vec::new()));

        mgr.start_task("task-1", processors, connections).unwrap();
        assert_eq!(
            mgr.get_task("task-1").unwrap().state,
            ReportingTaskState::Running
        );

        mgr.stop_task("task-1").await.unwrap();
        assert_eq!(
            mgr.get_task("task-1").unwrap().state,
            ReportingTaskState::Stopped
        );
    }

    #[test]
    fn scheduling_display() {
        let timer = ReportingTaskSchedule::Timer {
            interval_ms: 30_000,
        };
        assert_eq!(timer.display(), "timer-driven (30000ms)");

        let cron = ReportingTaskSchedule::Cron {
            expression: "0 */5 * * * *".to_string(),
        };
        assert_eq!(cron.display(), "cron-driven (0 */5 * * * *)");
    }
}
