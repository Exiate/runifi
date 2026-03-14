use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use runifi_plugin_api::reporting::{ReportingContext, ReportingTask};
use runifi_plugin_api::result::ProcessResult;

use super::supervisor::InvocationResult;

/// Wraps a reporting task with fault isolation, failure tracking, and circuit breaker logic.
///
/// Every `on_trigger` call is wrapped in `catch_unwind` to prevent panics from
/// crashing the runtime. Consecutive failures trigger exponential backoff,
/// and the circuit breaker trips after `max_consecutive_failures`.
pub struct ReportingTaskSupervisor {
    task: Box<dyn ReportingTask>,
    consecutive_failures: u32,
    total_invocations: u64,
    total_failures: u64,
    max_consecutive_failures: u32,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
    circuit_open: bool,
}

impl ReportingTaskSupervisor {
    pub fn new(task: Box<dyn ReportingTask>) -> Self {
        Self {
            task,
            consecutive_failures: 0,
            total_invocations: 0,
            total_failures: 0,
            max_consecutive_failures: 5,
            backoff_base_ms: 100,
            backoff_cap_ms: 30_000,
            circuit_open: false,
        }
    }

    pub fn is_circuit_open(&self) -> bool {
        self.circuit_open
    }

    pub fn reset_circuit(&mut self) {
        self.circuit_open = false;
        self.consecutive_failures = 0;
    }

    pub fn current_backoff(&self) -> Duration {
        if self.consecutive_failures == 0 {
            return Duration::ZERO;
        }
        let ms = self
            .backoff_base_ms
            .saturating_mul(1u64 << self.consecutive_failures.min(20))
            .min(self.backoff_cap_ms);
        Duration::from_millis(ms)
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn total_invocations(&self) -> u64 {
        self.total_invocations
    }

    pub fn total_failures(&self) -> u64 {
        self.total_failures
    }

    /// Invoke the reporting task's `on_trigger` with panic isolation.
    pub fn invoke(&mut self, context: &dyn ReportingContext) -> InvocationResult {
        if self.circuit_open {
            return InvocationResult::Failed("circuit breaker open".to_string());
        }

        self.total_invocations += 1;

        let result = catch_unwind(AssertUnwindSafe(|| self.task.on_trigger(context)));

        match result {
            Ok(Ok(())) => {
                self.consecutive_failures = 0;
                InvocationResult::Success
            }
            Ok(Err(runifi_plugin_api::result::PluginError::Yield)) => {
                self.consecutive_failures = 0;
                InvocationResult::Yield
            }
            Ok(Err(e)) => {
                self.record_failure();
                InvocationResult::Failed(e.to_string())
            }
            Err(panic_info) => {
                self.record_failure();
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                InvocationResult::Panic(msg)
            }
        }
    }

    /// Call on_scheduled on the reporting task.
    pub fn on_scheduled(&mut self, context: &dyn ReportingContext) -> ProcessResult {
        self.task.on_scheduled(context)
    }

    /// Call on_stopped on the reporting task.
    pub fn on_stopped(&mut self, context: &dyn ReportingContext) {
        self.task.on_stopped(context);
    }

    /// Get the reporting task's property descriptors.
    pub fn property_descriptors(&self) -> Vec<runifi_plugin_api::PropertyDescriptor> {
        self.task.property_descriptors()
    }

    /// Consume the supervisor and return the inner reporting task.
    pub fn into_inner(self) -> Box<dyn ReportingTask> {
        self.task
    }

    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.total_failures += 1;
        if self.consecutive_failures >= self.max_consecutive_failures {
            self.circuit_open = true;
            tracing::error!(
                consecutive_failures = self.consecutive_failures,
                "Circuit breaker opened — reporting task disabled"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::reporting::{
        BulletinSnapshot, ConnectionStatus, ProcessorStatus, ReportingContext, ReportingTask,
    };
    use runifi_plugin_api::result::PluginError;

    // --- Test helpers ---

    struct OkTask;
    impl ReportingTask for OkTask {
        fn on_trigger(&mut self, _ctx: &dyn ReportingContext) -> ProcessResult {
            Ok(())
        }
    }

    struct FailTask;
    impl ReportingTask for FailTask {
        fn on_trigger(&mut self, _ctx: &dyn ReportingContext) -> ProcessResult {
            Err(PluginError::ProcessingFailed("test error".into()))
        }
    }

    struct PanicTask;
    impl ReportingTask for PanicTask {
        fn on_trigger(&mut self, _ctx: &dyn ReportingContext) -> ProcessResult {
            panic!("deliberate test panic");
        }
    }

    struct TestContext;
    impl ReportingContext for TestContext {
        fn get_property(&self, _name: &str) -> PropertyValue {
            PropertyValue::Unset
        }
        fn name(&self) -> &str {
            "test"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn processor_statuses(&self) -> Vec<ProcessorStatus> {
            Vec::new()
        }
        fn connection_statuses(&self) -> Vec<ConnectionStatus> {
            Vec::new()
        }
        fn bulletins(&self) -> Vec<BulletinSnapshot> {
            Vec::new()
        }
    }

    // --- Tests ---

    #[test]
    fn success_resets_consecutive_failures() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(OkTask));
        let ctx = TestContext;

        let result = sup.invoke(&ctx);
        assert!(matches!(result, InvocationResult::Success));
        assert_eq!(sup.consecutive_failures(), 0);
        assert_eq!(sup.total_invocations(), 1);
    }

    #[test]
    fn failure_increments_count() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        let result = sup.invoke(&ctx);
        assert!(matches!(result, InvocationResult::Failed(_)));
        assert_eq!(sup.consecutive_failures(), 1);
        assert_eq!(sup.total_failures(), 1);
    }

    #[test]
    fn panic_is_caught() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(PanicTask));
        let ctx = TestContext;

        let result = sup.invoke(&ctx);
        assert!(matches!(result, InvocationResult::Panic(_)));
        if let InvocationResult::Panic(msg) = result {
            assert!(msg.contains("deliberate test panic"));
        }
        assert_eq!(sup.consecutive_failures(), 1);
    }

    #[test]
    fn circuit_breaker_trips_after_max_failures() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        for _ in 0..5 {
            sup.invoke(&ctx);
        }

        assert!(sup.is_circuit_open());
        let result = sup.invoke(&ctx);
        assert!(matches!(result, InvocationResult::Failed(_)));
    }

    #[test]
    fn circuit_breaker_reset() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        for _ in 0..5 {
            sup.invoke(&ctx);
        }
        assert!(sup.is_circuit_open());

        sup.reset_circuit();
        assert!(!sup.is_circuit_open());
        assert_eq!(sup.consecutive_failures(), 0);
    }

    #[test]
    fn backoff_increases_exponentially() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        assert_eq!(sup.current_backoff(), Duration::ZERO);

        sup.invoke(&ctx);
        assert_eq!(sup.current_backoff(), Duration::from_millis(200));

        sup.invoke(&ctx);
        assert_eq!(sup.current_backoff(), Duration::from_millis(400));
    }

    #[test]
    fn backoff_capped_at_30s() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        for _ in 0..20 {
            sup.invoke(&ctx);
        }

        assert!(sup.current_backoff() <= Duration::from_millis(30_000));
    }

    #[test]
    fn success_after_failures_resets_count() {
        struct ToggleTask {
            call_count: u32,
            fail_count: u32,
        }
        impl ReportingTask for ToggleTask {
            fn on_trigger(&mut self, _ctx: &dyn ReportingContext) -> ProcessResult {
                self.call_count += 1;
                if self.call_count <= self.fail_count {
                    Err(PluginError::ProcessingFailed("test".into()))
                } else {
                    Ok(())
                }
            }
        }

        let task = ToggleTask {
            call_count: 0,
            fail_count: 3,
        };
        let mut sup = ReportingTaskSupervisor::new(Box::new(task));
        let ctx = TestContext;

        for _ in 0..3 {
            sup.invoke(&ctx);
        }
        assert_eq!(sup.consecutive_failures(), 3);
        assert_eq!(sup.total_failures(), 3);

        sup.invoke(&ctx);
        assert_eq!(sup.consecutive_failures(), 0);
        assert_eq!(sup.total_failures(), 3);
        assert_eq!(sup.total_invocations(), 4);
    }

    #[test]
    fn circuit_stays_open_after_more_invocations() {
        let mut sup = ReportingTaskSupervisor::new(Box::new(FailTask));
        let ctx = TestContext;

        for _ in 0..5 {
            sup.invoke(&ctx);
        }
        assert!(sup.is_circuit_open());

        let before_invocations = sup.total_invocations();
        let result = sup.invoke(&ctx);
        assert!(matches!(result, InvocationResult::Failed(_)));
        assert_eq!(sup.total_invocations(), before_invocations);
    }
}
