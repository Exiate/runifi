use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use runifi_plugin_api::Processor;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

/// Result of a single processor invocation.
#[derive(Debug)]
pub enum InvocationResult {
    /// Processor completed successfully.
    Success,
    /// Processor returned an error.
    Failed(String),
    /// Processor panicked (caught by catch_unwind).
    Panic(String),
    /// Processor requested a yield (no more work available right now).
    Yield,
}

/// Wraps a processor with fault isolation, failure tracking, and circuit breaker logic.
///
/// Every `on_trigger` call is wrapped in `catch_unwind` to prevent panics from
/// crashing the runtime. Consecutive failures trigger exponential backoff,
/// and the circuit breaker trips after `max_consecutive_failures`.
pub struct ProcessorSupervisor {
    processor: Box<dyn Processor>,
    consecutive_failures: u32,
    total_invocations: u64,
    total_failures: u64,
    max_consecutive_failures: u32,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
    circuit_open: bool,
}

impl ProcessorSupervisor {
    pub fn new(processor: Box<dyn Processor>) -> Self {
        Self {
            processor,
            consecutive_failures: 0,
            total_invocations: 0,
            total_failures: 0,
            max_consecutive_failures: 5,
            backoff_base_ms: 100,
            backoff_cap_ms: 30_000,
            circuit_open: false,
        }
    }

    /// Check if the circuit breaker is open (processor is disabled).
    pub fn is_circuit_open(&self) -> bool {
        self.circuit_open
    }

    /// Reset the circuit breaker, re-enabling the processor.
    pub fn reset_circuit(&mut self) {
        self.circuit_open = false;
        self.consecutive_failures = 0;
    }

    /// Get the current backoff duration based on consecutive failures.
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

    /// Invoke the processor's `on_trigger` with panic isolation.
    ///
    /// This method is meant to be called from `spawn_blocking` by the engine.
    pub fn invoke(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> InvocationResult {
        if self.circuit_open {
            return InvocationResult::Failed("circuit breaker open".to_string());
        }

        self.total_invocations += 1;

        // Wrap in catch_unwind for panic isolation.
        // SAFETY: We need AssertUnwindSafe because ProcessContext/ProcessSession
        // are trait objects that may not be UnwindSafe. This is acceptable because
        // we don't access them after a panic — the session is rolled back.
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.processor.on_trigger(context, session)
        }));

        match result {
            Ok(Ok(())) => {
                self.consecutive_failures = 0;
                InvocationResult::Success
            }
            Ok(Err(runifi_plugin_api::result::PluginError::Yield)) => {
                // Yield is not a failure — reset consecutive failures.
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

    /// Call on_scheduled on the processor.
    pub fn on_scheduled(&mut self, context: &dyn ProcessContext) -> ProcessResult {
        self.processor.on_scheduled(context)
    }

    /// Call on_stopped on the processor.
    pub fn on_stopped(&mut self, context: &dyn ProcessContext) {
        self.processor.on_stopped(context);
    }

    /// Get the processor's relationships.
    pub fn relationships(&self) -> Vec<runifi_plugin_api::Relationship> {
        self.processor.relationships()
    }

    /// Get the processor's property descriptors.
    pub fn property_descriptors(&self) -> Vec<runifi_plugin_api::PropertyDescriptor> {
        self.processor.property_descriptors()
    }

    /// Validate the processor's configuration.
    pub fn validate(
        &self,
        context: &dyn ProcessContext,
    ) -> Vec<runifi_plugin_api::ValidationResult> {
        self.processor.validate(context)
    }

    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.total_failures += 1;
        if self.consecutive_failures >= self.max_consecutive_failures {
            self.circuit_open = true;
            tracing::error!(
                consecutive_failures = self.consecutive_failures,
                "Circuit breaker opened — processor disabled"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::relationship::Relationship;
    use runifi_plugin_api::result::PluginError;

    // --- Test helpers ---

    struct OkProcessor;
    impl Processor for OkProcessor {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            Ok(())
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![]
        }
    }

    struct FailProcessor;
    impl Processor for FailProcessor {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            Err(PluginError::ProcessingFailed("test error".into()))
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![]
        }
    }

    struct PanicProcessor;
    impl Processor for PanicProcessor {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            panic!("deliberate test panic");
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![]
        }
    }

    struct TestContext;
    impl ProcessContext for TestContext {
        fn get_property(&self, _name: &str) -> PropertyValue {
            PropertyValue::Unset
        }
        fn name(&self) -> &str {
            "test"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct NoOpSession;
    impl ProcessSession for NoOpSession {
        fn get(&mut self) -> Option<runifi_plugin_api::FlowFile> {
            None
        }
        fn get_batch(&mut self, _max: usize) -> Vec<runifi_plugin_api::FlowFile> {
            vec![]
        }
        fn read_content(&self, _ff: &runifi_plugin_api::FlowFile) -> ProcessResult<bytes::Bytes> {
            Ok(bytes::Bytes::new())
        }
        fn write_content(
            &mut self,
            ff: runifi_plugin_api::FlowFile,
            _data: bytes::Bytes,
        ) -> ProcessResult<runifi_plugin_api::FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> runifi_plugin_api::FlowFile {
            unimplemented!()
        }
        fn clone_flowfile(
            &mut self,
            _ff: &runifi_plugin_api::FlowFile,
        ) -> runifi_plugin_api::FlowFile {
            unimplemented!()
        }
        fn transfer(&mut self, _ff: runifi_plugin_api::FlowFile, _rel: &Relationship) {}
        fn remove(&mut self, _ff: runifi_plugin_api::FlowFile) {}
        fn penalize(&mut self, ff: runifi_plugin_api::FlowFile) -> runifi_plugin_api::FlowFile {
            ff
        }
        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    // --- Tests ---

    #[test]
    fn success_resets_consecutive_failures() {
        let mut sup = ProcessorSupervisor::new(Box::new(OkProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        let result = sup.invoke(&ctx, &mut session);
        assert!(matches!(result, InvocationResult::Success));
        assert_eq!(sup.consecutive_failures(), 0);
        assert_eq!(sup.total_invocations(), 1);
    }

    #[test]
    fn failure_increments_count() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        let result = sup.invoke(&ctx, &mut session);
        assert!(matches!(result, InvocationResult::Failed(_)));
        assert_eq!(sup.consecutive_failures(), 1);
        assert_eq!(sup.total_failures(), 1);
    }

    #[test]
    fn panic_is_caught() {
        let mut sup = ProcessorSupervisor::new(Box::new(PanicProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        let result = sup.invoke(&ctx, &mut session);
        assert!(matches!(result, InvocationResult::Panic(_)));
        if let InvocationResult::Panic(msg) = result {
            assert!(msg.contains("deliberate test panic"));
        }
        assert_eq!(sup.consecutive_failures(), 1);
    }

    #[test]
    fn circuit_breaker_trips_after_max_failures() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        for _ in 0..5 {
            sup.invoke(&ctx, &mut session);
        }

        assert!(sup.is_circuit_open());
        let result = sup.invoke(&ctx, &mut session);
        assert!(matches!(result, InvocationResult::Failed(_)));
    }

    #[test]
    fn circuit_breaker_reset() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        for _ in 0..5 {
            sup.invoke(&ctx, &mut session);
        }
        assert!(sup.is_circuit_open());

        sup.reset_circuit();
        assert!(!sup.is_circuit_open());
        assert_eq!(sup.consecutive_failures(), 0);
    }

    #[test]
    fn backoff_increases_exponentially() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        assert_eq!(sup.current_backoff(), Duration::ZERO);

        sup.invoke(&ctx, &mut session);
        assert_eq!(sup.current_backoff(), Duration::from_millis(200));

        sup.invoke(&ctx, &mut session);
        assert_eq!(sup.current_backoff(), Duration::from_millis(400));
    }

    #[test]
    fn backoff_capped_at_30s() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        // Trip through many failures to reach the cap.
        for _ in 0..20 {
            sup.invoke(&ctx, &mut session);
        }

        // Backoff should be capped at 30s.
        assert!(sup.current_backoff() <= Duration::from_millis(30_000));
    }

    #[test]
    fn success_after_failures_resets_count() {
        // Create a processor that fails first, then succeeds.
        struct ToggleProcessor {
            call_count: u32,
            fail_count: u32,
        }
        impl Processor for ToggleProcessor {
            fn on_trigger(
                &mut self,
                _ctx: &dyn ProcessContext,
                _session: &mut dyn ProcessSession,
            ) -> ProcessResult {
                self.call_count += 1;
                if self.call_count <= self.fail_count {
                    Err(PluginError::ProcessingFailed("test".into()))
                } else {
                    Ok(())
                }
            }
            fn relationships(&self) -> Vec<Relationship> {
                vec![]
            }
        }

        let proc = ToggleProcessor {
            call_count: 0,
            fail_count: 3,
        };
        let mut sup = ProcessorSupervisor::new(Box::new(proc));
        let ctx = TestContext;
        let mut session = NoOpSession;

        // 3 failures.
        for _ in 0..3 {
            sup.invoke(&ctx, &mut session);
        }
        assert_eq!(sup.consecutive_failures(), 3);
        assert_eq!(sup.total_failures(), 3);

        // Then success.
        sup.invoke(&ctx, &mut session);
        assert_eq!(sup.consecutive_failures(), 0);
        assert_eq!(sup.total_failures(), 3); // Total doesn't reset.
        assert_eq!(sup.total_invocations(), 4);
    }

    #[test]
    fn on_scheduled_delegates_to_processor() {
        let mut sup = ProcessorSupervisor::new(Box::new(OkProcessor));
        let ctx = TestContext;
        assert!(sup.on_scheduled(&ctx).is_ok());
    }

    #[test]
    fn relationships_delegates_to_processor() {
        let sup = ProcessorSupervisor::new(Box::new(OkProcessor));
        let rels = sup.relationships();
        assert!(rels.is_empty());
    }

    #[test]
    fn panic_with_string_message() {
        struct StringPanicProcessor;
        impl Processor for StringPanicProcessor {
            fn on_trigger(
                &mut self,
                _ctx: &dyn ProcessContext,
                _session: &mut dyn ProcessSession,
            ) -> ProcessResult {
                panic!("{}", "string panic".to_string());
            }
            fn relationships(&self) -> Vec<Relationship> {
                vec![]
            }
        }

        let mut sup = ProcessorSupervisor::new(Box::new(StringPanicProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        let result = sup.invoke(&ctx, &mut session);
        if let InvocationResult::Panic(msg) = result {
            assert!(msg.contains("string panic"));
        } else {
            panic!("Expected Panic result");
        }
    }

    #[test]
    fn circuit_stays_open_after_more_invocations() {
        let mut sup = ProcessorSupervisor::new(Box::new(FailProcessor));
        let ctx = TestContext;
        let mut session = NoOpSession;

        // Trip the circuit breaker.
        for _ in 0..5 {
            sup.invoke(&ctx, &mut session);
        }
        assert!(sup.is_circuit_open());

        // Subsequent invocations return circuit-open error without incrementing counters.
        let before_invocations = sup.total_invocations();
        let result = sup.invoke(&ctx, &mut session);
        assert!(matches!(result, InvocationResult::Failed(_)));
        // total_invocations should not change since the circuit was open.
        assert_eq!(sup.total_invocations(), before_invocations);
    }
}
