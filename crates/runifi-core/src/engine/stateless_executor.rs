use runifi_plugin_api::Processor;
use runifi_plugin_api::context::ProcessContext;

use crate::session::group_session::GroupSession;

/// Configuration for stateless execution.
#[derive(Default)]
pub struct StatelessConfig {
    /// Maximum retry attempts before routing to failure.
    pub max_retries: u32,
}

/// Result of a stateless group execution.
pub enum StatelessResult {
    /// All processors in the group completed successfully.
    Success,
    /// A processor in the group failed after exhausting retries.
    Failed {
        processor_name: String,
        error: String,
    },
}

/// Executes all processors in a stateless process group as a single transaction.
///
/// On success, all sessions commit atomically. On any failure, everything
/// rolls back and source FlowFiles return to input connections.
pub struct StatelessExecutor {
    config: StatelessConfig,
}

impl StatelessExecutor {
    pub fn new(config: StatelessConfig) -> Self {
        Self { config }
    }

    /// Execute a sequence of processors as a single transaction.
    ///
    /// Each processor's transfers are fed into the internal queue for the next
    /// processor. On success, the group session is committed atomically.
    /// On failure or panic, the group session is rolled back.
    pub fn execute(
        &self,
        processors: &mut [(String, &mut dyn Processor, &dyn ProcessContext)],
        session: &mut GroupSession,
    ) -> StatelessResult {
        for (name, processor, context) in processors.iter_mut() {
            let mut attempts = 0;
            loop {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    processor.on_trigger(*context, session)
                }));

                match result {
                    Ok(Ok(())) => {
                        // Move transfers to internal queue for next processor.
                        let transfers = session.take_transfers();
                        let flowfiles: Vec<_> = transfers.into_iter().map(|(ff, _)| ff).collect();
                        session.feed_internal(flowfiles);
                        break;
                    }
                    Ok(Err(e)) => {
                        attempts += 1;
                        if attempts > self.config.max_retries {
                            tracing::error!(
                                processor = %name,
                                error = %e,
                                attempts,
                                "Stateless execution failed"
                            );
                            session.group_rollback();
                            return StatelessResult::Failed {
                                processor_name: name.clone(),
                                error: e.to_string(),
                            };
                        }
                        tracing::warn!(
                            processor = %name,
                            error = %e,
                            attempt = attempts,
                            max_retries = self.config.max_retries,
                            "Retrying processor in stateless group"
                        );
                    }
                    Err(panic_info) => {
                        let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        tracing::error!(
                            processor = %name,
                            error = %msg,
                            "Processor panicked in stateless group"
                        );
                        session.group_rollback();
                        return StatelessResult::Failed {
                            processor_name: name.clone(),
                            error: format!("panic: {msg}"),
                        };
                    }
                }
            }
        }

        // All processors succeeded — commit the group transaction.
        session.group_commit();
        StatelessResult::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use runifi_plugin_api::context::ProcessContext;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::relationship::Relationship;
    use runifi_plugin_api::result::ProcessResult;
    use runifi_plugin_api::session::ProcessSession;
    use runifi_plugin_api::{FlowFile, REL_SUCCESS};

    use crate::connection::back_pressure::BackPressureConfig;
    use crate::connection::flow_connection::FlowConnection;
    use crate::id::IdGenerator;
    use crate::repository::content_memory::InMemoryContentRepository;

    // ── Test helpers ────────────────────────────────────────────

    struct TestContext {
        name: String,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, _name: &str) -> PropertyValue {
            PropertyValue::Unset
        }
        fn name(&self) -> &str {
            &self.name
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    /// A processor that reads a FlowFile and transfers it to success.
    struct PassThroughProcessor;

    impl Processor for PassThroughProcessor {
        fn on_trigger(
            &mut self,
            _context: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            if let Some(ff) = session.get() {
                session.transfer(ff, &REL_SUCCESS);
            }
            session.commit();
            Ok(())
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS.clone()]
        }
    }

    /// A processor that creates a FlowFile with content and transfers it.
    struct GeneratorProcessor {
        data: Bytes,
    }

    impl Processor for GeneratorProcessor {
        fn on_trigger(
            &mut self,
            _context: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            let ff = session.create();
            let ff = session.write_content(ff, self.data.clone())?;
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
            Ok(())
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS.clone()]
        }
    }

    /// A processor that always fails.
    struct FailingProcessor;

    impl Processor for FailingProcessor {
        fn on_trigger(
            &mut self,
            _context: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            Err(runifi_plugin_api::PluginError::ProcessingFailed(
                "intentional failure".to_string(),
            ))
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS.clone()]
        }
    }

    /// A processor that panics.
    struct PanickingProcessor;

    impl Processor for PanickingProcessor {
        fn on_trigger(
            &mut self,
            _context: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            panic!("intentional panic");
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS.clone()]
        }
    }

    /// A processor that fails N times then succeeds.
    struct EventuallySucceedsProcessor {
        failures_remaining: u32,
    }

    impl Processor for EventuallySucceedsProcessor {
        fn on_trigger(
            &mut self,
            _context: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            if self.failures_remaining > 0 {
                self.failures_remaining -= 1;
                return Err(runifi_plugin_api::PluginError::ProcessingFailed(
                    "transient failure".to_string(),
                ));
            }
            if let Some(ff) = session.get() {
                session.transfer(ff, &REL_SUCCESS);
            }
            session.commit();
            Ok(())
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS.clone()]
        }
    }

    fn make_flowfile(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    fn make_group_session(
        input_connections: Vec<Arc<FlowConnection>>,
    ) -> (GroupSession, Arc<InMemoryContentRepository>) {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let session = GroupSession::new(content_repo.clone(), id_gen, input_connections);
        (session, content_repo)
    }

    // ── Tests ───────────────────────────────────────────────────

    #[test]
    fn successful_two_processor_chain() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("proc-1");
        let ctx2 = TestContext::new("proc-2");
        let mut proc1 = PassThroughProcessor;
        let mut proc2 = PassThroughProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("proc-1".to_string(), &mut proc1, &ctx1),
            ("proc-2".to_string(), &mut proc2, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Success));
        assert!(session.is_committed());
        // Input connection should be empty (FlowFile consumed).
        assert_eq!(conn.count(), 0);
    }

    #[test]
    fn failure_rolls_back_everything() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("proc-1");
        let ctx2 = TestContext::new("proc-2");
        let mut proc1 = PassThroughProcessor;
        let mut proc2 = FailingProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("proc-1".to_string(), &mut proc1, &ctx1),
            ("proc-2".to_string(), &mut proc2, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Failed { .. }));
        assert!(!session.is_committed());
        // FlowFile should be returned to input connection.
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn panic_rolls_back_everything() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("proc-1");
        let ctx2 = TestContext::new("proc-2");
        let mut proc1 = PassThroughProcessor;
        let mut proc2 = PanickingProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("proc-1".to_string(), &mut proc1, &ctx1),
            ("proc-2".to_string(), &mut proc2, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Failed { .. }));
        if let StatelessResult::Failed { error, .. } = &result {
            assert!(error.contains("panic"));
        }
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn retry_succeeds_within_limit() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig { max_retries: 2 });

        let ctx = TestContext::new("proc-1");
        let mut proc1 = EventuallySucceedsProcessor {
            failures_remaining: 2,
        };

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> =
            vec![("proc-1".to_string(), &mut proc1, &ctx)];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Success));
        assert!(session.is_committed());
    }

    #[test]
    fn retry_exhausted_rolls_back() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig { max_retries: 1 });

        let ctx = TestContext::new("proc-1");
        let mut proc1 = EventuallySucceedsProcessor {
            failures_remaining: 3,
        };

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> =
            vec![("proc-1".to_string(), &mut proc1, &ctx)];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Failed { .. }));
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn content_created_by_first_processor_available_to_second() {
        let (mut session, _repo) = make_group_session(vec![]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("generator");
        let ctx2 = TestContext::new("reader");
        let mut generator = GeneratorProcessor {
            data: Bytes::from_static(b"test data"),
        };
        let mut reader = PassThroughProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("generator".to_string(), &mut generator, &ctx1),
            ("reader".to_string(), &mut reader, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Success));
    }

    #[test]
    fn failure_cleans_up_created_content() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("generator");
        let ctx2 = TestContext::new("failer");
        let mut generator = GeneratorProcessor {
            data: Bytes::from_static(b"will be rolled back"),
        };
        let mut failer = FailingProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("generator".to_string(), &mut generator, &ctx1),
            ("failer".to_string(), &mut failer, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Failed { .. }));
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn empty_processor_chain_succeeds() {
        let (mut session, _repo) = make_group_session(vec![]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Success));
        assert!(session.is_committed());
    }

    #[test]
    fn first_processor_failure_rolls_back() {
        let conn = Arc::new(FlowConnection::new("input", BackPressureConfig::default()));
        conn.try_send(make_flowfile(1)).unwrap();

        let (mut session, _repo) = make_group_session(vec![conn.clone()]);
        let executor = StatelessExecutor::new(StatelessConfig::default());

        let ctx1 = TestContext::new("failer");
        let ctx2 = TestContext::new("never-reached");
        let mut failer = FailingProcessor;
        let mut passthrough = PassThroughProcessor;

        let mut processors: Vec<(String, &mut dyn Processor, &dyn ProcessContext)> = vec![
            ("failer".to_string(), &mut failer, &ctx1),
            ("never-reached".to_string(), &mut passthrough, &ctx2),
        ];

        let result = executor.execute(&mut processors, &mut session);
        assert!(matches!(result, StatelessResult::Failed { .. }));
        if let StatelessResult::Failed { processor_name, .. } = &result {
            assert_eq!(processor_name, "failer");
        }
        assert_eq!(conn.count(), 1);
    }
}
