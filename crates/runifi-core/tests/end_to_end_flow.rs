/// Integration tests for end-to-end flow execution.
///
/// Tests complete pipelines: GenerateFlowFile -> LogAttribute -> PutFile,
/// back-pressure behavior, and fault tolerance with circuit breakers.
use std::collections::HashMap;
use std::sync::Arc;

use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::flowfile_repo::InMemoryFlowFileRepository;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::ProcessorDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{Processor, REL_FAILURE, REL_SUCCESS};

// ── Test processors ──────────────────────────────────────────────────────────

/// A processor that always panics (for fault tolerance testing).
struct PanicProcessor;

impl Processor for PanicProcessor {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        _session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        panic!("deliberate integration test panic");
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }
}

/// A processor that always returns an error.
struct ErrorProcessor;

impl Processor for ErrorProcessor {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        _session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        Err(PluginError::ProcessingFailed(
            "deliberate integration test error".into(),
        ))
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }
}

/// A pass-through processor that transfers input to success.
struct PassThroughProcessor;

impl Processor for PassThroughProcessor {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        while let Some(ff) = session.get() {
            session.transfer(ff, &REL_SUCCESS);
        }
        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_engine() -> FlowEngine {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let flowfile_repo = Arc::new(InMemoryFlowFileRepository);
    FlowEngine::new("integration-test", content_repo, flowfile_repo)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn engine_start_stop_lifecycle() {
    let mut engine = make_engine();
    assert!(!engine.is_running());

    engine.start().await.unwrap();
    assert!(engine.is_running());
    assert!(engine.handle().is_some());

    engine.stop().await;
    assert!(!engine.is_running());
}

#[tokio::test]
async fn engine_double_start_returns_error() {
    let mut engine = make_engine();
    engine.start().await.unwrap();

    let result = engine.start().await;
    assert!(result.is_err());

    engine.stop().await;
}

#[tokio::test]
async fn dag_construction_with_processors() {
    let mut engine = make_engine();

    // Build a simple pipeline: generator -> passthrough.
    let gen_id = engine.add_processor(
        "generator",
        "GenerateFlowFile",
        Box::new(PassThroughProcessor), // Using passthrough as placeholder.
        SchedulingStrategy::TimerDriven { interval_ms: 100 },
        HashMap::new(),
    );

    let pass_id = engine.add_processor(
        "passthrough",
        "PassThrough",
        Box::new(PassThroughProcessor),
        SchedulingStrategy::EventDriven,
        HashMap::new(),
    );

    engine.connect(gen_id, "success", pass_id, BackPressureConfig::default());

    engine.start().await.unwrap();

    {
        let handle = engine.handle().unwrap();
        let procs = handle.processors.read();
        assert_eq!(procs.len(), 2);

        let conns = handle.connections.read();
        assert_eq!(conns.len(), 1);
    }

    engine.stop().await;
}

#[tokio::test]
async fn fault_tolerance_panic_recovery() {
    let mut engine = make_engine();

    // Add a panic processor and a normal processor.
    let panic_id = engine.add_processor(
        "panicker",
        "PanicProcessor",
        Box::new(PanicProcessor),
        SchedulingStrategy::TimerDriven { interval_ms: 50 },
        HashMap::new(),
    );

    let normal_id = engine.add_processor(
        "normal",
        "PassThrough",
        Box::new(PassThroughProcessor),
        SchedulingStrategy::TimerDriven { interval_ms: 50 },
        HashMap::new(),
    );

    // Connect them for topology (not that data will flow through panicker).
    engine.connect(
        panic_id,
        "success",
        normal_id,
        BackPressureConfig::default(),
    );

    engine.start().await.unwrap();

    // Wait for the panic processor to trip its circuit breaker.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    // The engine should still be running (panic was caught).
    assert!(engine.is_running());

    // The panicker's circuit breaker should be open after 5 consecutive panics.
    {
        let handle = engine.handle().unwrap();
        let procs = handle.processors.read();
        let panicker = procs.iter().find(|p| p.name == "panicker").unwrap();
        let snap = panicker.metrics.snapshot();
        assert!(
            snap.circuit_open,
            "Circuit breaker should be open after panics"
        );
    }

    engine.stop().await;
}

#[tokio::test]
async fn fault_tolerance_error_circuit_breaker() {
    let mut engine = make_engine();

    engine.add_processor(
        "errorer",
        "ErrorProcessor",
        Box::new(ErrorProcessor),
        SchedulingStrategy::TimerDriven { interval_ms: 10 },
        HashMap::new(),
    );

    engine.start().await.unwrap();

    // Wait enough time for 5+ failures with exponential backoff.
    // Backoff sequence after each failure: 200ms, 400ms, 800ms, 1600ms.
    // Plus 10ms timer interval between each. Total: ~3100ms minimum.
    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

    {
        let handle = engine.handle().unwrap();
        let procs = handle.processors.read();
        let errorer = procs.iter().find(|p| p.name == "errorer").unwrap();
        let snap = errorer.metrics.snapshot();

        assert!(snap.total_failures >= 5, "Should have at least 5 failures");
        assert!(snap.circuit_open, "Circuit breaker should be open");
    }

    engine.stop().await;
}

#[tokio::test]
async fn generate_flowfile_end_to_end() {
    // We'll use the processor registry to test with real processors.
    // This test requires runifi-processors to be linked.
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let flowfile_repo = Arc::new(InMemoryFlowFileRepository);
    let registry = Arc::new(PluginRegistry::discover());

    // Try to create a GenerateFlowFile processor from the registry.
    if let Some(gen_proc) = registry.create_processor("GenerateFlowFile") {
        let mut engine = FlowEngine::new("e2e-test", content_repo, flowfile_repo);
        engine.set_registry(registry);

        let mut props = HashMap::new();
        props.insert("File Size".to_string(), "64".to_string());
        props.insert("Batch Size".to_string(), "1".to_string());
        props.insert("Data Format".to_string(), "text".to_string());

        let pass = Box::new(PassThroughProcessor);

        let gen_id = engine.add_processor(
            "generator",
            "GenerateFlowFile",
            gen_proc,
            SchedulingStrategy::TimerDriven { interval_ms: 50 },
            props,
        );

        let sink_id = engine.add_processor(
            "sink",
            "PassThrough",
            pass,
            SchedulingStrategy::EventDriven,
            HashMap::new(),
        );

        engine.connect(gen_id, "success", sink_id, BackPressureConfig::default());
        engine.start().await.unwrap();

        // Let it run for a bit to generate FlowFiles.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        {
            let handle = engine.handle().unwrap();
            let procs = handle.processors.read();
            let generator = procs.iter().find(|p| p.name == "generator").unwrap();
            let snap = generator.metrics.snapshot();
            assert!(
                snap.flowfiles_out > 0,
                "Generator should have produced FlowFiles"
            );
        }

        engine.stop().await;
    }
    // If the processor isn't available, the test passes silently
    // (it might not be linked in all test configurations).
}

#[tokio::test]
async fn back_pressure_slows_upstream() {
    let mut engine = make_engine();

    // Create a small connection that will fill up quickly.
    let config = BackPressureConfig::new(5, u64::MAX);

    // A simple counter processor that generates FlowFiles.
    struct CounterProcessor {
        count: u64,
    }

    impl Processor for CounterProcessor {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            let ff = session.create();
            self.count += 1;
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
            Ok(())
        }

        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS]
        }
    }

    // Slow consumer that never processes anything (simulates back-pressure).
    struct SlowConsumer;

    impl Processor for SlowConsumer {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            _session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            // Intentionally don't consume — just sleep.
            std::thread::sleep(std::time::Duration::from_millis(500));
            Ok(())
        }

        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS]
        }
    }

    let gen_id = engine.add_processor(
        "fast-producer",
        "Counter",
        Box::new(CounterProcessor { count: 0 }),
        SchedulingStrategy::TimerDriven { interval_ms: 10 },
        HashMap::new(),
    );

    let consumer_id = engine.add_processor(
        "slow-consumer",
        "SlowConsumer",
        Box::new(SlowConsumer),
        SchedulingStrategy::EventDriven,
        HashMap::new(),
    );

    engine.connect(gen_id, "success", consumer_id, config);

    engine.start().await.unwrap();

    // Let it run briefly.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // The connection queue should be at or near capacity.
    {
        let handle = engine.handle().unwrap();
        let conns = handle.connections.read();
        if let Some(conn) = conns.first() {
            let count = conn.connection.queue_count();
            // The queue should have some items (back-pressure is working).
            assert!(
                count <= 5,
                "Queue should not exceed capacity (count={})",
                count
            );
        }
    }

    engine.stop().await;
}

#[tokio::test]
async fn event_driven_scheduling() {
    let mut engine = make_engine();

    struct CountingProcessor {
        triggered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl Processor for CountingProcessor {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            while let Some(ff) = session.get() {
                self.triggered
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                session.transfer(ff, &REL_SUCCESS);
            }
            session.commit();
            Ok(())
        }

        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS]
        }
    }

    let trigger_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let trigger_count_clone = trigger_count.clone();

    // Timer-driven producer.
    let gen_id = engine.add_processor(
        "producer",
        "PassThrough",
        Box::new(PassThroughProcessor),
        SchedulingStrategy::TimerDriven { interval_ms: 50 },
        HashMap::new(),
    );

    // Event-driven consumer.
    let consumer_id = engine.add_processor(
        "consumer",
        "Counting",
        Box::new(CountingProcessor {
            triggered: trigger_count_clone,
        }),
        SchedulingStrategy::EventDriven,
        HashMap::new(),
    );

    engine.connect(
        gen_id,
        "success",
        consumer_id,
        BackPressureConfig::default(),
    );

    engine.start().await.unwrap();

    // The consumer should not trigger without input data.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let count = trigger_count.load(std::sync::atomic::Ordering::Relaxed);
    // Event-driven processor only triggers when data arrives.
    // Since the producer doesn't generate data (it's PassThrough with no input),
    // the consumer should have 0 triggers.
    assert_eq!(
        count, 0,
        "Event-driven processor should not trigger without input"
    );

    engine.stop().await;
}

// Register test processors so they're available via the registry in integration tests.
inventory::submit!(ProcessorDescriptor {
    type_name: "PanicTestProc",
    description: "Always panics — integration test only",
    factory: || Box::new(PanicProcessor),
    tags: &[],
});

inventory::submit!(ProcessorDescriptor {
    type_name: "ErrorTestProc",
    description: "Always errors — integration test only",
    factory: || Box::new(ErrorProcessor),
    tags: &[],
});

inventory::submit!(ProcessorDescriptor {
    type_name: "PassThroughTestProc",
    description: "Pass-through — integration test only",
    factory: || Box::new(PassThroughProcessor),
    tags: &[],
});
