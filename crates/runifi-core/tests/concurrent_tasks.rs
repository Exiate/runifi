/// Integration tests for per-processor concurrent tasks.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::flowfile_repo::InMemoryFlowFileRepository;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{InputRequirement, ProcessorDescriptor};
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{Processor, REL_SUCCESS};

// ── Test processor that counts triggers across all concurrent tasks ──────────

struct ConcurrentCounter {
    shared_count: Arc<AtomicU64>,
}

impl Processor for ConcurrentCounter {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        self.shared_count.fetch_add(1, Ordering::Relaxed);
        let ff = session.create();
        session.transfer(ff, &REL_SUCCESS);
        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS]
    }
}

// We need a factory-registered processor for the mutation handler to instantiate
// siblings. The shared counter is set via a global for test purposes.
static GLOBAL_COUNTER: std::sync::OnceLock<Arc<AtomicU64>> = std::sync::OnceLock::new();

struct RegistryConcurrentCounter;

impl Processor for RegistryConcurrentCounter {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        if let Some(counter) = GLOBAL_COUNTER.get() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let ff = session.create();
        session.transfer(ff, &REL_SUCCESS);
        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS]
    }
}

inventory::submit!(ProcessorDescriptor {
    type_name: "ConcurrentTestProc",
    description: "Counts triggers via global atomic — concurrent task test only",
    factory: || Box::new(RegistryConcurrentCounter),
    tags: &[],
    input_requirement: InputRequirement::Allowed,
    trigger_when_empty: false,
    side_effect_free: false,
    supports_batching: false,
});

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_engine_with_registry() -> FlowEngine {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let flowfile_repo = Arc::new(InMemoryFlowFileRepository);
    let registry = Arc::new(PluginRegistry::discover());
    let mut engine = FlowEngine::new("concurrent-test", content_repo, flowfile_repo);
    engine.set_registry(registry);
    engine
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn concurrent_tasks_clamp_accepts_up_to_64() {
    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    handle
        .add_processor(
            "clamped".to_string(),
            "ConcurrentTestProc".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Setting concurrent_tasks to 4 should be accepted (not clamped to 1).
    handle
        .update_processor_config("clamped", None, None, None, None, Some(4), None, None)
        .expect("config update failed");

    let procs = handle.processors.read();
    let info = procs.iter().find(|p| p.name == "clamped").unwrap();
    assert_eq!(info.concurrent_tasks.load(Ordering::Relaxed), 4);

    drop(procs);
    engine.stop().await;
}

#[tokio::test]
async fn concurrent_tasks_clamp_enforces_max_64() {
    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    handle
        .add_processor(
            "overclamped".to_string(),
            "ConcurrentTestProc".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Setting concurrent_tasks to 100 should be clamped to 64.
    handle
        .update_processor_config("overclamped", None, None, None, None, Some(100), None, None)
        .expect("config update failed");

    let procs = handle.processors.read();
    let info = procs.iter().find(|p| p.name == "overclamped").unwrap();
    assert_eq!(info.concurrent_tasks.load(Ordering::Relaxed), 64);

    drop(procs);
    engine.stop().await;
}

#[tokio::test]
async fn concurrent_tasks_clamp_enforces_min_1() {
    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    handle
        .add_processor(
            "underclamped".to_string(),
            "ConcurrentTestProc".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Setting concurrent_tasks to 0 should be clamped to 1.
    handle
        .update_processor_config("underclamped", None, None, None, None, Some(0), None, None)
        .expect("config update failed");

    let procs = handle.processors.read();
    let info = procs.iter().find(|p| p.name == "underclamped").unwrap();
    assert_eq!(info.concurrent_tasks.load(Ordering::Relaxed), 1);

    drop(procs);
    engine.stop().await;
}

#[tokio::test]
async fn spawn_concurrent_tasks_creates_sibling_tasks() {
    let counter = Arc::new(AtomicU64::new(0));
    // Initialize the global counter (only works once per process, but test
    // isolation via cargo test forks means this is fine).
    let _ = GLOBAL_COUNTER.set(counter.clone());

    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    handle
        .add_processor(
            "multi".to_string(),
            "ConcurrentTestProc".to_string(),
            HashMap::new(),
            "timer".to_string(),
            100,
        )
        .await
        .expect("add_processor failed");

    // Set concurrent_tasks to 4 before starting.
    handle
        .update_processor_config("multi", None, None, None, None, Some(4), None, None)
        .expect("config update failed");

    // Start the processor (which will spawn concurrent tasks via mutation channel).
    handle.start_processor("multi").expect("start failed");

    // Give the concurrent tasks time to run.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    let count = counter.load(Ordering::Relaxed);
    // With 4 concurrent tasks on a 100ms timer, we expect more triggers than
    // a single task would produce. A single task in 800ms would get ~8 triggers.
    // With shared timer coordination (notify_waiters), all 4 tasks wake each
    // interval, so we expect roughly 4x as many triggers.
    assert!(
        count >= 20,
        "Expected at least 20 triggers with 4 concurrent tasks on 100ms timer in 800ms, got {}",
        count
    );

    engine.stop().await;
}

#[tokio::test]
async fn spawn_concurrent_tasks_method_works() {
    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    handle
        .add_processor(
            "spawn-test".to_string(),
            "ConcurrentTestProc".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Direct spawn_concurrent_tasks call should succeed.
    let result = handle.spawn_concurrent_tasks("spawn-test", 3).await;
    assert!(result.is_ok(), "spawn_concurrent_tasks should succeed");

    engine.stop().await;
}

#[tokio::test]
async fn spawn_concurrent_tasks_unknown_processor_fails() {
    let mut engine = make_engine_with_registry();
    engine.start().await.unwrap();
    let handle = engine.handle().unwrap();

    let result = handle.spawn_concurrent_tasks("nonexistent", 3).await;
    assert!(
        result.is_err(),
        "spawn_concurrent_tasks for unknown processor should fail"
    );

    engine.stop().await;
}

#[tokio::test]
async fn concurrent_tasks_default_is_one() {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let flowfile_repo = Arc::new(InMemoryFlowFileRepository);
    let registry = Arc::new(PluginRegistry::discover());

    let counter = Box::new(ConcurrentCounter {
        shared_count: Arc::new(AtomicU64::new(0)),
    });

    let mut engine = FlowEngine::new("default-test", content_repo, flowfile_repo);
    engine.set_registry(registry);

    engine.add_processor(
        "single",
        "ConcurrentTestProc",
        counter,
        SchedulingStrategy::TimerDriven { interval_ms: 100 },
        HashMap::new(),
    );

    engine.start().await.unwrap();

    let handle = engine.handle().unwrap();
    let procs = handle.processors.read();
    let info = procs.iter().find(|p| p.name == "single").unwrap();
    assert_eq!(
        info.concurrent_tasks.load(Ordering::Relaxed),
        1,
        "Default concurrent_tasks should be 1"
    );

    drop(procs);
    engine.stop().await;
}

#[tokio::test]
async fn event_driven_concurrent_tasks_share_input_connections() {
    let mut engine = make_engine_with_registry();

    // Create a simple producer -> consumer pipeline.
    struct SimpleProducer;
    impl Processor for SimpleProducer {
        fn on_trigger(
            &mut self,
            _ctx: &dyn ProcessContext,
            session: &mut dyn ProcessSession,
        ) -> ProcessResult {
            let ff = session.create();
            session.transfer(ff, &REL_SUCCESS);
            session.commit();
            Ok(())
        }
        fn relationships(&self) -> Vec<Relationship> {
            vec![REL_SUCCESS]
        }
    }

    let producer_id = engine.add_processor(
        "producer",
        "SimpleProducer",
        Box::new(SimpleProducer),
        SchedulingStrategy::TimerDriven { interval_ms: 50 },
        HashMap::new(),
    );

    let consumer_id = engine.add_processor(
        "consumer",
        "ConcurrentTestProc",
        Box::new(RegistryConcurrentCounter),
        SchedulingStrategy::EventDriven,
        HashMap::new(),
    );

    engine.connect(
        producer_id,
        "success",
        consumer_id,
        BackPressureConfig::default(),
    );

    engine.start().await.unwrap();

    let handle = engine.handle().unwrap();

    // Stop the consumer so we can update its config.
    handle.stop_processor("consumer");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Set concurrent_tasks on the consumer and spawn siblings.
    handle
        .update_processor_config("consumer", None, None, None, None, Some(3), None, None)
        .expect("config update failed");
    handle
        .spawn_concurrent_tasks("consumer", 3)
        .await
        .expect("spawn failed");

    // Restart the consumer.
    handle.start_processor("consumer").expect("start failed");

    // Let it run.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The consumer's metrics should show FlowFiles processed.
    let procs = handle.processors.read();
    let consumer = procs.iter().find(|p| p.name == "consumer").unwrap();
    let snap = consumer.metrics.snapshot();
    // With event-driven concurrent tasks sharing the input queue via crossbeam MPMC,
    // they naturally compete for FlowFiles. We just verify data flows.
    assert!(
        snap.total_invocations > 0,
        "Consumer should have been triggered"
    );

    drop(procs);
    engine.stop().await;
}
