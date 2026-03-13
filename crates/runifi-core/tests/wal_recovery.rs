/// Integration test: WAL-based FlowFile recovery across engine restarts.
///
/// 1. Build engine with WAL repo → GenerateFlowFile → LogAttribute (stopped).
/// 2. Start engine, let GenerateFlowFile produce FlowFiles into the connection queue.
/// 3. Stop engine (triggers final checkpoint).
/// 4. Build a NEW engine with the same WAL directory.
/// 5. Start the new engine → verify FlowFiles are restored to the connection queue.
use std::collections::HashMap;
use std::sync::Arc;

use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::flowfile_wal::{
    FsyncMode, WalFlowFileRepoConfig, WalFlowFileRepository,
};

// Link in the built-in processors (GenerateFlowFile, LogAttribute, etc.).
extern crate runifi_processors;

fn wal_repo(dir: &std::path::Path) -> Arc<WalFlowFileRepository> {
    Arc::new(
        WalFlowFileRepository::new(WalFlowFileRepoConfig {
            dir: dir.to_path_buf(),
            fsync_mode: FsyncMode::Always,
            checkpoint_interval_secs: 3600, // no auto-checkpoint during test
        })
        .expect("WAL repo creation failed"),
    )
}

fn build_engine(
    content_repo: Arc<InMemoryContentRepository>,
    wal_repo: Arc<WalFlowFileRepository>,
    registry: &Arc<PluginRegistry>,
) -> FlowEngine {
    let mut engine = FlowEngine::new("wal-test-flow", content_repo, wal_repo);
    engine.set_registry(registry.clone());

    let gen_props = {
        let mut p = HashMap::new();
        p.insert("File Size".to_string(), "64".to_string());
        p
    };

    // GenerateFlowFile → (success) → LogAttribute (stopped, so FlowFiles queue up).
    let gen_id = engine.add_processor(
        "generate",
        "GenerateFlowFile",
        registry.create_processor("GenerateFlowFile").unwrap(),
        SchedulingStrategy::TimerDriven { interval_ms: 50 },
        gen_props,
    );

    let log_id = engine.add_processor(
        "log",
        "LogAttribute",
        registry.create_processor("LogAttribute").unwrap(),
        SchedulingStrategy::EventDriven,
        HashMap::new(),
    );

    engine.connect(gen_id, "success", log_id, BackPressureConfig::default());

    engine
}

#[tokio::test]
async fn engine_restart_recovers_flowfiles_from_wal() {
    let wal_dir = tempfile::TempDir::new().unwrap();
    let registry = Arc::new(PluginRegistry::discover());

    // ── Phase 1: Start engine, generate some FlowFiles ─────────────────
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let repo1 = wal_repo(wal_dir.path());
    let mut engine1 = build_engine(content_repo.clone(), repo1, &registry);

    engine1.start().await.expect("engine1 start failed");

    // Stop LogAttribute so FlowFiles accumulate in the connection queue
    // instead of being consumed immediately.
    let handle1 = engine1.handle().expect("handle must exist");
    handle1.stop_processor("log");

    // Let GenerateFlowFile run for a bit — it produces one FlowFile per 50ms.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;

    // Check that FlowFiles have accumulated in the connection queue.
    let conns1 = handle1.connections.read();
    assert!(!conns1.is_empty(), "should have at least one connection");
    let queued_before_stop = conns1[0].connection.queue_count();
    assert!(queued_before_stop > 0, "expected FlowFiles in queue, got 0");
    let queued_ids_before: Vec<u64> = conns1[0]
        .connection
        .queue_snapshot(0, 1000)
        .iter()
        .map(|s| s.id)
        .collect();
    drop(conns1);

    // Stop engine (triggers final checkpoint).
    engine1.stop().await;

    // ── Phase 2: Rebuild engine from same WAL dir ──────────────────────
    let content_repo2 = Arc::new(InMemoryContentRepository::new());
    let repo2 = wal_repo(wal_dir.path());
    let mut engine2 = build_engine(content_repo2, repo2, &registry);

    engine2.start().await.expect("engine2 start failed");

    // Verify FlowFiles were restored to the connection queue.
    let handle2 = engine2.handle().expect("handle must exist");
    let conns2 = handle2.connections.read();
    assert!(!conns2.is_empty());
    let queued_after_recovery = conns2[0].connection.queue_count();

    // The recovered count should be >= what we had before stop.
    // (Could be slightly more if GenerateFlowFile produced one more before shutdown.)
    assert!(
        queued_after_recovery >= queued_before_stop,
        "expected at least {} recovered FlowFiles, got {}",
        queued_before_stop,
        queued_after_recovery
    );

    // Verify the same FlowFile IDs are present.
    let queued_ids_after: Vec<u64> = conns2[0]
        .connection
        .queue_snapshot(0, 1000)
        .iter()
        .map(|s| s.id)
        .collect();
    for id in &queued_ids_before {
        assert!(
            queued_ids_after.contains(id),
            "FlowFile {} was lost during recovery",
            id
        );
    }

    drop(conns2);
    engine2.stop().await;
}
