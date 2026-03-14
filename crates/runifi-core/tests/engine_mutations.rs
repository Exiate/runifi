/// Integration tests for runtime engine mutations (hot-add/remove processors and connections).
///
/// These tests use minimal mock processors registered via `inventory::submit!`
/// so the plugin registry can instantiate them during hot-add calls.
use std::collections::HashMap;
use std::sync::Arc;

use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::handle::ConfigUpdateError;
use runifi_core::engine::mutation::MutationError;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::flowfile_repo::InMemoryFlowFileRepository;
use runifi_plugin_api::{
    ProcessorDescriptor, PropertyDescriptor, Relationship, context::ProcessContext,
    session::ProcessSession,
};

// ── Minimal test processors ───────────────────────────────────────────────────

struct NoOpProcessor;

impl runifi_plugin_api::Processor for NoOpProcessor {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        _session: &mut dyn ProcessSession,
    ) -> runifi_plugin_api::result::ProcessResult {
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![Relationship {
            name: "success",
            description: "Success",
            auto_terminated: false,
        }]
    }
}

inventory::submit!(ProcessorDescriptor {
    type_name: "NoOp",
    description: "Does nothing — used in tests.",
    factory: || Box::new(NoOpProcessor),
    tags: &[],
});

/// A processor with a required property (no default) for validation tests.
struct RequiredPropProcessor;

impl runifi_plugin_api::Processor for RequiredPropProcessor {
    fn on_trigger(
        &mut self,
        _ctx: &dyn ProcessContext,
        _session: &mut dyn ProcessSession,
    ) -> runifi_plugin_api::result::ProcessResult {
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![Relationship {
            name: "success",
            description: "Success",
            auto_terminated: false,
        }]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("Directory", "Target directory").required(),
            PropertyDescriptor::new("Optional Prop", "An optional property"),
            PropertyDescriptor::new("With Default", "Has a default value")
                .required()
                .default_value("/tmp"),
        ]
    }
}

inventory::submit!(ProcessorDescriptor {
    type_name: "RequiredPropTest",
    description: "Processor with required properties — used in validation tests.",
    factory: || Box::new(RequiredPropProcessor),
    tags: &["Testing"],
});

// ── Test helpers ──────────────────────────────────────────────────────────────

async fn start_empty_engine() -> FlowEngine {
    let content_repo = Arc::new(InMemoryContentRepository::new());
    let flowfile_repo = Arc::new(InMemoryFlowFileRepository);
    let registry = Arc::new(PluginRegistry::discover());
    let mut engine = FlowEngine::new("test-flow", content_repo, flowfile_repo);
    engine.set_registry(registry);
    engine.start().await.expect("engine start failed");
    engine
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn hot_add_processor_appears_in_handle() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    assert_eq!(handle.processors.read().len(), 0);

    handle
        .add_processor(
            "proc1".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    let procs = handle.processors.read();
    assert_eq!(procs.len(), 1);
    assert_eq!(procs[0].name, "proc1");
    assert_eq!(procs[0].type_name, "NoOp");
    // New processors must start STOPPED.
    assert!(
        !procs[0]
            .metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed),
        "new processor should start STOPPED"
    );
}

#[tokio::test]
async fn hot_add_duplicate_name_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "p".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("first add should succeed");

    let result = handle
        .add_processor(
            "p".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::DuplicateName(_)
    ));
}

#[tokio::test]
async fn hot_add_unknown_type_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    let result = handle
        .add_processor(
            "p".to_string(),
            "Nonexistent".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MutationError::UnknownType(_)));
}

#[tokio::test]
async fn hot_remove_stopped_processor_succeeds() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "p".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add failed");

    assert_eq!(handle.processors.read().len(), 1);

    handle
        .remove_processor("p".to_string())
        .await
        .expect("remove_processor failed");

    assert_eq!(handle.processors.read().len(), 0);
}

#[tokio::test]
async fn hot_remove_running_processor_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "p".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add failed");

    handle.start_processor("p").expect("start_processor failed");

    let result = handle.remove_processor("p".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::ProcessorNotStopped
    ));
}

#[tokio::test]
async fn hot_add_connection_appears_in_handle() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");
    handle
        .add_processor(
            "dst".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "event".to_string(),
            0,
        )
        .await
        .expect("add dst");

    let conn_id = handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "dst".to_string(),
            BackPressureConfig::default(),
            None,
        )
        .await
        .expect("add_connection failed");

    assert!(!conn_id.is_empty());

    let conns = handle.connections.read();
    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0].id, conn_id);
    assert_eq!(conns[0].source_name, "src");
    assert_eq!(conns[0].relationship, "success");
    assert_eq!(conns[0].dest_name, "dst");
}

#[tokio::test]
async fn hot_add_connection_invalid_relationship_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");
    handle
        .add_processor(
            "dst".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "event".to_string(),
            0,
        )
        .await
        .expect("add dst");

    let result = handle
        .add_connection(
            "src".to_string(),
            "nonexistent-rel".to_string(),
            "dst".to_string(),
            BackPressureConfig::default(),
            None,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::UnknownRelationship(_, _)
    ));
}

#[tokio::test]
async fn hot_add_connection_missing_destination_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");

    let result = handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "missing-dst".to_string(),
            BackPressureConfig::default(),
            None,
        )
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::ProcessorNotFound(_)
    ));
}

#[tokio::test]
async fn duplicate_connection_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");
    handle
        .add_processor(
            "dst".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "event".to_string(),
            0,
        )
        .await
        .expect("add dst");

    let bp = BackPressureConfig::default();
    handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "dst".to_string(),
            bp,
            None,
        )
        .await
        .expect("first add");

    let result = handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "dst".to_string(),
            bp,
            None,
        )
        .await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::DuplicateConnection(_, _, _)
    ));
}

#[tokio::test]
async fn hot_remove_connection_succeeds() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");
    handle
        .add_processor(
            "dst".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "event".to_string(),
            0,
        )
        .await
        .expect("add dst");

    let conn_id = handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "dst".to_string(),
            BackPressureConfig::default(),
            None,
        )
        .await
        .expect("add connection");

    assert_eq!(handle.connections.read().len(), 1);

    handle
        .remove_connection(conn_id, false)
        .await
        .expect("remove failed");
    assert_eq!(handle.connections.read().len(), 0);
}

#[tokio::test]
async fn hot_remove_processor_blocked_by_connection() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    handle
        .add_processor(
            "src".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add src");
    handle
        .add_processor(
            "dst".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "event".to_string(),
            0,
        )
        .await
        .expect("add dst");
    handle
        .add_connection(
            "src".to_string(),
            "success".to_string(),
            "dst".to_string(),
            BackPressureConfig::default(),
            None,
        )
        .await
        .expect("add conn");

    let result = handle.remove_processor("src".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::ProcessorHasConnections(_)
    ));
}

#[tokio::test]
async fn position_set_and_get() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    assert!(handle.get_position("p").is_none());

    handle.set_position("p", 42.5, 100.0);
    let pos = handle.get_position("p").expect("position should exist");
    assert!((pos.x - 42.5).abs() < f64::EPSILON);
    assert!((pos.y - 100.0).abs() < f64::EPSILON);

    handle.set_position("p", 0.0, 0.0);
    let pos2 = handle.get_position("p").unwrap();
    assert!((pos2.x).abs() < f64::EPSILON);
}

#[tokio::test]
async fn multi_processor_topology_visible() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    for i in 0..5 {
        handle
            .add_processor(
                format!("proc-{}", i),
                "NoOp".to_string(),
                HashMap::new(),
                "timer".to_string(),
                1000,
            )
            .await
            .expect("add failed");
    }

    assert_eq!(handle.processors.read().len(), 5);
}

#[tokio::test]
async fn remove_nonexistent_processor_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    let result = handle.remove_processor("nonexistent".to_string()).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::ProcessorNotFound(_)
    ));
}

#[tokio::test]
async fn remove_nonexistent_connection_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    let result = handle
        .remove_connection("nonexistent-conn".to_string(), false)
        .await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MutationError::ConnectionNotFound(_)
    ));
}

// ── start_processor validation tests ─────────────────────────────────────────

#[tokio::test]
async fn start_processor_missing_required_property_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    // Add a processor with required properties but don't supply the required one.
    handle
        .add_processor(
            "needs-props".to_string(),
            "RequiredPropTest".to_string(),
            HashMap::new(), // No properties supplied
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Starting should fail because "Directory" is required and has no default.
    let result = handle.start_processor("needs-props");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConfigUpdateError::ValidationError(_)
    ));

    // Processor should remain stopped.
    let procs = handle.processors.read();
    let proc = procs.iter().find(|p| p.name == "needs-props").unwrap();
    assert!(
        !proc
            .metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed),
        "processor should remain STOPPED after failed start"
    );
}

#[tokio::test]
async fn start_processor_with_all_required_properties_succeeds() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    let mut props = HashMap::new();
    props.insert("Directory".to_string(), "/tmp/test".to_string());
    // "With Default" is required but has a default, so it's not needed.
    // "Optional Prop" is not required, so it's not needed.

    handle
        .add_processor(
            "has-props".to_string(),
            "RequiredPropTest".to_string(),
            props,
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    // Starting should succeed because "Directory" is supplied.
    let result = handle.start_processor("has-props");
    assert!(result.is_ok());

    // Processor should be enabled.
    let procs = handle.processors.read();
    let proc = procs.iter().find(|p| p.name == "has-props").unwrap();
    assert!(
        proc.metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed),
        "processor should be RUNNING after successful start"
    );
}

#[tokio::test]
async fn start_processor_not_found_returns_error() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    let result = handle.start_processor("nonexistent");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ConfigUpdateError::NotFound(_)
    ));
}

#[tokio::test]
async fn start_processor_no_required_properties_succeeds() {
    let engine = start_empty_engine().await;
    let handle = engine.handle().expect("handle must exist").clone();

    // NoOp has no property descriptors at all.
    handle
        .add_processor(
            "noop".to_string(),
            "NoOp".to_string(),
            HashMap::new(),
            "timer".to_string(),
            1000,
        )
        .await
        .expect("add_processor failed");

    let result = handle.start_processor("noop");
    assert!(result.is_ok());
}
