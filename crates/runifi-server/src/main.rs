use std::sync::Arc;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use runifi_core::config::flow_config::FlowConfig;
use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::handle::{PluginKind, PluginTypeInfo};
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_file::{FileContentRepoConfig, FileContentRepository};
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;
use runifi_core::repository::flowfile_repo::{FlowFileRepository, InMemoryFlowFileRepository};
use runifi_core::repository::flowfile_wal::{
    FsyncMode, WalFlowFileRepoConfig, WalFlowFileRepository,
};

// Ensure processor registrations are linked in.
extern crate runifi_processors;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("RuniFi v{} starting...", env!("CARGO_PKG_VERSION"));

    // Discover all registered plugins.
    let registry = PluginRegistry::discover();
    tracing::info!(
        processors = ?registry.processor_types(),
        sources = ?registry.source_types(),
        sinks = ?registry.sink_types(),
        "Plugin registry initialized"
    );

    // Load flow configuration.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/flow.toml".to_string());

    let config_str = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(path = %config_path, error = %e, "No config file found, running idle");
            wait_for_shutdown().await;
            return Ok(());
        }
    };

    let config: FlowConfig =
        toml::from_str(&config_str).context("Failed to parse flow configuration")?;

    tracing::info!(flow = %config.flow.name, "Loaded flow configuration");

    // Create content repository based on config.
    let content_repo: Arc<dyn ContentRepository> =
        match config.engine.content_repository.repo_type.as_str() {
            "file" => {
                let file_config = match &config.engine.content_repository.file {
                    Some(fc) => FileContentRepoConfig {
                        containers: fc.containers.clone(),
                        max_segment_size: fc.max_segment_size_bytes,
                        memory_threshold: fc.memory_threshold_bytes,
                        inline_threshold: fc.inline_threshold_bytes,
                        cleanup_interval_secs: fc.cleanup_interval_secs,
                    },
                    None => FileContentRepoConfig::default(),
                };
                let repo = Arc::new(
                    FileContentRepository::new(file_config)
                        .context("Failed to create file content repository")?,
                );
                // Spawn background cleanup task.
                runifi_core::repository::cleanup::spawn_cleanup_task(repo.clone());
                tracing::info!("Using file-based content repository");
                repo
            }
            _ => {
                tracing::info!("Using in-memory content repository");
                Arc::new(InMemoryContentRepository::new())
            }
        };

    // Create FlowFile repository based on config.
    let flowfile_repo: Arc<dyn FlowFileRepository> =
        match config.engine.flowfile_repository.repo_type.as_str() {
            "wal" => {
                let wal_config = match &config.engine.flowfile_repository.wal {
                    Some(wc) => {
                        let fsync = match wc.fsync_mode.as_str() {
                            "never" => FsyncMode::Never,
                            _ => FsyncMode::Always,
                        };
                        WalFlowFileRepoConfig {
                            dir: wc.dir.clone(),
                            fsync_mode: fsync,
                            checkpoint_interval_secs: wc.checkpoint_interval_secs,
                        }
                    }
                    None => WalFlowFileRepoConfig::default(),
                };
                let repo = Arc::new(
                    WalFlowFileRepository::new(wal_config)
                        .context("Failed to create WAL FlowFile repository")?,
                );
                tracing::info!("Using WAL-based FlowFile repository");
                repo
            }
            _ => {
                tracing::info!("Using in-memory FlowFile repository (no crash recovery)");
                Arc::new(InMemoryFlowFileRepository)
            }
        };

    // Build the flow engine.
    let content_repo_ref = content_repo.clone();
    let registry = Arc::new(registry);
    let mut engine = FlowEngine::new(&config.flow.name, content_repo, flowfile_repo);
    // Provide the registry so the engine can hot-add processors at runtime.
    engine.set_registry(registry.clone());

    // Add processors from config.
    let mut node_ids = std::collections::HashMap::new();

    for proc_config in &config.flow.processors {
        let processor = registry
            .create_processor(&proc_config.type_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown processor type: {}", proc_config.type_name))?;

        let scheduling = match proc_config.scheduling.strategy.as_str() {
            "event" => SchedulingStrategy::EventDriven,
            _ => SchedulingStrategy::TimerDriven {
                interval_ms: proc_config.scheduling.interval_ms,
            },
        };

        let node_id = engine.add_processor(
            &proc_config.name,
            &proc_config.type_name,
            processor,
            scheduling,
            proc_config.properties.clone(),
        );
        node_ids.insert(proc_config.name.clone(), node_id);

        tracing::info!(
            name = %proc_config.name,
            type_name = %proc_config.type_name,
            "Added processor"
        );
    }

    // Wire connections.
    for conn_config in &config.flow.connections {
        let source_id = *node_ids
            .get(&conn_config.source)
            .ok_or_else(|| anyhow::anyhow!("Unknown source processor: {}", conn_config.source))?;
        let dest_id = *node_ids.get(&conn_config.destination).ok_or_else(|| {
            anyhow::anyhow!("Unknown destination processor: {}", conn_config.destination)
        })?;

        let bp_config = match &conn_config.back_pressure {
            Some(bp) => BackPressureConfig::new(
                bp.max_count
                    .unwrap_or(BackPressureConfig::DEFAULT_MAX_COUNT),
                bp.max_bytes
                    .unwrap_or(BackPressureConfig::DEFAULT_MAX_BYTES),
            ),
            None => BackPressureConfig::default(),
        };

        // We need a &'static str for the relationship name.
        // Leak is acceptable here since these live for the program's lifetime.
        let rel_name: &'static str = Box::leak(conn_config.relationship.clone().into_boxed_str());
        engine.connect(source_id, rel_name, dest_id, bp_config);

        tracing::info!(
            source = %conn_config.source,
            relationship = %conn_config.relationship,
            destination = %conn_config.destination,
            "Added connection"
        );
    }

    // Start the engine.
    engine.start().await.context("Failed to start engine")?;
    tracing::info!("Flow engine is running");

    // Set plugin types on the engine handle.
    let mut plugin_types: Vec<PluginTypeInfo> = Vec::new();
    for name in registry.processor_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Processor,
        });
    }
    for name in registry.source_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Source,
        });
    }
    for name in registry.sink_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Sink,
        });
    }
    engine.set_plugin_types(plugin_types);

    // Start the API server if enabled.
    let api_handle = if config.api.enabled {
        let engine_handle = engine
            .handle()
            .expect("engine handle must exist after start")
            .clone();

        let bind_address = config.api.bind_address.clone();
        let port = config.api.port;

        Some(tokio::spawn(async move {
            if let Err(e) = runifi_api::start_api_server(engine_handle, &bind_address, port).await {
                tracing::error!(error = %e, "API server failed");
            }
        }))
    } else {
        tracing::info!("API server disabled");
        None
    };

    // Wait for shutdown signal.
    wait_for_shutdown().await;

    // Graceful shutdown.
    tracing::info!("Shutting down...");
    if let Some(handle) = api_handle {
        handle.abort();
    }
    engine.stop().await;
    content_repo_ref.shutdown();
    tracing::info!("RuniFi stopped");

    Ok(())
}

async fn wait_for_shutdown() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    tracing::info!("Received shutdown signal");
}
