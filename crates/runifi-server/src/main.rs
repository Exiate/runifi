use std::sync::Arc;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use runifi_core::audit::{
    AuditLogger, CompositeAuditLogger, FileAuditLogger, NullAuditLogger, TracingAuditLogger,
};
use runifi_core::config::flow_config::FlowConfig;
use runifi_core::config::permissions::check_config_permissions;
use runifi_core::config::property_encryption::{
    decrypt_property_value, expand_env_vars, is_encrypted_value,
};
use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::handle::{PluginKind, PluginTypeInfo};
use runifi_core::engine::persistence::{self, FlowPersistence, PersistedFlowState};
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_encrypted::EncryptedContentRepository;
use runifi_core::repository::content_file::{FileContentRepoConfig, FileContentRepository};
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;
use runifi_core::repository::flowfile_repo::{FlowFileRepository, InMemoryFlowFileRepository};
use runifi_core::repository::flowfile_wal::{
    FsyncMode, WalFlowFileRepoConfig, WalFlowFileRepository,
};
use runifi_core::repository::static_key_provider::StaticKeyProvider;

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

    // Load seed flow configuration (TOML), defaulting to an empty flow.
    let config_path = std::env::args().nth(1);

    let config: FlowConfig = match &config_path {
        Some(path) => {
            // Check config file permissions (Unix only).
            if let Err(e) = check_config_permissions(path) {
                tracing::error!(error = %e, "Config file permission check failed");
                return Err(anyhow::anyhow!("{}", e));
            }

            let config_str = std::fs::read_to_string(path).context("Failed to read config file")?;
            // Expand environment variable references before parsing.
            let config_str = expand_env_vars(&config_str);
            let cfg: FlowConfig =
                toml::from_str(&config_str).context("Failed to parse flow configuration")?;
            tracing::info!(flow = %cfg.flow.name, path = %path, "Loaded seed flow configuration");
            cfg
        }
        None => {
            // Try the default path; if it doesn't exist, start with an empty flow.
            let default_path = "config/flow.toml";
            match std::fs::read_to_string(default_path) {
                Ok(config_str) => {
                    // Check permissions on default path too.
                    if let Err(e) = check_config_permissions(default_path) {
                        tracing::error!(error = %e, "Config file permission check failed");
                        return Err(anyhow::anyhow!("{}", e));
                    }
                    let config_str = expand_env_vars(&config_str);
                    let cfg: FlowConfig = toml::from_str(&config_str)
                        .context("Failed to parse flow configuration")?;
                    tracing::info!(
                        flow = %cfg.flow.name,
                        path = %default_path,
                        "Loaded seed flow configuration"
                    );
                    cfg
                }
                Err(_) => {
                    tracing::info!("No config file found, starting with blank canvas");
                    FlowConfig::default()
                }
            }
        }
    };

    // Resolve the encryption key from config (needed for ENC() decryption).
    let encryption_key: Option<Vec<u8>> = config
        .api
        .encryption
        .as_ref()
        .filter(|enc| enc.enabled)
        .map(|enc| hex::decode(&enc.key).context("Invalid hex encryption key in config"))
        .transpose()?;

    // Warn if the encryption key appears to be stored as plaintext in the config file.
    if encryption_key.is_some()
        && let Some(enc) = config.api.encryption.as_ref()
    {
        let raw_path = config_path.as_deref().unwrap_or("config/flow.toml");
        if let Ok(raw_config) = std::fs::read_to_string(raw_path)
            && raw_config.contains(&enc.key)
        {
            tracing::warn!(
                "Encryption key appears to be stored as plaintext in the config file. \
                 Consider using environment variable substitution: \
                 key = \"${{RUNIFI_ENCRYPTION_KEY}}\""
            );
        }
    }

    // Check for persisted runtime flow state.
    let conf_dir = config.engine.conf_dir.clone();
    let runtime_flow = match persistence::load_runtime_flow(&conf_dir) {
        Ok(Some(state)) => {
            tracing::info!(
                conf_dir = %conf_dir.display(),
                processors = state.processors.len(),
                connections = state.connections.len(),
                "Loaded persisted runtime flow state"
            );
            Some(state)
        }
        Ok(None) => {
            tracing::info!(
                conf_dir = %conf_dir.display(),
                "No persisted flow state found"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                conf_dir = %conf_dir.display(),
                error = %e,
                "Failed to load persisted flow state, falling back to seed config"
            );
            None
        }
    };

    // Create content repository based on config, optionally wrapping with encryption.
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
                let file_repo = Arc::new(
                    FileContentRepository::new(file_config)
                        .context("Failed to create file content repository")?,
                );
                // Spawn background cleanup task.
                runifi_core::repository::cleanup::spawn_cleanup_task(file_repo.clone());
                tracing::info!("Using file-based content repository");
                // Wrap with encryption if configured.
                wrap_with_encryption(file_repo, &config)?
            }
            _ => {
                let base_repo: Arc<dyn ContentRepository> =
                    Arc::new(InMemoryContentRepository::new());
                tracing::info!("Using in-memory content repository");
                wrap_with_encryption(base_repo, &config)?
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

    // Build the audit logger.
    let audit_logger: Arc<dyn AuditLogger> = if config.audit.enabled {
        let mut sinks: Vec<Box<dyn AuditLogger>> = Vec::new();

        if config.audit.log_to_tracing {
            sinks.push(Box::new(TracingAuditLogger));
        }

        if let Some(ref path) = config.audit.file_path {
            match FileAuditLogger::new(path) {
                Ok(file_logger) => {
                    tracing::info!(path = %path, "Audit file logger enabled");
                    sinks.push(Box::new(file_logger));
                }
                Err(e) => {
                    tracing::error!(error = %e, path = %path, "Failed to open audit log file");
                    return Err(anyhow::anyhow!(
                        "Failed to open audit log file {}: {}",
                        path,
                        e
                    ));
                }
            }
        }

        if sinks.is_empty() {
            tracing::info!("Audit trail enabled but no sinks configured");
            Arc::new(NullAuditLogger)
        } else {
            tracing::info!(sinks = sinks.len(), "Audit trail enabled");
            Arc::new(CompositeAuditLogger::new(sinks))
        }
    } else {
        tracing::info!("Audit trail disabled");
        Arc::new(NullAuditLogger)
    };

    // Determine flow name — runtime state takes precedence.
    let flow_name = runtime_flow
        .as_ref()
        .map(|s| s.flow_name.clone())
        .unwrap_or_else(|| config.flow.name.clone());

    // Build the flow engine.
    let content_repo_ref = content_repo.clone();
    let registry = Arc::new(registry);
    let mut engine = FlowEngine::new(&flow_name, content_repo, flowfile_repo);
    engine.set_audit_logger(audit_logger);
    // Provide the registry so the engine can hot-add processors at runtime.
    engine.set_registry(registry.clone());

    // Set up flow persistence.
    let persistence_layer = FlowPersistence::new(conf_dir);
    engine.set_persistence(persistence_layer);

    // Populate the engine from either runtime state or seed config.
    if let Some(ref state) = runtime_flow {
        load_from_persisted_state(&mut engine, state, &registry)?;
    } else {
        load_from_seed_config(&mut engine, &config, &registry, encryption_key.as_deref())?;
    }

    // Start the engine.
    engine.start().await.context("Failed to start engine")?;
    tracing::info!("Flow engine is running");

    // Restore positions from persisted state.
    if let Some(ref state) = runtime_flow
        && let Some(handle) = engine.handle()
    {
        for (name, pos) in &state.positions {
            handle.set_position(name, pos.x, pos.y);
        }
    }

    // On first startup (no runtime flow), trigger an initial persist so the
    // seed config is saved as the runtime state.
    if runtime_flow.is_none()
        && let Some(handle) = engine.handle()
    {
        handle.notify_persist();
    }

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
    for name in registry.service_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Service,
        });
    }
    engine.set_plugin_types(plugin_types);

    // Start the API server if enabled.
    let api_handle = if config.api.enabled {
        let engine_handle = engine
            .handle()
            .expect("engine handle must exist after start")
            .clone();

        let api_config = config.api.clone();
        let api_registry = registry.clone();

        Some(tokio::spawn(async move {
            if let Err(e) = runifi_api::start_api_server_with_registry(
                engine_handle,
                &api_config,
                Some(api_registry),
            )
            .await
            {
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

/// Optionally wrap a content repository with encryption based on config.
fn wrap_with_encryption(
    base_repo: Arc<dyn ContentRepository>,
    config: &FlowConfig,
) -> Result<Arc<dyn ContentRepository>> {
    match &config.api.encryption {
        Some(enc) if enc.enabled => {
            let key_provider = Arc::new(
                StaticKeyProvider::from_hex(enc.key_id.clone(), &enc.key)
                    .context("Invalid encryption configuration")?,
            );
            tracing::info!(key_id = %enc.key_id, "Content encryption at rest enabled");
            Ok(Arc::new(EncryptedContentRepository::new(
                base_repo,
                key_provider,
            )))
        }
        _ => Ok(base_repo),
    }
}

/// Load processors and connections from a persisted runtime flow state.
fn load_from_persisted_state(
    engine: &mut FlowEngine,
    state: &PersistedFlowState,
    registry: &PluginRegistry,
) -> Result<()> {
    let mut node_ids = std::collections::HashMap::new();

    for proc_state in &state.processors {
        let processor = registry
            .create_processor(&proc_state.type_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown processor type in persisted state: {}",
                    proc_state.type_name
                )
            })?;

        let scheduling = match proc_state.scheduling.strategy.as_str() {
            "event" => SchedulingStrategy::EventDriven,
            _ => SchedulingStrategy::TimerDriven {
                interval_ms: proc_state.scheduling.interval_ms,
            },
        };

        let node_id = engine.add_processor(
            &proc_state.name,
            &proc_state.type_name,
            processor,
            scheduling,
            proc_state.properties.clone(),
        );
        node_ids.insert(proc_state.name.clone(), node_id);

        tracing::info!(
            name = %proc_state.name,
            type_name = %proc_state.type_name,
            "Added processor (from persisted state)"
        );
    }

    // Load controller services from persisted state.
    for svc_state in &state.services {
        let service = registry
            .create_service(&svc_state.type_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown service type in persisted state: {}",
                    svc_state.type_name
                )
            })?;

        let service_registry = engine.service_registry();
        let mut reg = service_registry.write();
        reg.add_service(svc_state.name.clone(), svc_state.type_name.clone(), service)
            .map_err(|e| anyhow::anyhow!("Failed to add service '{}': {}", svc_state.name, e))?;

        if !svc_state.properties.is_empty() {
            reg.configure_service(&svc_state.name, svc_state.properties.clone())
                .map_err(|e| {
                    anyhow::anyhow!("Failed to configure service '{}': {}", svc_state.name, e)
                })?;
        }
        drop(reg);

        tracing::info!(
            name = %svc_state.name,
            type_name = %svc_state.type_name,
            "Added controller service (from persisted state)"
        );
    }

    for conn_state in &state.connections {
        let source_id = *node_ids.get(&conn_state.source).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown source processor in persisted state: {}",
                conn_state.source
            )
        })?;
        let dest_id = *node_ids.get(&conn_state.destination).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown destination processor in persisted state: {}",
                conn_state.destination
            )
        })?;

        let bp_config = match &conn_state.back_pressure {
            Some(bp) => BackPressureConfig::new(
                bp.max_count
                    .unwrap_or(BackPressureConfig::DEFAULT_MAX_COUNT),
                bp.max_bytes
                    .unwrap_or(BackPressureConfig::DEFAULT_MAX_BYTES),
            ),
            None => BackPressureConfig::default(),
        };

        let rel_name: &'static str = Box::leak(conn_state.relationship.clone().into_boxed_str());
        engine.connect(source_id, rel_name, dest_id, bp_config);

        tracing::info!(
            source = %conn_state.source,
            relationship = %conn_state.relationship,
            destination = %conn_state.destination,
            "Added connection (from persisted state)"
        );
    }

    Ok(())
}

/// Load processors and connections from the seed TOML config.
fn load_from_seed_config(
    engine: &mut FlowEngine,
    config: &FlowConfig,
    registry: &PluginRegistry,
    encryption_key: Option<&[u8]>,
) -> Result<()> {
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

        // Decrypt any ENC() property values.
        let mut properties = proc_config.properties.clone();
        for (prop_name, prop_value) in properties.iter_mut() {
            if is_encrypted_value(prop_value) {
                let key = encryption_key.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Processor '{}' property '{}' uses ENC() but no encryption key is configured",
                        proc_config.name,
                        prop_name
                    )
                })?;
                *prop_value = decrypt_property_value(prop_value, key).with_context(|| {
                    format!(
                        "Failed to decrypt property '{}' on processor '{}'",
                        prop_name, proc_config.name
                    )
                })?;
                tracing::debug!(
                    processor = %proc_config.name,
                    property = %prop_name,
                    "Decrypted ENC() property value"
                );
            }
        }

        let node_id = engine.add_processor(
            &proc_config.name,
            &proc_config.type_name,
            processor,
            scheduling,
            properties,
        );
        node_ids.insert(proc_config.name.clone(), node_id);

        tracing::info!(
            name = %proc_config.name,
            type_name = %proc_config.type_name,
            "Added processor"
        );
    }

    // Load controller services from seed config.
    for svc_config in &config.flow.services {
        let service = registry
            .create_service(&svc_config.type_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown service type: {}", svc_config.type_name))?;

        let service_registry = engine.service_registry();
        let mut reg = service_registry.write();
        reg.add_service(
            svc_config.name.clone(),
            svc_config.type_name.clone(),
            service,
        )
        .map_err(|e| anyhow::anyhow!("Failed to add service '{}': {}", svc_config.name, e))?;

        if !svc_config.properties.is_empty() {
            reg.configure_service(&svc_config.name, svc_config.properties.clone())
                .map_err(|e| {
                    anyhow::anyhow!("Failed to configure service '{}': {}", svc_config.name, e)
                })?;
        }
        drop(reg);

        tracing::info!(
            name = %svc_config.name,
            type_name = %svc_config.type_name,
            "Added controller service"
        );
    }

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

        let rel_name: &'static str = Box::leak(conn_config.relationship.clone().into_boxed_str());
        engine.connect(source_id, rel_name, dest_id, bp_config);

        tracing::info!(
            source = %conn_config.source,
            relationship = %conn_config.relationship,
            destination = %conn_config.destination,
            "Added connection"
        );
    }

    Ok(())
}

async fn wait_for_shutdown() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    tracing::info!("Received shutdown signal");
}
