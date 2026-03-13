use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
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

/// RuniFi — high-performance data flow engine.
///
/// A Rust reimplementation of Apache NiFi, purpose-built for ultra-low-latency
/// and high-throughput file transfers.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Path to flow config TOML file.
    #[arg(short, long, default_value = "config/flow.toml")]
    config: PathBuf,

    /// API server bind address (overrides config file).
    #[arg(short, long)]
    bind: Option<String>,

    /// API server port (overrides config file).
    #[arg(short, long)]
    port: Option<u16>,

    /// Log level (overrides RUST_LOG env var).
    #[arg(long)]
    log_level: Option<String>,

    /// Runtime persistence directory (overrides config file).
    #[arg(long)]
    conf_dir: Option<PathBuf>,

    /// WAL FlowFile repository directory (overrides config file).
    #[arg(long)]
    wal_dir: Option<PathBuf>,
}

/// Apply CLI overrides to the loaded config.
fn apply_cli_overrides(config: &mut FlowConfig, cli: &Cli) {
    if let Some(ref bind) = cli.bind {
        config.api.bind_address = bind.clone();
    }
    if let Some(port) = cli.port {
        config.api.port = port;
    }
    if let Some(ref dir) = cli.conf_dir {
        config.engine.conf_dir = dir.clone();
    }
    if let Some(ref dir) = cli.wal_dir {
        let wal_config = config
            .engine
            .flowfile_repository
            .wal
            .get_or_insert_with(|| runifi_core::config::flow_config::WalRepoConfigToml {
                dir: dir.clone(),
                fsync_mode: "always".to_string(),
                checkpoint_interval_secs: 120,
            });
        wal_config.dir = dir.clone();
        // If the user specifies a WAL dir, ensure the repo type is set to WAL.
        config.engine.flowfile_repository.repo_type = "wal".to_string();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Determine log filter: CLI flag > RUST_LOG env > default "info".
    let log_filter = if let Some(ref level) = cli.log_level {
        EnvFilter::new(level)
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::fmt().with_env_filter(log_filter).init();

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
    let config_path = &cli.config;

    let mut config: FlowConfig = match std::fs::read_to_string(config_path) {
        Ok(config_str) => {
            let path_str = config_path.display().to_string();
            // Check config file permissions (Unix only).
            if let Err(e) = check_config_permissions(&path_str) {
                tracing::error!(error = %e, "Config file permission check failed");
                return Err(anyhow::anyhow!("{}", e));
            }
            // Expand environment variable references before parsing.
            let config_str = expand_env_vars(&config_str);
            let cfg: FlowConfig =
                toml::from_str(&config_str).context("Failed to parse flow configuration")?;
            tracing::info!(flow = %cfg.flow.name, path = %path_str, "Loaded seed flow configuration");
            cfg
        }
        Err(_) => {
            tracing::info!("No config file found, starting with blank canvas");
            FlowConfig::default()
        }
    };

    // Apply CLI overrides on top of the config file values.
    apply_cli_overrides(&mut config, &cli);

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
        && let Ok(raw_config) = std::fs::read_to_string(config_path)
        && raw_config.contains(&enc.key)
    {
        tracing::warn!(
            "Encryption key appears to be stored as plaintext in the config file. \
             Consider using environment variable substitution: \
             key = \"${{RUNIFI_ENCRYPTION_KEY}}\""
        );
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

    // Restore positions from persisted state. Uses restore_position() to
    // avoid triggering an unnecessary persist of the state just loaded.
    if let Some(ref state) = runtime_flow
        && let Some(handle) = engine.handle()
    {
        for (name, pos) in &state.positions {
            handle.restore_position(name, pos.x, pos.y);
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
            tags: registry.processor_tags(name),
        });
    }
    for name in registry.source_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Source,
            tags: registry.source_tags(name),
        });
    }
    for name in registry.sink_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Sink,
            tags: registry.sink_tags(name),
        });
    }
    for name in registry.service_types() {
        plugin_types.push(PluginTypeInfo {
            type_name: name.to_string(),
            kind: PluginKind::Service,
            tags: Vec::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cli_defaults() {
        let cli = Cli::parse_from(["runifi"]);
        assert_eq!(cli.config, PathBuf::from("config/flow.toml"));
        assert!(cli.bind.is_none());
        assert!(cli.port.is_none());
        assert!(cli.log_level.is_none());
        assert!(cli.conf_dir.is_none());
        assert!(cli.wal_dir.is_none());
    }

    #[test]
    fn cli_config_short_flag() {
        let cli = Cli::parse_from(["runifi", "-c", "my/flow.toml"]);
        assert_eq!(cli.config, PathBuf::from("my/flow.toml"));
    }

    #[test]
    fn cli_config_long_flag() {
        let cli = Cli::parse_from(["runifi", "--config", "my/flow.toml"]);
        assert_eq!(cli.config, PathBuf::from("my/flow.toml"));
    }

    #[test]
    fn cli_bind_and_port() {
        let cli = Cli::parse_from(["runifi", "-b", "0.0.0.0", "-p", "9090"]);
        assert_eq!(cli.bind.as_deref(), Some("0.0.0.0"));
        assert_eq!(cli.port, Some(9090));
    }

    #[test]
    fn cli_log_level() {
        let cli = Cli::parse_from(["runifi", "--log-level", "debug"]);
        assert_eq!(cli.log_level.as_deref(), Some("debug"));
    }

    #[test]
    fn cli_conf_dir_and_wal_dir() {
        let cli = Cli::parse_from([
            "runifi",
            "--conf-dir",
            "/data/conf",
            "--wal-dir",
            "/data/wal",
        ]);
        assert_eq!(cli.conf_dir, Some(PathBuf::from("/data/conf")));
        assert_eq!(cli.wal_dir, Some(PathBuf::from("/data/wal")));
    }

    #[test]
    fn cli_all_flags_combined() {
        let cli = Cli::parse_from([
            "runifi",
            "-c",
            "test.toml",
            "-b",
            "192.168.1.1",
            "-p",
            "3000",
            "--log-level",
            "trace",
            "--conf-dir",
            "/tmp/conf",
            "--wal-dir",
            "/tmp/wal",
        ]);
        assert_eq!(cli.config, PathBuf::from("test.toml"));
        assert_eq!(cli.bind.as_deref(), Some("192.168.1.1"));
        assert_eq!(cli.port, Some(3000));
        assert_eq!(cli.log_level.as_deref(), Some("trace"));
        assert_eq!(cli.conf_dir, Some(PathBuf::from("/tmp/conf")));
        assert_eq!(cli.wal_dir, Some(PathBuf::from("/tmp/wal")));
    }

    #[test]
    fn apply_overrides_bind_and_port() {
        let mut config = FlowConfig::default();
        let cli = Cli::parse_from(["runifi", "-b", "10.0.0.1", "-p", "4000"]);
        apply_cli_overrides(&mut config, &cli);
        assert_eq!(config.api.bind_address, "10.0.0.1");
        assert_eq!(config.api.port, 4000);
    }

    #[test]
    fn apply_overrides_conf_dir() {
        let mut config = FlowConfig::default();
        let cli = Cli::parse_from(["runifi", "--conf-dir", "/var/runifi/conf"]);
        apply_cli_overrides(&mut config, &cli);
        assert_eq!(config.engine.conf_dir, PathBuf::from("/var/runifi/conf"));
    }

    #[test]
    fn apply_overrides_wal_dir_sets_repo_type() {
        let mut config = FlowConfig::default();
        assert_eq!(config.engine.flowfile_repository.repo_type, "memory");
        let cli = Cli::parse_from(["runifi", "--wal-dir", "/var/runifi/wal"]);
        apply_cli_overrides(&mut config, &cli);
        assert_eq!(config.engine.flowfile_repository.repo_type, "wal");
        let wal = config.engine.flowfile_repository.wal.as_ref().unwrap();
        assert_eq!(wal.dir, PathBuf::from("/var/runifi/wal"));
    }

    #[test]
    fn apply_overrides_no_flags_leaves_defaults() {
        let mut config = FlowConfig::default();
        let original_bind = config.api.bind_address.clone();
        let original_port = config.api.port;
        let original_conf = config.engine.conf_dir.clone();
        let cli = Cli::parse_from(["runifi"]);
        apply_cli_overrides(&mut config, &cli);
        assert_eq!(config.api.bind_address, original_bind);
        assert_eq!(config.api.port, original_port);
        assert_eq!(config.engine.conf_dir, original_conf);
        assert!(config.engine.flowfile_repository.wal.is_none());
    }

    #[test]
    fn cli_version_flag() {
        let result = Cli::try_parse_from(["runifi", "--version"]);
        // --version causes clap to exit with an error containing version info.
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
    }

    #[test]
    fn cli_help_flag() {
        let result = Cli::try_parse_from(["runifi", "--help"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }
}
