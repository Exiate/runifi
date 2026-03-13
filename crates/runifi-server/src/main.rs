use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use runifi_core::audit::{
    AuditLogger, CompositeAuditLogger, FileAuditLogger, NullAuditLogger, TracingAuditLogger,
};
use runifi_core::auth::jwt::JwtConfig;
use runifi_core::auth::store::UserStore;
use runifi_core::config::flow_config::FlowConfig;
use runifi_core::config::flow_config::parse_duration_str;
use runifi_core::config::permissions::check_config_permissions;
use runifi_core::config::property_encryption::{
    decrypt_property_value, expand_env_vars, is_encrypted_value,
};
use runifi_core::connection::back_pressure::BackPressureConfig;
use runifi_core::connection::flow_connection::QueuePriority;
use runifi_core::engine::flow_engine::FlowEngine;
use runifi_core::engine::handle::{PluginKind, PluginTypeInfo};
use runifi_core::engine::persistence::{
    self, FlowPersistence, PersistedFlowState, PersistedProcessGroup,
};
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_encrypted::EncryptedContentRepository;
use runifi_core::repository::content_file::{FileContentRepoConfig, FileContentRepository};
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;
use runifi_core::repository::encrypted_wal::{EncryptedWalConfig, EncryptedWalFlowFileRepository};
use runifi_core::repository::env_key_provider::EnvKeyProvider;
use runifi_core::repository::file_key_provider::FileKeyProvider;
use runifi_core::repository::flowfile_repo::{FlowFileRepository, InMemoryFlowFileRepository};
use runifi_core::repository::flowfile_wal::{
    FsyncMode, WalFlowFileRepoConfig, WalFlowFileRepository,
};
use runifi_core::repository::key_provider::KeyProvider;
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
    // First check api.encryption (legacy), then engine.encryption (new).
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

    // Build the engine-level key provider for repository encryption (if configured).
    let repo_key_provider: Option<Arc<dyn KeyProvider>> = build_key_provider(&config)?;

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
                wrap_with_encryption(file_repo, &config, repo_key_provider.clone())?
            }
            _ => {
                let base_repo: Arc<dyn ContentRepository> =
                    Arc::new(InMemoryContentRepository::new());
                tracing::info!("Using in-memory content repository");
                wrap_with_encryption(base_repo, &config, repo_key_provider.clone())?
            }
        };

    // Create FlowFile repository based on config.
    let flowfile_repo: Arc<dyn FlowFileRepository> =
        match config.engine.flowfile_repository.repo_type.as_str() {
            "wal" => {
                let wal_toml = config.engine.flowfile_repository.wal.as_ref();
                let fsync = match wal_toml.map(|wc| wc.fsync_mode.as_str()) {
                    Some("never") => FsyncMode::Never,
                    _ => FsyncMode::Always,
                };
                let dir = wal_toml
                    .map(|wc| wc.dir.clone())
                    .unwrap_or_else(|| PathBuf::from("data/flowfile-repo"));
                let checkpoint_secs = wal_toml
                    .map(|wc| wc.checkpoint_interval_secs)
                    .unwrap_or(120);

                // Use encrypted WAL if engine.encryption is configured.
                if let Some(ref kp) = repo_key_provider {
                    let enc_config = EncryptedWalConfig {
                        dir,
                        fsync_mode: fsync,
                        checkpoint_interval_secs: checkpoint_secs,
                    };
                    let repo = Arc::new(
                        EncryptedWalFlowFileRepository::new(enc_config, kp.clone())
                            .context("Failed to create encrypted WAL FlowFile repository")?,
                    );
                    tracing::info!("Using encrypted WAL-based FlowFile repository");
                    repo
                } else {
                    let wal_config = WalFlowFileRepoConfig {
                        dir,
                        fsync_mode: fsync,
                        checkpoint_interval_secs: checkpoint_secs,
                    };
                    let repo = Arc::new(
                        WalFlowFileRepository::new(wal_config)
                            .context("Failed to create WAL FlowFile repository")?,
                    );
                    tracing::info!("Using WAL-based FlowFile repository");
                    repo
                }
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

    // Restore positions and labels from persisted state. Uses restore_position()
    // to avoid triggering an unnecessary persist of the state just loaded.
    if let Some(ref state) = runtime_flow
        && let Some(handle) = engine.handle()
    {
        for (name, pos) in &state.positions {
            handle.restore_position(name, pos.x, pos.y);
        }
        // Restore labels directly into the shared labels vec (no persist trigger).
        {
            let mut labels = handle.labels.write();
            let mut max_label_id: u64 = 0;
            for pl in &state.labels {
                // Extract numeric suffix from "label-N" for counter reset.
                if let Some(suffix) = pl.id.strip_prefix("label-")
                    && let Ok(n) = suffix.parse::<u64>()
                {
                    max_label_id = max_label_id.max(n);
                }
                labels.push(runifi_core::engine::handle::LabelInfo {
                    id: pl.id.clone(),
                    text: pl.text.clone(),
                    x: pl.x,
                    y: pl.y,
                    width: pl.width,
                    height: pl.height,
                    background_color: pl.background_color.clone(),
                    font_size: pl.font_size,
                });
            }
            // Reset the label ID counter so new labels don't collide.
            if max_label_id > 0 {
                runifi_core::engine::handle::reset_label_id_counter(max_label_id + 1);
            }
        }

        // Restore process groups from persisted state.
        restore_process_groups(handle, &state.process_groups);
    }

    // On first startup (no runtime flow), load process groups from seed config
    // and trigger an initial persist so the seed config is saved as the runtime state.
    if runtime_flow.is_none()
        && let Some(handle) = engine.handle()
    {
        if !config.flow.process_groups.is_empty() {
            load_seed_process_groups(handle, &config.flow.process_groups, None);
        }
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

    // Initialize user management if auth is enabled.
    let user_store = Arc::new(UserStore::new());
    let jwt_config = if config.auth.enabled {
        if config.auth.jwt_secret == "change-me-in-production" {
            tracing::warn!(
                "Using default JWT secret. Set auth.jwt_secret or RUNIFI_JWT_SECRET for production."
            );
        }
        let jwt = JwtConfig::new(&config.auth.jwt_secret, config.auth.jwt_expiry_secs);

        // Single-user mode: bootstrap a default admin if no users exist.
        if config.auth.single_user_mode && user_store.user_count() == 0 {
            match user_store.create_user(
                config.auth.default_admin_username.clone(),
                &config.auth.default_admin_password,
            ) {
                Ok(user) => {
                    tracing::info!(
                        username = %user.username,
                        "Single-user mode: default admin account created"
                    );
                    if config.auth.default_admin_password == "admin" {
                        tracing::warn!(
                            "Default admin password is 'admin'. Change it immediately in production."
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create default admin user");
                }
            }
        }

        tracing::info!(
            expiry_secs = config.auth.jwt_expiry_secs,
            single_user_mode = config.auth.single_user_mode,
            "JWT user authentication enabled"
        );
        Some(jwt)
    } else {
        tracing::info!("User authentication is disabled");
        None
    };

    // Start the API server if enabled.
    let api_handle = if config.api.enabled {
        let engine_handle = engine
            .handle()
            .expect("engine handle must exist after start")
            .clone();

        let api_config = config.api.clone();
        let api_registry = registry.clone();
        let api_user_store = user_store.clone();
        let api_jwt_config = jwt_config.clone();
        let api_auth_config = config.auth.clone();

        Some(tokio::spawn(async move {
            if let Err(e) = runifi_api::start_api_server_with_registry(
                engine_handle,
                &api_config,
                Some(api_registry),
                if api_auth_config.enabled {
                    Some(api_user_store)
                } else {
                    None
                },
                api_jwt_config,
                Some(api_auth_config),
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

/// Build a key provider from the engine-level encryption config.
fn build_key_provider(config: &FlowConfig) -> Result<Option<Arc<dyn KeyProvider>>> {
    let enc = match &config.engine.encryption {
        Some(enc) if enc.enabled => enc,
        _ => return Ok(None),
    };

    if enc.algorithm != "AES-256-GCM" {
        return Err(anyhow::anyhow!(
            "Unsupported encryption algorithm: '{}'. Only 'AES-256-GCM' is supported.",
            enc.algorithm
        ));
    }

    let kp = &enc.key_provider;
    let provider: Arc<dyn KeyProvider> = match kp.provider_type.as_str() {
        "file" => {
            let path = kp.path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("engine.encryption.key_provider.path is required for file provider")
            })?;
            let provider = FileKeyProvider::from_file(std::path::Path::new(path))
                .map_err(|e| anyhow::anyhow!("Failed to load key file: {}", e))?;
            tracing::info!(path = %path, "Loaded encryption keys from file");
            Arc::new(provider)
        }
        "env" => {
            let active_id = kp.active_key_id.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "engine.encryption.key_provider.active_key_id is required for env provider"
                )
            })?;
            let key_ids = kp.key_ids.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "engine.encryption.key_provider.key_ids is required for env provider"
                )
            })?;
            let provider = EnvKeyProvider::new(active_id.clone(), key_ids, &kp.key_env_prefix)
                .map_err(|e| anyhow::anyhow!("Failed to load encryption keys from env: {}", e))?;
            tracing::info!(
                active_key_id = %active_id,
                key_count = key_ids.len(),
                "Loaded encryption keys from environment variables"
            );
            Arc::new(provider)
        }
        "static" => {
            let key_hex = kp.key.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "engine.encryption.key_provider.key is required for static provider"
                )
            })?;
            let key_id = kp.key_id.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "engine.encryption.key_provider.key_id is required for static provider"
                )
            })?;
            let provider = StaticKeyProvider::from_hex(key_id.clone(), key_hex)
                .map_err(|e| anyhow::anyhow!("Invalid static encryption key: {}", e))?;
            tracing::info!(key_id = %key_id, "Using static encryption key");
            Arc::new(provider)
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unknown key provider type: '{}'. Supported types: file, env, static",
                other
            ));
        }
    };

    Ok(Some(provider))
}

/// Optionally wrap a content repository with encryption based on config.
fn wrap_with_encryption(
    base_repo: Arc<dyn ContentRepository>,
    config: &FlowConfig,
    repo_key_provider: Option<Arc<dyn KeyProvider>>,
) -> Result<Arc<dyn ContentRepository>> {
    // First, try engine-level encryption (new config path).
    if let Some(kp) = repo_key_provider {
        tracing::info!("Content encryption at rest enabled (engine.encryption)");
        return Ok(Arc::new(EncryptedContentRepository::new(base_repo, kp)));
    }

    // Fall back to legacy api.encryption config.
    match &config.api.encryption {
        Some(enc) if enc.enabled => {
            let key_provider = Arc::new(
                StaticKeyProvider::from_hex(enc.key_id.clone(), &enc.key)
                    .context("Invalid encryption configuration")?,
            );
            tracing::info!(key_id = %enc.key_id, "Content encryption at rest enabled (api.encryption)");
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
            "cron" => SchedulingStrategy::CronDriven {
                expression: proc_state
                    .scheduling
                    .expression
                    .clone()
                    .unwrap_or_else(|| "0 * * * * *".to_string()),
            },
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

        let expiration = conn_state
            .expiration
            .as_deref()
            .and_then(parse_duration_str);
        let priority = parse_queue_priority(
            conn_state.priority.as_deref(),
            conn_state.priority_attribute.as_deref(),
        );

        let rel_name: &'static str = Box::leak(conn_state.relationship.clone().into_boxed_str());
        engine.connect_with_options(
            source_id, rel_name, dest_id, bp_config, expiration, priority,
        );

        tracing::info!(
            source = %conn_state.source,
            relationship = %conn_state.relationship,
            destination = %conn_state.destination,
            "Added connection (from persisted state)"
        );
    }

    Ok(())
}

/// Parse queue priority from config strings.
fn parse_queue_priority(priority: Option<&str>, priority_attribute: Option<&str>) -> QueuePriority {
    match priority {
        Some("NewestFirst") => QueuePriority::NewestFirst,
        Some("PriorityAttribute") => {
            QueuePriority::PriorityAttribute(priority_attribute.unwrap_or("priority").to_string())
        }
        _ => QueuePriority::Fifo,
    }
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
            "cron" => SchedulingStrategy::CronDriven {
                expression: proc_config
                    .scheduling
                    .expression
                    .clone()
                    .unwrap_or_else(|| "0 * * * * *".to_string()),
            },
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

        let expiration = conn_config
            .expiration
            .as_deref()
            .and_then(parse_duration_str);
        let priority = parse_queue_priority(
            conn_config.priority.as_deref(),
            conn_config.priority_attribute.as_deref(),
        );

        let rel_name: &'static str = Box::leak(conn_config.relationship.clone().into_boxed_str());
        engine.connect_with_options(
            source_id, rel_name, dest_id, bp_config, expiration, priority,
        );

        tracing::info!(
            source = %conn_config.source,
            relationship = %conn_config.relationship,
            destination = %conn_config.destination,
            "Added connection"
        );
    }

    Ok(())
}

/// Restore process groups from persisted state directly into the engine handle.
///
/// This bypasses the `create_process_group` API (which would generate new IDs
/// and trigger audit events) and instead populates the handle's process_groups
/// store directly — preserving original IDs from the persisted state.
fn restore_process_groups(
    handle: &runifi_core::engine::handle::EngineHandle,
    persisted_groups: &[PersistedProcessGroup],
) {
    use runifi_core::engine::process_group::{PortInfo, PortType, ProcessGroupInfo};

    if persisted_groups.is_empty() {
        return;
    }

    let mut groups = handle.process_groups.write();
    let mut max_group_id: u64 = 0;

    for pg in persisted_groups {
        // Extract numeric suffix from "pg-N" for counter reset.
        if let Some(suffix) = pg.id.strip_prefix("pg-")
            && let Ok(n) = suffix.parse::<u64>()
        {
            max_group_id = max_group_id.max(n);
        }

        let input_ports: Vec<PortInfo> = pg
            .input_ports
            .iter()
            .map(|p| PortInfo {
                id: p.id.clone(),
                name: p.name.clone(),
                port_type: PortType::Input,
                group_id: pg.id.clone(),
            })
            .collect();

        let output_ports: Vec<PortInfo> = pg
            .output_ports
            .iter()
            .map(|p| PortInfo {
                id: p.id.clone(),
                name: p.name.clone(),
                port_type: PortType::Output,
                group_id: pg.id.clone(),
            })
            .collect();

        groups.push(ProcessGroupInfo {
            id: pg.id.clone(),
            name: pg.name.clone(),
            input_ports,
            output_ports,
            processor_names: pg.processor_names.clone(),
            connection_ids: pg.connection_ids.clone(),
            child_group_ids: pg.child_group_ids.clone(),
            parent_group_id: pg.parent_group_id.clone(),
            variables: pg.variables.clone(),
        });
    }

    // Reset the group ID counter so new groups don't collide.
    if max_group_id > 0 {
        runifi_core::engine::handle::reset_group_id_counter(max_group_id + 1);
    }

    tracing::info!(
        count = persisted_groups.len(),
        "Restored process groups from persisted state"
    );
}

/// Load process groups from the seed TOML configuration into the engine handle.
///
/// This is called when starting fresh (no persisted state) and creates the
/// hierarchical process group structure defined in the config.
fn load_seed_process_groups(
    handle: &runifi_core::engine::handle::EngineHandle,
    groups_config: &[runifi_core::config::flow_config::ProcessGroupConfig],
    parent_id: Option<String>,
) {
    for pg_config in groups_config {
        let input_ports = pg_config
            .input_ports
            .as_ref()
            .map(|p| p.ports.clone())
            .unwrap_or_default();

        let output_ports = pg_config
            .output_ports
            .as_ref()
            .map(|p| p.ports.clone())
            .unwrap_or_default();

        match handle.create_process_group(
            pg_config.name.clone(),
            parent_id.clone(),
            input_ports,
            output_ports,
            pg_config.variables.clone(),
        ) {
            Ok(group_id) => {
                tracing::info!(
                    name = %pg_config.name,
                    id = %group_id,
                    "Created process group (from seed config)"
                );

                // Recursively create child process groups.
                if !pg_config.process_groups.is_empty() {
                    load_seed_process_groups(handle, &pg_config.process_groups, Some(group_id));
                }
            }
            Err(e) => {
                tracing::error!(
                    name = %pg_config.name,
                    error = %e,
                    "Failed to create process group from seed config"
                );
            }
        }
    }
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
