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
use runifi_core::engine::processor_node::SchedulingStrategy;
use runifi_core::registry::plugin_registry::PluginRegistry;
use runifi_core::repository::content_encrypted::EncryptedContentRepository;
use runifi_core::repository::content_memory::InMemoryContentRepository;
use runifi_core::repository::content_repo::ContentRepository;
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

    // Load flow configuration.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/flow.toml".to_string());

    // Check config file permissions (Unix only).
    if let Err(e) = check_config_permissions(&config_path) {
        tracing::error!(error = %e, "Config file permission check failed");
        return Err(anyhow::anyhow!("{}", e));
    }

    let config_str = match std::fs::read_to_string(&config_path) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(path = %config_path, error = %e, "No config file found, running idle");
            wait_for_shutdown().await;
            return Ok(());
        }
    };

    // Expand environment variable references in the config before parsing.
    let config_str = expand_env_vars(&config_str);

    let config: FlowConfig =
        toml::from_str(&config_str).context("Failed to parse flow configuration")?;

    // Resolve the encryption key from config (needed for ENC() decryption).
    let encryption_key: Option<Vec<u8>> = config
        .api
        .encryption
        .as_ref()
        .filter(|enc| enc.enabled)
        .map(|enc| hex::decode(&enc.key).context("Invalid hex encryption key in config"))
        .transpose()?;

    tracing::info!(flow = %config.flow.name, "Loaded flow configuration");

    // Create content repository, optionally wrapping with encryption.
    let content_repo: Arc<dyn ContentRepository> = {
        let base_repo = Arc::new(InMemoryContentRepository::new());

        match &config.api.encryption {
            Some(enc) if enc.enabled => {
                let key_provider = Arc::new(
                    StaticKeyProvider::from_hex(enc.key_id.clone(), &enc.key)
                        .context("Invalid encryption configuration")?,
                );
                tracing::info!(key_id = %enc.key_id, "Content encryption at rest enabled");
                Arc::new(EncryptedContentRepository::new(base_repo, key_provider))
            }
            _ => {
                tracing::info!("Content encryption at rest disabled");
                base_repo
            }
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

    // Build the flow engine.
    let registry = Arc::new(registry);
    let mut engine = FlowEngine::new(&config.flow.name, content_repo);
    engine.set_audit_logger(audit_logger);
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

        // Decrypt any ENC() property values.
        let mut properties = proc_config.properties.clone();
        for (prop_name, prop_value) in properties.iter_mut() {
            if is_encrypted_value(prop_value) {
                let key = encryption_key.as_deref().ok_or_else(|| {
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

        let api_config = config.api.clone();

        Some(tokio::spawn(async move {
            if let Err(e) = runifi_api::start_api_server(engine_handle, &api_config).await {
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
    tracing::info!("RuniFi stopped");

    Ok(())
}

async fn wait_for_shutdown() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
    tracing::info!("Received shutdown signal");
}
