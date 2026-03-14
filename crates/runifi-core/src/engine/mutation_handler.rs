use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use runifi_plugin_api::Processor;
use runifi_plugin_api::relationship::Relationship;

use super::bulletin::BulletinBoard;
use super::handle::{ConnectionInfo, ProcessorInfo, PropertyDescriptorInfo, RelationshipInfo};
use super::metrics::ProcessorMetrics;
use super::mutation::MutationError;
use super::processor_node::{ProcessorNode, SchedulingStrategy};
use crate::audit::{AuditAction, AuditEvent, AuditLogger, AuditTarget};
use crate::cluster::load_balance::LoadBalanceConfig;
use crate::connection::back_pressure::BackPressureConfig;
use crate::connection::flow_connection::FlowConnection;
use crate::connection::query::FlowConnectionQuery;
use crate::id::IdGenerator;
use crate::registry::plugin_registry::PluginRegistry;
use crate::repository::content_repo::ContentRepository;
use crate::repository::flowfile_repo::FlowFileRepository;
use crate::repository::provenance_repo::SharedProvenanceRepository;

use super::flow_engine::scheduling_display;

/// Handles runtime topology mutations (add/remove processors and connections).
///
/// Extracted from `FlowEngine` to break the god object. Holds all the shared
/// state references needed for hot-add and hot-remove operations so that
/// `run_mutation_handler` in `flow_engine.rs` can delegate cleanly.
pub struct DefaultMutationHandler {
    pub live_procs: Arc<RwLock<Vec<ProcessorInfo>>>,
    pub live_conns: Arc<RwLock<Vec<ConnectionInfo>>>,
    pub content_repo: Arc<dyn ContentRepository>,
    pub id_gen: Arc<IdGenerator>,
    pub bulletin_board: Arc<BulletinBoard>,
    pub registry: Option<Arc<PluginRegistry>>,
    pub parent_cancel: CancellationToken,
    pub proc_tokens: Arc<parking_lot::Mutex<HashMap<String, CancellationToken>>>,
    pub runtime_conn_id: usize,
    pub flowfile_repo: Arc<dyn FlowFileRepository>,
    pub audit_logger: Arc<dyn AuditLogger>,
    pub provenance_repo: SharedProvenanceRepository,
}

impl DefaultMutationHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn handle_add_processor(
        &self,
        name: &str,
        type_name: &str,
        properties: HashMap<String, String>,
        scheduling_strategy: &str,
        interval_ms: u64,
        cron_expression: Option<&str>,
    ) -> std::result::Result<(), MutationError> {
        if scheduling_strategy != "timer"
            && scheduling_strategy != "event"
            && scheduling_strategy != "cron"
        {
            return Err(MutationError::InvalidSchedulingStrategy(
                scheduling_strategy.to_string(),
            ));
        }

        let reg = self
            .registry
            .as_ref()
            .ok_or_else(|| MutationError::Internal("No plugin registry available".into()))?;

        let processor: Box<dyn Processor> = reg
            .create_processor(type_name)
            .ok_or_else(|| MutationError::UnknownType(type_name.to_string()))?;

        if self.live_procs.read().iter().any(|p| p.name == name) {
            return Err(MutationError::DuplicateName(name.to_string()));
        }

        let scheduling = if scheduling_strategy == "event" {
            SchedulingStrategy::EventDriven
        } else if scheduling_strategy == "cron" {
            let expr = cron_expression.unwrap_or("0 * * * * *");
            // Validate the CRON expression.
            use std::str::FromStr;
            cron::Schedule::from_str(expr).map_err(|e| {
                MutationError::InvalidCronExpression(expr.to_string(), e.to_string())
            })?;
            SchedulingStrategy::CronDriven {
                expression: expr.to_string(),
            }
        } else {
            SchedulingStrategy::TimerDriven { interval_ms }
        };

        let metrics = Arc::new(ProcessorMetrics::new());
        metrics
            .enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let prop_descriptors: Vec<PropertyDescriptorInfo> = processor
            .property_descriptors()
            .into_iter()
            .map(|pd| PropertyDescriptorInfo {
                name: pd.name.to_string(),
                description: pd.description.to_string(),
                required: pd.required,
                default_value: pd.default_value.map(|v| v.to_string()),
                sensitive: pd.sensitive,
                allowed_values: pd
                    .allowed_values
                    .map(|av| av.iter().map(|v| v.to_string()).collect()),
                expression_language_supported: pd.expression_language_supported,
            })
            .collect();

        let relationships: Vec<RelationshipInfo> = processor
            .relationships()
            .into_iter()
            .map(|r| RelationshipInfo {
                name: r.name.to_string(),
                description: r.description.to_string(),
                auto_terminated: r.auto_terminated,
            })
            .collect();

        let shared_props = Arc::new(RwLock::new(properties));
        let child_token = self.parent_cancel.child_token();

        let mut pn = ProcessorNode::new(
            name.to_string(),
            format!("runtime-{}", name),
            processor,
            scheduling.clone(),
            shared_props.clone(),
            self.content_repo.clone(),
            self.id_gen.clone(),
            child_token.clone(),
            metrics.clone(),
            self.bulletin_board.clone(),
            self.flowfile_repo.clone(),
        );
        pn.set_provenance_repo(self.provenance_repo.clone());
        pn.set_type_name(type_name.to_string());
        let sensitive_names: Vec<String> = prop_descriptors
            .iter()
            .filter(|d| d.sensitive)
            .map(|d| d.name.clone())
            .collect();
        if !sensitive_names.is_empty() {
            pn.set_sensitive_property_names(sensitive_names);
        }

        let input_h = pn.input_connections_handle();
        let output_h = pn.output_connections_handle();
        let notifiers_h = pn.input_notifiers_handle();

        self.proc_tokens
            .lock()
            .insert(name.to_string(), child_token);
        tokio::spawn(pn.run());

        self.live_procs.write().push(ProcessorInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            scheduling_display: scheduling_display(&scheduling),
            metrics,
            property_descriptors: prop_descriptors,
            relationships,
            properties: shared_props,
            input_connections: input_h,
            output_connections: output_h,
            input_notifiers: notifiers_h,
            penalty_duration_ms: Arc::new(AtomicU64::new(30_000)),
            yield_duration_ms: Arc::new(AtomicU64::new(1_000)),
            bulletin_level: Arc::new(RwLock::new("WARN".to_string())),
            concurrent_tasks: Arc::new(AtomicU64::new(1)),
            comments: Arc::new(RwLock::new(String::new())),
            auto_terminated_relationships: Arc::new(RwLock::new(Vec::new())),
        });

        tracing::info!(name, type_name, "Hot-added processor");
        self.audit_logger.log(&AuditEvent::success_with_details(
            AuditAction::ProcessorCreated,
            AuditTarget::processor(name),
            format!("type={}", type_name),
        ));
        Ok(())
    }

    /// Remove a processor at runtime (TOCTOU-safe: write lock held for full operation).
    pub async fn handle_remove_processor(
        &self,
        name: &str,
    ) -> std::result::Result<(), MutationError> {
        {
            let conns = self.live_conns.read();
            let conn_ids: Vec<String> = conns
                .iter()
                .filter(|c| c.source_name == name || c.dest_name == name)
                .map(|c| c.id.clone())
                .collect();
            if !conn_ids.is_empty() {
                return Err(MutationError::ProcessorHasConnections(conn_ids.join(", ")));
            }
        }

        let mut procs = self.live_procs.write();

        let info = procs
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| MutationError::ProcessorNotFound(name.to_string()))?;

        let enabled = info
            .metrics
            .enabled
            .load(std::sync::atomic::Ordering::Relaxed);
        let active = info
            .metrics
            .active
            .load(std::sync::atomic::Ordering::Relaxed);
        if enabled || active {
            return Err(MutationError::ProcessorNotStopped);
        }

        if let Some(token) = self.proc_tokens.lock().remove(name) {
            token.cancel();
        }

        procs.retain(|p| p.name != name);

        tracing::info!(name, "Hot-removed processor");
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ProcessorRemoved,
            AuditTarget::processor(name),
        ));
        Ok(())
    }

    /// Add a connection at runtime and wire it into processor data paths.
    pub fn handle_add_connection(
        &self,
        conn_id: String,
        source_name: &str,
        relationship: &str,
        dest_name: &str,
        config: BackPressureConfig,
        load_balance: Option<LoadBalanceConfig>,
    ) -> std::result::Result<String, MutationError> {
        let (src_output_h, src_rel, dst_input_h, dst_notifiers_h) = {
            let procs = self.live_procs.read();

            let src = procs
                .iter()
                .find(|p| p.name == source_name)
                .ok_or_else(|| MutationError::ProcessorNotFound(source_name.to_string()))?;

            let src_rel_info = src
                .relationships
                .iter()
                .find(|r| r.name == relationship)
                .ok_or_else(|| {
                    MutationError::UnknownRelationship(
                        relationship.to_string(),
                        source_name.to_string(),
                    )
                })?;

            let dst = procs
                .iter()
                .find(|p| p.name == dest_name)
                .ok_or_else(|| MutationError::ProcessorNotFound(dest_name.to_string()))?;

            let rel = Relationship {
                name: Box::leak(src_rel_info.name.clone().into_boxed_str()),
                description: Box::leak(src_rel_info.description.clone().into_boxed_str()),
                auto_terminated: src_rel_info.auto_terminated,
            };

            (
                Arc::clone(&src.output_connections),
                rel,
                Arc::clone(&dst.input_connections),
                Arc::clone(&dst.input_notifiers),
            )
        };

        {
            let conns = self.live_conns.read();
            if conns.iter().any(|c| {
                c.source_name == source_name
                    && c.relationship == relationship
                    && c.dest_name == dest_name
            }) {
                return Err(MutationError::DuplicateConnection(
                    source_name.to_string(),
                    relationship.to_string(),
                    dest_name.to_string(),
                ));
            }
        }

        let fc = if let Some(lb_config) = load_balance {
            Arc::new(FlowConnection::with_load_balance(
                conn_id.clone(),
                config,
                lb_config,
            ))
        } else {
            Arc::new(FlowConnection::new(conn_id.clone(), config))
        };

        let notifier = fc.notifier();
        dst_input_h.write().push(Arc::clone(&fc));
        dst_notifiers_h.write().push(notifier);
        src_output_h.write().push((src_rel, Arc::clone(&fc)));

        let query = Arc::new(FlowConnectionQuery::new(
            source_name.to_string(),
            relationship.to_string(),
            dest_name.to_string(),
            Arc::clone(&fc),
        ));
        self.live_conns.write().push(ConnectionInfo {
            id: conn_id.clone(),
            source_name: source_name.to_string(),
            relationship: relationship.to_string(),
            dest_name: dest_name.to_string(),
            connection: query,
        });

        tracing::info!(
            id = %conn_id,
            source = source_name,
            relationship,
            destination = dest_name,
            "Hot-added connection"
        );
        self.audit_logger.log(&AuditEvent::success_with_details(
            AuditAction::ConnectionCreated,
            AuditTarget::connection(&conn_id),
            format!("{} -[{}]-> {}", source_name, relationship, dest_name),
        ));
        Ok(conn_id)
    }

    /// Remove a connection at runtime.
    pub fn handle_remove_connection(
        &self,
        id: &str,
        force: bool,
    ) -> std::result::Result<(), MutationError> {
        let queue_count = {
            let conns = self.live_conns.read();
            let info = conns
                .iter()
                .find(|c| c.id == id)
                .ok_or_else(|| MutationError::ConnectionNotFound(id.to_string()))?;
            info.connection.queue_count()
        };

        if queue_count > 0 && !force {
            return Err(MutationError::QueueNotEmpty(queue_count));
        }

        if queue_count > 0 {
            let conns = self.live_conns.read();
            if let Some(info) = conns.iter().find(|c| c.id == id) {
                let removed = info.connection.clear_queue();
                tracing::warn!(
                    connection_id = %id,
                    discarded = removed,
                    "Force-removing connection with non-empty queue - FlowFiles discarded"
                );
            }
        }

        self.live_conns.write().retain(|c| c.id != id);

        tracing::info!(id, "Hot-removed connection");
        self.audit_logger.log(&AuditEvent::success(
            AuditAction::ConnectionRemoved,
            AuditTarget::connection(id),
        ));
        Ok(())
    }
}
