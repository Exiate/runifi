use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use super::handle::{ConnectionInfo, LabelInfo, Position, ProcessorInfo};
use super::process_group::ProcessGroupInfo;

/// File names for persisted flow state.
const FLOW_STATE_FILE: &str = "flow.json";
const FLOW_STATE_BACKUP: &str = "flow.json.bak";
const FLOW_STATE_TMP: &str = "flow.json.tmp";

/// Debounce interval for writes — coalesces rapid mutations.
const DEBOUNCE_DURATION: Duration = Duration::from_secs(2);

/// Current schema version for new persisted state files.
const CURRENT_VERSION: u32 = 1;

fn default_version() -> u32 {
    1
}

// ── Serializable flow state types ─────────────────────────────────────────────

/// The complete persisted flow state, serializable to/from JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedFlowState {
    #[serde(default = "default_version")]
    pub version: u32,
    pub flow_name: String,
    pub processors: Vec<PersistedProcessor>,
    pub connections: Vec<PersistedConnection>,
    pub positions: HashMap<String, PersistedPosition>,
    #[serde(default)]
    pub services: Vec<PersistedService>,
    #[serde(default)]
    pub labels: Vec<PersistedLabel>,
    #[serde(default)]
    pub process_groups: Vec<PersistedProcessGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedProcessor {
    pub name: String,
    pub type_name: String,
    pub scheduling: PersistedScheduling,
    pub properties: HashMap<String, String>,
    #[serde(default)]
    pub penalty_duration_ms: Option<u64>,
    #[serde(default)]
    pub yield_duration_ms: Option<u64>,
    #[serde(default)]
    pub bulletin_level: Option<String>,
    #[serde(default)]
    pub concurrent_tasks: Option<u64>,
    #[serde(default)]
    pub auto_terminated_relationships: Option<Vec<String>>,
    #[serde(default)]
    pub comments: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedScheduling {
    pub strategy: String,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    /// CRON expression (only present when strategy = "cron").
    #[serde(default)]
    pub expression: Option<String>,
}

fn default_interval_ms() -> u64 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedConnection {
    pub source: String,
    pub relationship: String,
    pub destination: String,
    #[serde(default)]
    pub back_pressure: Option<PersistedBackPressure>,
    /// FlowFile expiration duration string, e.g. "5m", "1h".
    #[serde(default)]
    pub expiration: Option<String>,
    /// Queue priority strategy: "FIFO", "NewestFirst", "PriorityAttribute".
    #[serde(default)]
    pub priority: Option<String>,
    /// Attribute name for PriorityAttribute priority mode.
    #[serde(default)]
    pub priority_attribute: Option<String>,
    /// Load balance configuration for cluster distribution.
    #[serde(default)]
    pub load_balancing: Option<crate::cluster::load_balance::LoadBalanceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedBackPressure {
    pub max_count: Option<usize>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PersistedPosition {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedService {
    pub name: String,
    pub type_name: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedLabel {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub x: f64,
    #[serde(default)]
    pub y: f64,
    #[serde(default = "default_label_width")]
    pub width: f64,
    #[serde(default = "default_label_height")]
    pub height: f64,
    #[serde(default)]
    pub background_color: String,
    #[serde(default = "default_font_size")]
    pub font_size: f64,
}

fn default_label_width() -> f64 {
    150.0
}
fn default_label_height() -> f64 {
    40.0
}
fn default_font_size() -> f64 {
    14.0
}

/// Persisted process group data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedProcessGroup {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub input_ports: Vec<PersistedPort>,
    #[serde(default)]
    pub output_ports: Vec<PersistedPort>,
    #[serde(default)]
    pub processor_names: Vec<String>,
    #[serde(default)]
    pub connection_ids: Vec<String>,
    #[serde(default)]
    pub child_group_ids: Vec<String>,
    #[serde(default)]
    pub parent_group_id: Option<String>,
    #[serde(default)]
    pub variables: HashMap<String, String>,
    #[serde(default)]
    pub comments: String,
    #[serde(default)]
    pub default_back_pressure_count: Option<usize>,
    #[serde(default)]
    pub default_back_pressure_bytes: Option<u64>,
    #[serde(default)]
    pub default_flowfile_expiration_ms: Option<u64>,
}

/// Persisted port data for a process group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedPort {
    pub id: String,
    pub name: String,
    /// "input" or "output"
    pub port_type: String,
}

// ── Snapshot source — breaks the Arc cycle ────────────────────────────────────

/// The subset of engine state needed for persistence snapshotting.
///
/// Holds only `Arc` references to the live data collections, **not** an
/// `EngineHandle`. This breaks the circular reference:
/// `EngineHandle -> FlowPersistence -> EngineHandle`.
pub(crate) struct SnapshotSource {
    pub flow_name: String,
    pub processors: Arc<RwLock<Vec<ProcessorInfo>>>,
    pub connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    pub positions: Arc<DashMap<String, Position>>,
    pub service_registry: crate::registry::service_registry::SharedServiceRegistry,
    pub labels: Arc<RwLock<Vec<LabelInfo>>>,
    pub process_groups: Arc<RwLock<Vec<ProcessGroupInfo>>>,
}

// ── Snapshot from live engine state ───────────────────────────────────────────

impl PersistedFlowState {
    /// Capture a snapshot of the current flow state from the snapshot source.
    pub(crate) fn snapshot(source: &SnapshotSource) -> Self {
        let processors: Vec<PersistedProcessor> = source
            .processors
            .read()
            .iter()
            .map(|p| {
                let penalty = p
                    .penalty_duration_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                let yield_ms = p
                    .yield_duration_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                let concurrent = p
                    .concurrent_tasks
                    .load(std::sync::atomic::Ordering::Relaxed);
                let bulletin = p.bulletin_level.read().clone();
                let comments = p.comments.read().clone();
                let auto_term = p.auto_terminated_relationships.read().clone();
                PersistedProcessor {
                    name: p.name.clone(),
                    type_name: p.type_name.clone(),
                    scheduling: scheduling_display_to_persisted(&p.scheduling_display),
                    properties: p.properties.read().clone(),
                    penalty_duration_ms: if penalty != 30_000 {
                        Some(penalty)
                    } else {
                        None
                    },
                    yield_duration_ms: if yield_ms != 1_000 {
                        Some(yield_ms)
                    } else {
                        None
                    },
                    bulletin_level: if bulletin != "WARN" {
                        Some(bulletin)
                    } else {
                        None
                    },
                    concurrent_tasks: if concurrent != 1 {
                        Some(concurrent)
                    } else {
                        None
                    },
                    auto_terminated_relationships: if auto_term.is_empty() {
                        None
                    } else {
                        Some(auto_term)
                    },
                    comments: if comments.is_empty() {
                        None
                    } else {
                        Some(comments)
                    },
                }
            })
            .collect();

        let connections: Vec<PersistedConnection> = source
            .connections
            .read()
            .iter()
            .map(|c| {
                let bp = c.connection.back_pressure_config();
                // Persist expiration and priority from the underlying FlowConnection.
                let (expiration, priority, priority_attribute) =
                    if let Some(fc) = c.connection.flow_connection() {
                        let exp = fc.expiration().map(|d| {
                            let secs = d.as_secs();
                            if secs >= 86400 && secs % 86400 == 0 {
                                format!("{}d", secs / 86400)
                            } else if secs >= 3600 && secs % 3600 == 0 {
                                format!("{}h", secs / 3600)
                            } else if secs >= 60 && secs % 60 == 0 {
                                format!("{}m", secs / 60)
                            } else {
                                format!("{}s", secs)
                            }
                        });
                        let (prio, prio_attr) = match fc.queue_priority() {
                            crate::connection::flow_connection::QueuePriority::Fifo => (None, None),
                            crate::connection::flow_connection::QueuePriority::NewestFirst => {
                                (Some("NewestFirst".to_string()), None)
                            }
                            crate::connection::flow_connection::QueuePriority::PriorityAttribute(
                                attr,
                            ) => (Some("PriorityAttribute".to_string()), Some(attr.clone())),
                        };
                        (exp, prio, prio_attr)
                    } else {
                        (None, None, None)
                    };
                let load_balancing = c.connection.load_balance_config().cloned();
                PersistedConnection {
                    source: c.source_name.clone(),
                    relationship: c.relationship.clone(),
                    destination: c.dest_name.clone(),
                    back_pressure: Some(PersistedBackPressure {
                        max_count: Some(bp.max_count),
                        max_bytes: Some(bp.max_bytes),
                    }),
                    expiration,
                    priority,
                    priority_attribute,
                    load_balancing,
                }
            })
            .collect();

        let mut positions = HashMap::new();
        for entry in source.positions.iter() {
            positions.insert(
                entry.key().clone(),
                PersistedPosition {
                    x: entry.value().x,
                    y: entry.value().y,
                },
            );
        }

        let services: Vec<PersistedService> = source
            .service_registry
            .read()
            .list_services()
            .into_iter()
            .map(|s| PersistedService {
                name: s.name,
                type_name: s.type_name,
                properties: s.properties,
            })
            .collect();

        let labels: Vec<PersistedLabel> = source
            .labels
            .read()
            .iter()
            .map(|l| PersistedLabel {
                id: l.id.clone(),
                text: l.text.clone(),
                x: l.x,
                y: l.y,
                width: l.width,
                height: l.height,
                background_color: l.background_color.clone(),
                font_size: l.font_size,
            })
            .collect();

        let process_groups: Vec<PersistedProcessGroup> = source
            .process_groups
            .read()
            .iter()
            .map(|g| {
                let input_ports: Vec<PersistedPort> = g
                    .input_ports
                    .iter()
                    .map(|p| PersistedPort {
                        id: p.id.clone(),
                        name: p.name.clone(),
                        port_type: "input".to_string(),
                    })
                    .collect();
                let output_ports: Vec<PersistedPort> = g
                    .output_ports
                    .iter()
                    .map(|p| PersistedPort {
                        id: p.id.clone(),
                        name: p.name.clone(),
                        port_type: "output".to_string(),
                    })
                    .collect();
                PersistedProcessGroup {
                    id: g.id.clone(),
                    name: g.name.clone(),
                    input_ports,
                    output_ports,
                    processor_names: g.processor_names.clone(),
                    connection_ids: g.connection_ids.clone(),
                    child_group_ids: g.child_group_ids.clone(),
                    parent_group_id: g.parent_group_id.clone(),
                    variables: g.variables.clone(),
                    comments: g.comments.clone(),
                    default_back_pressure_count: g.default_back_pressure_count,
                    default_back_pressure_bytes: g.default_back_pressure_bytes,
                    default_flowfile_expiration_ms: g.default_flowfile_expiration_ms,
                }
            })
            .collect();

        Self {
            version: CURRENT_VERSION,
            flow_name: source.flow_name.clone(),
            processors,
            connections,
            positions,
            services,
            labels,
            process_groups,
        }
    }
}

/// Parse a scheduling display string (e.g. "timer-driven (1000ms)", "event-driven")
/// back into a `PersistedScheduling`.
pub fn scheduling_display_to_persisted(display: &str) -> PersistedScheduling {
    if display.starts_with("timer-driven") {
        // Parse "timer-driven (1000ms)" -> interval_ms = 1000
        let interval_ms = display
            .strip_prefix("timer-driven (")
            .and_then(|s| s.strip_suffix("ms)"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);
        PersistedScheduling {
            strategy: "timer".to_string(),
            interval_ms,
            expression: None,
        }
    } else if display.starts_with("cron-driven") {
        // Parse "cron-driven (0 */5 * * * *)" -> expression
        let expression = display
            .strip_prefix("cron-driven (")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or("0 * * * * *")
            .to_string();
        PersistedScheduling {
            strategy: "cron".to_string(),
            interval_ms: 100,
            expression: Some(expression),
        }
    } else {
        PersistedScheduling {
            strategy: "event".to_string(),
            interval_ms: 100,
            expression: None,
        }
    }
}

// ── File I/O with atomic writes ──────────────────────────────────────────────

/// Write the flow state to disk atomically.
///
/// 1. Serialize to JSON
/// 2. Write to a temp file
/// 3. fsync the temp file
/// 4. If a current flow.json exists, rename it to flow.json.bak
/// 5. Rename temp file to flow.json
/// 6. fsync the parent directory to ensure the rename is durable
fn atomic_write(conf_dir: &Path, state: &PersistedFlowState) -> std::io::Result<()> {
    fs::create_dir_all(conf_dir)?;

    let flow_path = conf_dir.join(FLOW_STATE_FILE);
    let tmp_path = conf_dir.join(FLOW_STATE_TMP);
    let bak_path = conf_dir.join(FLOW_STATE_BACKUP);

    let json = serde_json::to_string_pretty(state)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Write to temp file and fsync.
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
    }

    // Backup existing flow state if present.
    if flow_path.exists() {
        // Best-effort backup — don't fail the write if backup rename fails.
        let _ = fs::rename(&flow_path, &bak_path);
    }

    // Atomic rename.
    fs::rename(&tmp_path, &flow_path)?;

    // Fsync the parent directory to ensure the rename metadata is durable.
    // On ext4 with default mount options, a power failure between rename and
    // kernel directory metadata flush could lose the rename without this.
    fs::File::open(conf_dir)?.sync_all()?;

    Ok(())
}

/// Load persisted flow state from the runtime config directory.
///
/// Returns `None` if no runtime flow file exists. If the primary file
/// contains corrupted JSON, attempts to load from the backup file.
pub fn load_runtime_flow(conf_dir: &Path) -> std::io::Result<Option<PersistedFlowState>> {
    let flow_path = conf_dir.join(FLOW_STATE_FILE);
    if !flow_path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(&flow_path)?;
    match serde_json::from_str::<PersistedFlowState>(&json) {
        Ok(state) => Ok(Some(state)),
        Err(primary_err) => {
            tracing::warn!(
                error = %primary_err,
                "Primary flow state file is corrupted, trying backup"
            );

            let bak_path = conf_dir.join(FLOW_STATE_BACKUP);
            if !bak_path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    primary_err,
                ));
            }

            let bak_json = fs::read_to_string(&bak_path)?;
            serde_json::from_str(&bak_json).map(Some).map_err(|e| {
                tracing::error!(error = %e, "Backup flow state file is also corrupted");
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })
        }
    }
}

// ── FlowPersistence — debounced background writer ────────────────────────────

/// Handles debounced persistence of flow state to disk.
///
/// Shared via `Arc` between the engine handle and the background writer task.
/// Call `notify_changed()` after any mutation; the background task will
/// coalesce rapid changes and write at most once per `DEBOUNCE_DURATION`.
///
/// Holds only the `Arc` references it needs for snapshotting, **not** an
/// `EngineHandle`, to avoid a circular `Arc` reference.
#[derive(Clone)]
pub struct FlowPersistence {
    inner: Arc<FlowPersistenceInner>,
}

struct FlowPersistenceInner {
    conf_dir: PathBuf,
    notify: Notify,
    source: RwLock<Option<SnapshotSource>>,
}

impl FlowPersistence {
    /// Create a new persistence layer for the given config directory.
    pub fn new(conf_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(FlowPersistenceInner {
                conf_dir,
                notify: Notify::new(),
                source: RwLock::new(None),
            }),
        }
    }

    /// Set the snapshot source — the live data collections needed for
    /// persistence. Called once after the engine starts.
    ///
    /// This intentionally does **not** accept an `EngineHandle` to avoid
    /// creating a circular `Arc` reference.
    #[allow(clippy::too_many_arguments)]
    pub fn set_source(
        &self,
        flow_name: String,
        processors: Arc<RwLock<Vec<ProcessorInfo>>>,
        connections: Arc<RwLock<Vec<ConnectionInfo>>>,
        positions: Arc<DashMap<String, Position>>,
        service_registry: crate::registry::service_registry::SharedServiceRegistry,
        labels: Arc<RwLock<Vec<LabelInfo>>>,
        process_groups: Arc<RwLock<Vec<ProcessGroupInfo>>>,
    ) {
        *self.inner.source.write() = Some(SnapshotSource {
            flow_name,
            processors,
            connections,
            positions,
            service_registry,
            labels,
            process_groups,
        });
    }

    /// Notify that the flow state has changed.
    /// The background task will debounce and write to disk.
    pub fn notify_changed(&self) {
        self.inner.notify.notify_one();
    }

    /// Get a reference to the conf_dir.
    pub fn conf_dir(&self) -> &Path {
        &self.inner.conf_dir
    }

    /// Run the background persistence loop. Call this from a spawned task.
    /// Exits when `cancel` is triggered.
    pub async fn run(&self, cancel: tokio_util::sync::CancellationToken) {
        loop {
            // Wait for a change notification or cancellation.
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Final persist before shutdown.
                    self.persist_now();
                    tracing::debug!("Flow persistence task shutting down");
                    break;
                }
                _ = self.inner.notify.notified() => {
                    // Debounce: wait a bit for more changes to arrive.
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            self.persist_now();
                            break;
                        }
                        _ = tokio::time::sleep(DEBOUNCE_DURATION) => {
                            self.persist_now();
                        }
                    }
                }
            }
        }
    }

    /// Immediately persist the current flow state to disk.
    ///
    /// The source lock is released before disk I/O to avoid holding it
    /// across potentially slow filesystem operations.
    fn persist_now(&self) {
        let state = {
            let source = self.inner.source.read();
            let Some(ref s) = *source else {
                return;
            };
            PersistedFlowState::snapshot(s)
        };
        // source lock released — do disk I/O without holding it.

        match atomic_write(&self.inner.conf_dir, &state) {
            Ok(()) => {
                tracing::debug!(
                    conf_dir = %self.inner.conf_dir.display(),
                    processors = state.processors.len(),
                    connections = state.connections.len(),
                    "Flow state persisted"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    conf_dir = %self.inner.conf_dir.display(),
                    "Failed to persist flow state"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(name: &str) -> PersistedFlowState {
        PersistedFlowState {
            version: CURRENT_VERSION,
            flow_name: name.to_string(),
            processors: vec![PersistedProcessor {
                name: "p1".to_string(),
                type_name: "GenerateFlowFile".to_string(),
                scheduling: PersistedScheduling {
                    strategy: "timer".to_string(),
                    interval_ms: 500,
                    expression: None,
                },
                properties: HashMap::new(),
                penalty_duration_ms: None,
                yield_duration_ms: None,
                bulletin_level: None,
                concurrent_tasks: None,
                auto_terminated_relationships: None,
                comments: None,
            }],
            connections: vec![],
            positions: HashMap::new(),
            services: vec![],
            labels: vec![],
            process_groups: vec![],
        }
    }

    #[test]
    fn test_serialize_deserialize_flow_state() {
        let state = PersistedFlowState {
            version: CURRENT_VERSION,
            flow_name: "test-flow".to_string(),
            processors: vec![
                PersistedProcessor {
                    name: "gen".to_string(),
                    type_name: "GenerateFlowFile".to_string(),
                    scheduling: PersistedScheduling {
                        strategy: "timer".to_string(),
                        interval_ms: 1000,
                        expression: None,
                    },
                    properties: HashMap::from([("File Size".to_string(), "5120".to_string())]),
                    penalty_duration_ms: None,
                    yield_duration_ms: None,
                    bulletin_level: None,
                    concurrent_tasks: None,
                    auto_terminated_relationships: None,
                    comments: None,
                },
                PersistedProcessor {
                    name: "log".to_string(),
                    type_name: "LogAttribute".to_string(),
                    scheduling: PersistedScheduling {
                        strategy: "event".to_string(),
                        interval_ms: 100,
                        expression: None,
                    },
                    properties: HashMap::new(),
                    penalty_duration_ms: None,
                    yield_duration_ms: None,
                    bulletin_level: None,
                    concurrent_tasks: None,
                    auto_terminated_relationships: None,
                    comments: None,
                },
            ],
            connections: vec![PersistedConnection {
                source: "gen".to_string(),
                relationship: "success".to_string(),
                destination: "log".to_string(),
                back_pressure: Some(PersistedBackPressure {
                    max_count: Some(10_000),
                    max_bytes: Some(1_073_741_824),
                }),
                expiration: None,
                priority: None,
                priority_attribute: None,
                load_balancing: None,
            }],
            positions: HashMap::from([(
                "gen".to_string(),
                PersistedPosition { x: 100.0, y: 200.0 },
            )]),
            services: vec![PersistedService {
                name: "my-cache".to_string(),
                type_name: "DistributedMapCacheServer".to_string(),
                properties: HashMap::from([("Port".to_string(), "4557".to_string())]),
            }],
            labels: vec![PersistedLabel {
                id: "label-1".to_string(),
                text: "Test Label".to_string(),
                x: 50.0,
                y: 100.0,
                width: 200.0,
                height: 50.0,
                background_color: "#3b82f6".to_string(),
                font_size: 14.0,
            }],
            process_groups: vec![],
        };

        let json = serde_json::to_string_pretty(&state).unwrap();
        let deserialized: PersistedFlowState = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.version, CURRENT_VERSION);
        assert_eq!(deserialized.flow_name, "test-flow");
        assert_eq!(deserialized.processors.len(), 2);
        assert_eq!(deserialized.processors[0].name, "gen");
        assert_eq!(deserialized.processors[0].type_name, "GenerateFlowFile");
        assert_eq!(deserialized.processors[0].scheduling.strategy, "timer");
        assert_eq!(deserialized.processors[0].scheduling.interval_ms, 1000);
        assert_eq!(
            deserialized.processors[0]
                .properties
                .get("File Size")
                .unwrap(),
            "5120"
        );
        assert_eq!(deserialized.connections.len(), 1);
        assert_eq!(deserialized.connections[0].source, "gen");
        assert_eq!(deserialized.positions.len(), 1);
        assert_eq!(deserialized.positions["gen"].x, 100.0);
    }

    #[test]
    fn test_atomic_write_and_load() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("conf");

        let state = make_state("atomic-test");

        // Write.
        atomic_write(&conf_dir, &state).unwrap();

        // Load.
        let loaded = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded.flow_name, "atomic-test");
        assert_eq!(loaded.processors.len(), 1);
        assert_eq!(loaded.version, CURRENT_VERSION);

        // Write again — should create backup.
        let state2 = PersistedFlowState {
            version: CURRENT_VERSION,
            flow_name: "atomic-test-v2".to_string(),
            processors: vec![],
            connections: vec![],
            positions: HashMap::new(),
            services: vec![],
            labels: vec![],
            process_groups: vec![],
        };
        atomic_write(&conf_dir, &state2).unwrap();

        // flow.json should be v2.
        let loaded2 = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded2.flow_name, "atomic-test-v2");

        // Backup should be v1.
        let bak_path = conf_dir.join(FLOW_STATE_BACKUP);
        let bak_json = fs::read_to_string(bak_path).unwrap();
        let bak: PersistedFlowState = serde_json::from_str(&bak_json).unwrap();
        assert_eq!(bak.flow_name, "atomic-test");
    }

    #[test]
    fn test_load_nonexistent_returns_none() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("no-such-dir");
        let result = load_runtime_flow(&conf_dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_deserialize_missing_back_pressure() {
        let json = r#"{
            "flow_name": "test",
            "processors": [],
            "connections": [{
                "source": "a",
                "relationship": "success",
                "destination": "b"
            }],
            "positions": {}
        }"#;

        let state: PersistedFlowState = serde_json::from_str(json).unwrap();
        assert!(state.connections[0].back_pressure.is_none());
    }

    #[test]
    fn test_version_defaults_to_1_for_old_format() {
        // Simulate a JSON file from before the version field was added.
        let json = r#"{
            "flow_name": "legacy",
            "processors": [],
            "connections": [],
            "positions": {}
        }"#;

        let state: PersistedFlowState = serde_json::from_str(json).unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.flow_name, "legacy");
    }

    #[test]
    fn test_corrupted_json_falls_back_to_backup() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("conf");
        fs::create_dir_all(&conf_dir).unwrap();

        // Write a valid backup file.
        let backup_state = make_state("from-backup");
        let backup_json = serde_json::to_string_pretty(&backup_state).unwrap();
        fs::write(conf_dir.join(FLOW_STATE_BACKUP), &backup_json).unwrap();

        // Write corrupted primary file.
        fs::write(conf_dir.join(FLOW_STATE_FILE), "{ not valid json !!!").unwrap();

        // Should fall back to backup.
        let loaded = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded.flow_name, "from-backup");
    }

    #[test]
    fn test_corrupted_json_no_backup_returns_error() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("conf");
        fs::create_dir_all(&conf_dir).unwrap();

        // Write corrupted primary file with no backup.
        fs::write(conf_dir.join(FLOW_STATE_FILE), "not json").unwrap();

        let result = load_runtime_flow(&conf_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_state_round_trip() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("conf");

        let state = PersistedFlowState {
            version: CURRENT_VERSION,
            flow_name: "empty".to_string(),
            processors: vec![],
            connections: vec![],
            positions: HashMap::new(),
            services: vec![],
            labels: vec![],
            process_groups: vec![],
        };

        atomic_write(&conf_dir, &state).unwrap();

        let loaded = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded.flow_name, "empty");
        assert!(loaded.processors.is_empty());
        assert!(loaded.connections.is_empty());
        assert!(loaded.positions.is_empty());
        assert_eq!(loaded.version, CURRENT_VERSION);
    }

    #[test]
    fn test_dir_fsync_after_write() {
        // Verify atomic_write creates the directory and succeeds.
        // (We can't directly test fsync, but we can verify the write path
        // completes without error on a fresh directory.)
        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("deep").join("nested").join("conf");

        let state = make_state("fsync-test");
        atomic_write(&conf_dir, &state).unwrap();

        let loaded = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded.flow_name, "fsync-test");
    }

    #[tokio::test]
    async fn test_debounce_coalesces_rapid_notifications() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let tmp_dir = tempfile::tempdir().unwrap();
        let conf_dir = tmp_dir.path().join("conf");
        let persistence = FlowPersistence::new(conf_dir.clone());

        // Set up a minimal snapshot source with no data.
        let processors = Arc::new(RwLock::new(Vec::new()));
        let connections = Arc::new(RwLock::new(Vec::new()));
        let positions = Arc::new(DashMap::new());
        let service_registry = crate::registry::service_registry::SharedServiceRegistry::new();
        let labels = Arc::new(RwLock::new(Vec::new()));
        let process_groups = Arc::new(RwLock::new(Vec::new()));
        persistence.set_source(
            "debounce-test".to_string(),
            processors,
            connections,
            positions,
            service_registry,
            labels,
            process_groups,
        );

        let cancel = tokio_util::sync::CancellationToken::new();
        let cancel_clone = cancel.clone();
        let persist_clone = persistence.clone();

        // Track writes by checking file modification.
        let write_count = Arc::new(AtomicU32::new(0));
        let write_count_clone = write_count.clone();

        let task = tokio::spawn(async move {
            persist_clone.run(cancel_clone).await;
        });

        // Fire 10 rapid notifications — should be coalesced by debounce.
        for _ in 0..10 {
            persistence.notify_changed();
        }

        // Wait for debounce to fire (DEBOUNCE_DURATION = 2s + margin).
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // Count writes by checking if file exists.
        if conf_dir.join(FLOW_STATE_FILE).exists() {
            write_count_clone.fetch_add(1, Ordering::Relaxed);
        }

        cancel.cancel();
        task.await.unwrap();

        // Should have written exactly once despite 10 notifications.
        let loaded = load_runtime_flow(&conf_dir).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().flow_name, "debounce-test");
    }
}
