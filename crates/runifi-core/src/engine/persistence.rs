use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use super::handle::EngineHandle;
use super::processor_node::SchedulingStrategy;

/// File names for persisted flow state.
const FLOW_STATE_FILE: &str = "flow.json";
const FLOW_STATE_BACKUP: &str = "flow.json.bak";
const FLOW_STATE_TMP: &str = "flow.json.tmp";

/// Debounce interval for writes — coalesces rapid mutations.
const DEBOUNCE_DURATION: Duration = Duration::from_secs(2);

// ── Serializable flow state types ─────────────────────────────────────────────

/// The complete persisted flow state, serializable to/from JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedFlowState {
    pub flow_name: String,
    pub processors: Vec<PersistedProcessor>,
    pub connections: Vec<PersistedConnection>,
    pub positions: HashMap<String, PersistedPosition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedProcessor {
    pub name: String,
    pub type_name: String,
    pub scheduling: PersistedScheduling,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedScheduling {
    pub strategy: String,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
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

// ── Snapshot from live engine state ───────────────────────────────────────────

impl PersistedFlowState {
    /// Capture a snapshot of the current flow state from the engine handle.
    pub fn snapshot(handle: &EngineHandle) -> Self {
        let processors: Vec<PersistedProcessor> = handle
            .processors
            .read()
            .iter()
            .map(|p| PersistedProcessor {
                name: p.name.clone(),
                type_name: p.type_name.clone(),
                scheduling: scheduling_to_persisted(&p.scheduling),
                properties: p.properties.read().clone(),
            })
            .collect();

        let connections: Vec<PersistedConnection> = handle
            .connections
            .read()
            .iter()
            .map(|c| {
                let bp = c.connection.back_pressure_config();
                PersistedConnection {
                    source: c.source_name.clone(),
                    relationship: c.relationship.clone(),
                    destination: c.dest_name.clone(),
                    back_pressure: Some(PersistedBackPressure {
                        max_count: Some(bp.max_count),
                        max_bytes: Some(bp.max_bytes),
                    }),
                }
            })
            .collect();

        let mut positions = HashMap::new();
        for entry in handle.positions.iter() {
            positions.insert(
                entry.key().clone(),
                PersistedPosition {
                    x: entry.value().x,
                    y: entry.value().y,
                },
            );
        }

        Self {
            flow_name: handle.flow_name.clone(),
            processors,
            connections,
            positions,
        }
    }
}

fn scheduling_to_persisted(s: &SchedulingStrategy) -> PersistedScheduling {
    match s {
        SchedulingStrategy::TimerDriven { interval_ms } => PersistedScheduling {
            strategy: "timer".to_string(),
            interval_ms: *interval_ms,
        },
        SchedulingStrategy::EventDriven => PersistedScheduling {
            strategy: "event".to_string(),
            interval_ms: 100,
        },
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

    Ok(())
}

/// Load persisted flow state from the runtime config directory.
///
/// Returns `None` if no runtime flow file exists.
pub fn load_runtime_flow(conf_dir: &Path) -> std::io::Result<Option<PersistedFlowState>> {
    let flow_path = conf_dir.join(FLOW_STATE_FILE);
    if !flow_path.exists() {
        return Ok(None);
    }

    let json = fs::read_to_string(&flow_path)?;
    let state: PersistedFlowState = serde_json::from_str(&json)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    Ok(Some(state))
}

// ── FlowPersistence — debounced background writer ────────────────────────────

/// Handles debounced persistence of flow state to disk.
///
/// Shared via `Arc` between the engine handle and the background writer task.
/// Call `notify_changed()` after any mutation; the background task will
/// coalesce rapid changes and write at most once per `DEBOUNCE_DURATION`.
#[derive(Clone)]
pub struct FlowPersistence {
    inner: Arc<FlowPersistenceInner>,
}

struct FlowPersistenceInner {
    conf_dir: PathBuf,
    notify: Notify,
    handle: RwLock<Option<EngineHandle>>,
}

impl FlowPersistence {
    /// Create a new persistence layer for the given config directory.
    pub fn new(conf_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(FlowPersistenceInner {
                conf_dir,
                notify: Notify::new(),
                handle: RwLock::new(None),
            }),
        }
    }

    /// Set the engine handle. Called once after the engine starts.
    pub fn set_handle(&self, handle: EngineHandle) {
        *self.inner.handle.write() = Some(handle);
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
    fn persist_now(&self) {
        let handle = self.inner.handle.read();
        let Some(ref h) = *handle else {
            return;
        };

        let state = PersistedFlowState::snapshot(h);

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

    #[test]
    fn test_serialize_deserialize_flow_state() {
        let state = PersistedFlowState {
            flow_name: "test-flow".to_string(),
            processors: vec![
                PersistedProcessor {
                    name: "gen".to_string(),
                    type_name: "GenerateFlowFile".to_string(),
                    scheduling: PersistedScheduling {
                        strategy: "timer".to_string(),
                        interval_ms: 1000,
                    },
                    properties: HashMap::from([("File Size".to_string(), "5120".to_string())]),
                },
                PersistedProcessor {
                    name: "log".to_string(),
                    type_name: "LogAttribute".to_string(),
                    scheduling: PersistedScheduling {
                        strategy: "event".to_string(),
                        interval_ms: 100,
                    },
                    properties: HashMap::new(),
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
            }],
            positions: HashMap::from([(
                "gen".to_string(),
                PersistedPosition { x: 100.0, y: 200.0 },
            )]),
        };

        let json = serde_json::to_string_pretty(&state).unwrap();
        let deserialized: PersistedFlowState = serde_json::from_str(&json).unwrap();

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

        let state = PersistedFlowState {
            flow_name: "atomic-test".to_string(),
            processors: vec![PersistedProcessor {
                name: "p1".to_string(),
                type_name: "GenerateFlowFile".to_string(),
                scheduling: PersistedScheduling {
                    strategy: "timer".to_string(),
                    interval_ms: 500,
                },
                properties: HashMap::new(),
            }],
            connections: vec![],
            positions: HashMap::new(),
        };

        // Write.
        atomic_write(&conf_dir, &state).unwrap();

        // Load.
        let loaded = load_runtime_flow(&conf_dir).unwrap().unwrap();
        assert_eq!(loaded.flow_name, "atomic-test");
        assert_eq!(loaded.processors.len(), 1);

        // Write again — should create backup.
        let state2 = PersistedFlowState {
            flow_name: "atomic-test-v2".to_string(),
            processors: vec![],
            connections: vec![],
            positions: HashMap::new(),
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
}
