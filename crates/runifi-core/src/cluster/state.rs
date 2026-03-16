//! Cluster-scoped state provider for shared processor state.
//!
//! Mirrors the local state provider pattern but stores state under
//! `state/cluster/{component-id}/state.json`. In a single-node deployment
//! this behaves identically to local state. Once gossip-based state
//! replication is enabled (Phase 3), the coordinator will broadcast
//! `StateUpdate` messages to keep all nodes converged.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::state::StateMap;

/// On-disk representation of processor state.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: i64,
    entries: HashMap<String, String>,
}

/// In-memory state with a version counter, protected by a mutex.
#[derive(Debug)]
struct ProcessorState {
    version: i64,
    entries: HashMap<String, String>,
}

impl ProcessorState {
    fn empty() -> Self {
        Self {
            version: -1,
            entries: HashMap::new(),
        }
    }

    fn to_state_map(&self) -> StateMap {
        StateMap::new(self.entries.clone(), self.version)
    }
}

/// Cluster-scoped state provider that manages shared state for all processors.
///
/// Thread-safe: uses per-component mutexes so concurrent access to different
/// components does not contend.
///
/// In single-node mode this is a local file-backed store under `state/cluster/`.
/// In multi-node mode the coordinator will replicate state updates via the
/// `StateUpdate` / `StateSync` protocol messages.
pub struct ClusterStateProvider {
    /// Root directory for cluster state storage (e.g., `state/cluster/`).
    base_dir: PathBuf,
    /// Per-component state, lazily loaded from disk.
    states: dashmap::DashMap<String, Arc<Mutex<ProcessorState>>>,
}

impl ClusterStateProvider {
    /// Create a new cluster state provider rooted at the given directory.
    ///
    /// The directory is created if it does not exist.
    pub fn new(base_dir: impl Into<PathBuf>) -> ProcessResult<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self {
            base_dir,
            states: dashmap::DashMap::new(),
        })
    }

    /// Get or lazily load state for a component.
    fn get_or_load(&self, component_id: &str) -> Arc<Mutex<ProcessorState>> {
        if let Some(entry) = self.states.get(component_id) {
            return entry.clone();
        }

        // Load from disk or create empty.
        let state = self
            .load_from_disk(component_id)
            .unwrap_or_else(ProcessorState::empty);

        let state = Arc::new(Mutex::new(state));
        self.states
            .entry(component_id.to_string())
            .or_insert(state.clone());

        // Re-fetch in case another thread inserted first.
        self.states.get(component_id).unwrap().clone()
    }

    /// Load state from disk for a component.
    fn load_from_disk(&self, component_id: &str) -> Option<ProcessorState> {
        let path = self.state_file_path(component_id);
        let data = std::fs::read_to_string(&path).ok()?;
        let persisted: PersistedState = serde_json::from_str(&data).ok()?;
        Some(ProcessorState {
            version: persisted.version,
            entries: persisted.entries,
        })
    }

    /// Persist state to disk for a component.
    fn save_to_disk(&self, component_id: &str, state: &ProcessorState) -> ProcessResult<()> {
        let dir = self.component_dir(component_id);
        std::fs::create_dir_all(&dir)?;

        let persisted = PersistedState {
            version: state.version,
            entries: state.entries.clone(),
        };

        let data = serde_json::to_string_pretty(&persisted).map_err(|e| {
            PluginError::ProcessingFailed(format!("Failed to serialize cluster state: {}", e))
        })?;

        // Atomic write: write to temp file then rename.
        let tmp_path = dir.join("state.json.tmp");
        let final_path = dir.join("state.json");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    /// Remove persisted state from disk for a component.
    fn remove_from_disk(&self, component_id: &str) -> ProcessResult<()> {
        let dir = self.component_dir(component_id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    /// Get the directory path for a component's state.
    fn component_dir(&self, component_id: &str) -> PathBuf {
        self.base_dir.join(sanitize_id(component_id))
    }

    /// Get the state file path for a component.
    fn state_file_path(&self, component_id: &str) -> PathBuf {
        self.component_dir(component_id).join("state.json")
    }

    /// Get the current state for a component.
    pub fn get_state(&self, component_id: &str) -> ProcessResult<StateMap> {
        let state = self.get_or_load(component_id);
        let locked = state.lock();
        Ok(locked.to_state_map())
    }

    /// Set the state for a component, replacing any existing state.
    pub fn set_state(
        &self,
        component_id: &str,
        entries: HashMap<String, String>,
    ) -> ProcessResult<()> {
        let state = self.get_or_load(component_id);
        let mut locked = state.lock();
        locked.version += 1;
        locked.entries = entries;
        self.save_to_disk(component_id, &locked)?;
        Ok(())
    }

    /// Compare-and-swap: replace state only if the version matches.
    ///
    /// Returns `true` if replacement succeeded, `false` if version mismatch.
    pub fn replace(
        &self,
        component_id: &str,
        old_version: i64,
        new_entries: HashMap<String, String>,
    ) -> ProcessResult<bool> {
        let state = self.get_or_load(component_id);
        let mut locked = state.lock();

        if locked.version != old_version {
            return Ok(false);
        }

        locked.version += 1;
        locked.entries = new_entries;
        self.save_to_disk(component_id, &locked)?;
        Ok(true)
    }

    /// Clear all state for a component (resets version to -1).
    pub fn clear(&self, component_id: &str) -> ProcessResult<()> {
        let state = self.get_or_load(component_id);
        let mut locked = state.lock();
        locked.version = -1;
        locked.entries.clear();
        self.remove_from_disk(component_id)?;
        Ok(())
    }

    /// Remove all state for a component (called when processor is removed from flow).
    pub fn remove_component(&self, component_id: &str) -> ProcessResult<()> {
        self.states.remove(component_id);
        self.remove_from_disk(component_id)
    }
}

/// Shared reference to a ClusterStateProvider.
pub type SharedClusterStateProvider = Arc<ClusterStateProvider>;

/// Sanitize a component ID for use as a directory name.
fn sanitize_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
        assert_eq!(state.version(), -1);
    }

    #[test]
    fn test_set_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        provider.set_state("proc-1", entries).unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.get("key1"), Some("value1"));
        assert_eq!(state.get("key2"), Some("value2"));
    }

    #[test]
    fn test_replace_success() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries).unwrap();

        let new_entries = HashMap::from([("k".to_string(), "v2".to_string())]);
        let replaced = provider.replace("proc-1", 0, new_entries).unwrap();
        assert!(replaced);

        let state = provider.get_state("proc-1").unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.get("k"), Some("v2"));
    }

    #[test]
    fn test_replace_version_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries).unwrap();

        let new_entries = HashMap::from([("k".to_string(), "v2".to_string())]);
        let replaced = provider.replace("proc-1", 99, new_entries).unwrap();
        assert!(!replaced);

        let state = provider.get_state("proc-1").unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.get("k"), Some("v1"));
    }

    #[test]
    fn test_clear() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v".to_string())]);
        provider.set_state("proc-1", entries).unwrap();
        provider.clear("proc-1").unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
        assert_eq!(state.version(), -1);
    }

    #[test]
    fn test_persistence_across_instances() {
        let dir = tempfile::tempdir().unwrap();

        {
            let provider = ClusterStateProvider::new(dir.path()).unwrap();
            let entries = HashMap::from([("cursor".to_string(), "100".to_string())]);
            provider.set_state("proc-1", entries).unwrap();
        }

        {
            let provider = ClusterStateProvider::new(dir.path()).unwrap();
            let state = provider.get_state("proc-1").unwrap();
            assert_eq!(state.version(), 0);
            assert_eq!(state.get("cursor"), Some("100"));
        }
    }

    #[test]
    fn test_multiple_components() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let e1 = HashMap::from([("k".to_string(), "comp1".to_string())]);
        let e2 = HashMap::from([("k".to_string(), "comp2".to_string())]);
        provider.set_state("comp-1", e1).unwrap();
        provider.set_state("comp-2", e2).unwrap();

        assert_eq!(
            provider.get_state("comp-1").unwrap().get("k"),
            Some("comp1")
        );
        assert_eq!(
            provider.get_state("comp-2").unwrap().get("k"),
            Some("comp2")
        );

        provider.clear("comp-1").unwrap();
        assert!(provider.get_state("comp-1").unwrap().is_empty());
        assert_eq!(
            provider.get_state("comp-2").unwrap().get("k"),
            Some("comp2")
        );
    }

    #[test]
    fn test_remove_component() {
        let dir = tempfile::tempdir().unwrap();
        let provider = ClusterStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v".to_string())]);
        provider.set_state("proc-1", entries).unwrap();
        provider.remove_component("proc-1").unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
    }
}
