//! Local file-backed state provider for processor state persistence.
//!
//! Each processor instance gets a directory under `state/local/{processor-id}/`.
//! State is stored as a JSON file containing the key-value entries and a version.
//!
//! The state survives restarts and is cleared when the processor is removed
//! from the flow.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::state::{StateManager, StateMap, StateScope};

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

/// File-backed local state provider that manages state for all processors.
///
/// Thread-safe: uses per-processor mutexes so concurrent access to different
/// processors does not contend.
pub struct LocalStateProvider {
    /// Root directory for state storage (e.g., `state/local/`).
    base_dir: PathBuf,
    /// Per-processor state, lazily loaded from disk.
    states: dashmap::DashMap<String, Arc<Mutex<ProcessorState>>>,
}

impl LocalStateProvider {
    /// Create a new local state provider rooted at the given directory.
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

    /// Get or lazily load state for a processor.
    fn get_or_load(&self, processor_id: &str) -> Arc<Mutex<ProcessorState>> {
        if let Some(entry) = self.states.get(processor_id) {
            return entry.clone();
        }

        // Load from disk or create empty.
        let state = self
            .load_from_disk(processor_id)
            .unwrap_or_else(ProcessorState::empty);

        let state = Arc::new(Mutex::new(state));
        self.states
            .entry(processor_id.to_string())
            .or_insert(state.clone());

        // Re-fetch in case another thread inserted first.
        self.states.get(processor_id).unwrap().clone()
    }

    /// Load state from disk for a processor.
    fn load_from_disk(&self, processor_id: &str) -> Option<ProcessorState> {
        let path = self.state_file_path(processor_id);
        let data = std::fs::read_to_string(&path).ok()?;
        let persisted: PersistedState = serde_json::from_str(&data).ok()?;
        Some(ProcessorState {
            version: persisted.version,
            entries: persisted.entries,
        })
    }

    /// Persist state to disk for a processor.
    fn save_to_disk(&self, processor_id: &str, state: &ProcessorState) -> ProcessResult<()> {
        let dir = self.processor_dir(processor_id);
        std::fs::create_dir_all(&dir)?;

        let persisted = PersistedState {
            version: state.version,
            entries: state.entries.clone(),
        };

        let data = serde_json::to_string_pretty(&persisted).map_err(|e| {
            PluginError::ProcessingFailed(format!("Failed to serialize state: {}", e))
        })?;

        // Atomic write: write to temp file then rename.
        let tmp_path = dir.join("state.json.tmp");
        let final_path = dir.join("state.json");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    /// Remove persisted state from disk for a processor (cleanup on removal).
    fn remove_from_disk(&self, processor_id: &str) -> ProcessResult<()> {
        let dir = self.processor_dir(processor_id);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    /// Get the directory path for a processor's state.
    fn processor_dir(&self, processor_id: &str) -> PathBuf {
        self.base_dir.join(sanitize_id(processor_id))
    }

    /// Get the state file path for a processor.
    fn state_file_path(&self, processor_id: &str) -> PathBuf {
        self.processor_dir(processor_id).join("state.json")
    }

    /// Get the current state for a processor.
    pub fn get_state(&self, processor_id: &str) -> ProcessResult<StateMap> {
        let state = self.get_or_load(processor_id);
        let locked = state.lock();
        Ok(locked.to_state_map())
    }

    /// Set the state for a processor, replacing any existing state.
    pub fn set_state(
        &self,
        processor_id: &str,
        entries: HashMap<String, String>,
    ) -> ProcessResult<()> {
        let state = self.get_or_load(processor_id);
        let mut locked = state.lock();
        locked.version += 1;
        locked.entries = entries;
        self.save_to_disk(processor_id, &locked)?;
        Ok(())
    }

    /// Compare-and-swap: replace state only if the version matches.
    ///
    /// Returns `true` if replacement succeeded, `false` if version mismatch.
    pub fn replace(
        &self,
        processor_id: &str,
        old_version: i64,
        new_entries: HashMap<String, String>,
    ) -> ProcessResult<bool> {
        let state = self.get_or_load(processor_id);
        let mut locked = state.lock();

        if locked.version != old_version {
            return Ok(false);
        }

        locked.version += 1;
        locked.entries = new_entries;
        self.save_to_disk(processor_id, &locked)?;
        Ok(true)
    }

    /// Clear all state for a processor (resets version to -1).
    pub fn clear(&self, processor_id: &str) -> ProcessResult<()> {
        let state = self.get_or_load(processor_id);
        let mut locked = state.lock();
        locked.version = -1;
        locked.entries.clear();
        self.remove_from_disk(processor_id)?;
        Ok(())
    }

    /// Remove all state for a processor (called when processor is removed from flow).
    pub fn remove_processor(&self, processor_id: &str) -> ProcessResult<()> {
        self.states.remove(processor_id);
        self.remove_from_disk(processor_id)
    }
}

/// Shared reference to a LocalStateProvider.
pub type SharedLocalStateProvider = Arc<LocalStateProvider>;

/// Per-processor state manager that wraps a shared LocalStateProvider
/// with a fixed processor ID. Implements the plugin-api StateManager trait.
pub struct CoreStateManager {
    provider: SharedLocalStateProvider,
    processor_id: String,
}

impl CoreStateManager {
    /// Create a new state manager for a specific processor instance.
    pub fn new(provider: SharedLocalStateProvider, processor_id: String) -> Self {
        Self {
            provider,
            processor_id,
        }
    }
}

impl StateManager for CoreStateManager {
    fn get_state(&self, scope: StateScope) -> ProcessResult<StateMap> {
        match scope {
            StateScope::Local => self.provider.get_state(&self.processor_id),
            StateScope::Cluster => Err(PluginError::ProcessingFailed(
                "Cluster state scope is not yet implemented".to_string(),
            )),
        }
    }

    fn set_state(&self, state: HashMap<String, String>, scope: StateScope) -> ProcessResult<()> {
        match scope {
            StateScope::Local => self.provider.set_state(&self.processor_id, state),
            StateScope::Cluster => Err(PluginError::ProcessingFailed(
                "Cluster state scope is not yet implemented".to_string(),
            )),
        }
    }

    fn replace(
        &self,
        old_state: &StateMap,
        new_state: HashMap<String, String>,
        scope: StateScope,
    ) -> ProcessResult<bool> {
        match scope {
            StateScope::Local => {
                self.provider
                    .replace(&self.processor_id, old_state.version(), new_state)
            }
            StateScope::Cluster => Err(PluginError::ProcessingFailed(
                "Cluster state scope is not yet implemented".to_string(),
            )),
        }
    }

    fn clear(&self, scope: StateScope) -> ProcessResult<()> {
        match scope {
            StateScope::Local => self.provider.clear(&self.processor_id),
            StateScope::Cluster => Err(PluginError::ProcessingFailed(
                "Cluster state scope is not yet implemented".to_string(),
            )),
        }
    }
}

/// Sanitize a processor ID for use as a directory name.
/// Replaces potentially dangerous characters with underscores.
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
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
        assert_eq!(state.version(), -1);
        assert!(state.entries().is_empty());
    }

    #[test]
    fn test_set_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let mut entries = HashMap::new();
        entries.insert("key1".to_string(), "value1".to_string());
        entries.insert("key2".to_string(), "value2".to_string());
        provider.set_state("proc-1", entries).unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.get("key1"), Some("value1"));
        assert_eq!(state.get("key2"), Some("value2"));
    }

    #[test]
    fn test_version_increments() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries1 = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries1).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 0);

        let entries2 = HashMap::from([("k".to_string(), "v2".to_string())]);
        provider.set_state("proc-1", entries2).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 1);

        let entries3 = HashMap::from([("k".to_string(), "v3".to_string())]);
        provider.set_state("proc-1", entries3).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 2);
    }

    #[test]
    fn test_replace_success() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries).unwrap();

        let old_state = provider.get_state("proc-1").unwrap();
        assert_eq!(old_state.version(), 0);

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
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries).unwrap();

        // Try to replace with wrong version.
        let new_entries = HashMap::from([("k".to_string(), "v2".to_string())]);
        let replaced = provider.replace("proc-1", 99, new_entries).unwrap();
        assert!(!replaced);

        // State should be unchanged.
        let state = provider.get_state("proc-1").unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.get("k"), Some("v1"));
    }

    #[test]
    fn test_clear() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v".to_string())]);
        provider.set_state("proc-1", entries).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 0);

        provider.clear("proc-1").unwrap();

        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
        assert_eq!(state.version(), -1);
    }

    #[test]
    fn test_remove_processor() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v".to_string())]);
        provider.set_state("proc-1", entries).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 0);

        provider.remove_processor("proc-1").unwrap();

        // State should be empty after removal.
        let state = provider.get_state("proc-1").unwrap();
        assert!(state.is_empty());
    }

    #[test]
    fn test_persistence_across_provider_instances() {
        let dir = tempfile::tempdir().unwrap();

        // Write state with first provider instance.
        {
            let provider = LocalStateProvider::new(dir.path()).unwrap();
            let entries = HashMap::from([
                ("last_file".to_string(), "/tmp/data.csv".to_string()),
                ("offset".to_string(), "42".to_string()),
            ]);
            provider.set_state("proc-1", entries).unwrap();
        }

        // Read state with a new provider instance (simulates restart).
        {
            let provider = LocalStateProvider::new(dir.path()).unwrap();
            let state = provider.get_state("proc-1").unwrap();
            assert_eq!(state.version(), 0);
            assert_eq!(state.get("last_file"), Some("/tmp/data.csv"));
            assert_eq!(state.get("offset"), Some("42"));
        }
    }

    #[test]
    fn test_multiple_processors() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries1 = HashMap::from([("k".to_string(), "proc1-value".to_string())]);
        let entries2 = HashMap::from([("k".to_string(), "proc2-value".to_string())]);

        provider.set_state("proc-1", entries1).unwrap();
        provider.set_state("proc-2", entries2).unwrap();

        assert_eq!(
            provider.get_state("proc-1").unwrap().get("k"),
            Some("proc1-value")
        );
        assert_eq!(
            provider.get_state("proc-2").unwrap().get("k"),
            Some("proc2-value")
        );

        // Clear one should not affect the other.
        provider.clear("proc-1").unwrap();
        assert!(provider.get_state("proc-1").unwrap().is_empty());
        assert_eq!(
            provider.get_state("proc-2").unwrap().get("k"),
            Some("proc2-value")
        );
    }

    #[test]
    fn test_core_state_manager_local() {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalStateProvider::new(dir.path()).unwrap());

        let manager = CoreStateManager::new(provider, "proc-1".to_string());

        // Initially empty.
        let state = manager.get_state(StateScope::Local).unwrap();
        assert!(state.is_empty());

        // Set state.
        let entries = HashMap::from([("k".to_string(), "v".to_string())]);
        manager.set_state(entries, StateScope::Local).unwrap();

        let state = manager.get_state(StateScope::Local).unwrap();
        assert_eq!(state.version(), 0);
        assert_eq!(state.get("k"), Some("v"));

        // Replace with CAS.
        let new_entries = HashMap::from([("k".to_string(), "v2".to_string())]);
        let ok = manager
            .replace(&state, new_entries, StateScope::Local)
            .unwrap();
        assert!(ok);

        let state = manager.get_state(StateScope::Local).unwrap();
        assert_eq!(state.version(), 1);
        assert_eq!(state.get("k"), Some("v2"));

        // Clear.
        manager.clear(StateScope::Local).unwrap();
        let state = manager.get_state(StateScope::Local).unwrap();
        assert!(state.is_empty());
    }

    #[test]
    fn test_core_state_manager_cluster_not_implemented() {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalStateProvider::new(dir.path()).unwrap());

        let manager = CoreStateManager::new(provider, "proc-1".to_string());

        let result = manager.get_state(StateScope::Cluster);
        assert!(result.is_err());

        let result = manager.set_state(HashMap::new(), StateScope::Cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_sanitize_id() {
        assert_eq!(sanitize_id("proc-1"), "proc-1");
        assert_eq!(sanitize_id("node_0"), "node_0");
        assert_eq!(sanitize_id("../../etc/passwd"), "______etc_passwd");
        assert_eq!(sanitize_id("proc/sub"), "proc_sub");
    }

    #[test]
    fn test_set_after_clear() {
        let dir = tempfile::tempdir().unwrap();
        let provider = LocalStateProvider::new(dir.path()).unwrap();

        let entries = HashMap::from([("k".to_string(), "v1".to_string())]);
        provider.set_state("proc-1", entries).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 0);

        provider.clear("proc-1").unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), -1);

        // After clear, version increments from -1 to 0 again.
        let entries2 = HashMap::from([("k".to_string(), "v2".to_string())]);
        provider.set_state("proc-1", entries2).unwrap();
        assert_eq!(provider.get_state("proc-1").unwrap().version(), 0);
        assert_eq!(provider.get_state("proc-1").unwrap().get("k"), Some("v2"));
    }
}
