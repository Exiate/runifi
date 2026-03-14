use std::collections::HashMap;

use crate::result::ProcessResult;

/// Scope for processor state storage.
///
/// Mirrors NiFi's `Scope` enum: `LOCAL` (per-node, survives restart)
/// and `CLUSTER` (shared across all cluster nodes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateScope {
    /// State stored locally on this node. Survives restart, cleared when the
    /// processor is removed from the flow.
    Local,
    /// State shared across all nodes in a cluster. Not yet implemented.
    Cluster,
}

impl std::fmt::Display for StateScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateScope::Local => write!(f, "local"),
            StateScope::Cluster => write!(f, "cluster"),
        }
    }
}

/// A versioned snapshot of processor state.
///
/// Contains the current key-value state map and a monotonically increasing
/// version number. The version starts at -1 (no state stored) and increments
/// on each `set_state` or successful `replace`.
///
/// Used by `StateManager::replace()` for compare-and-swap (CAS) semantics:
/// the replacement succeeds only if the current version matches.
#[derive(Debug, Clone)]
pub struct StateMap {
    /// The key-value state entries.
    entries: HashMap<String, String>,
    /// Monotonic version (-1 = no state has ever been stored).
    version: i64,
}

impl StateMap {
    /// Create a new empty state map with version -1 (no state).
    pub fn empty() -> Self {
        Self {
            entries: HashMap::new(),
            version: -1,
        }
    }

    /// Create a state map with the given entries and version.
    pub fn new(entries: HashMap<String, String>, version: i64) -> Self {
        Self { entries, version }
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(|v| v.as_str())
    }

    /// Get all entries as a reference to the underlying map.
    pub fn entries(&self) -> &HashMap<String, String> {
        &self.entries
    }

    /// Consume the state map and return the entries.
    pub fn into_entries(self) -> HashMap<String, String> {
        self.entries
    }

    /// The current version of the state (-1 = no state stored).
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Returns `true` if no state has ever been stored (version == -1).
    pub fn is_empty(&self) -> bool {
        self.version == -1
    }
}

/// Manages persistent state for a single processor instance.
///
/// Processors use this to store and retrieve key-value state that survives
/// restarts. Modeled after NiFi's `StateManager` interface.
///
/// # Concurrency
///
/// Implementations must be safe to call from multiple threads (`Send + Sync`).
/// The `replace()` method provides CAS semantics for safe concurrent updates.
pub trait StateManager: Send + Sync {
    /// Retrieve the current state for the given scope.
    ///
    /// Returns `StateMap::empty()` (version = -1) if no state has been stored.
    fn get_state(&self, scope: StateScope) -> ProcessResult<StateMap>;

    /// Store the given state, replacing any existing state for the scope.
    ///
    /// Increments the version number.
    fn set_state(&self, state: HashMap<String, String>, scope: StateScope) -> ProcessResult<()>;

    /// Compare-and-swap: replace the state only if the current version matches
    /// `old_state.version()`.
    ///
    /// Returns `true` if the replacement succeeded, `false` if the version
    /// did not match (another update happened concurrently).
    fn replace(
        &self,
        old_state: &StateMap,
        new_state: HashMap<String, String>,
        scope: StateScope,
    ) -> ProcessResult<bool>;

    /// Clear all state for the given scope. Resets version to -1.
    fn clear(&self, scope: StateScope) -> ProcessResult<()>;
}

/// Declares that a processor is stateful, including which scopes it uses
/// and a human-readable description of what state it stores.
#[derive(Debug, Clone)]
pub struct StatefulSpec {
    /// Which scopes this processor uses for state storage.
    pub scopes: Vec<StateScope>,
    /// Human-readable description of the state (e.g., "Tracks the last file seen").
    pub description: String,
}

impl StatefulSpec {
    /// Create a spec for a processor that uses only local state.
    pub fn local(description: impl Into<String>) -> Self {
        Self {
            scopes: vec![StateScope::Local],
            description: description.into(),
        }
    }

    /// Create a spec for a processor that uses both local and cluster state.
    pub fn local_and_cluster(description: impl Into<String>) -> Self {
        Self {
            scopes: vec![StateScope::Local, StateScope::Cluster],
            description: description.into(),
        }
    }
}
