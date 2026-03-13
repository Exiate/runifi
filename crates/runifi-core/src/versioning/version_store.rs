//! Git-backed flow version storage.
//!
//! Each saved version creates a git commit in a dedicated repository.
//! The flow state is serialized to JSON with sorted keys for meaningful diffs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use git2::{Oid, Repository, Signature, Time};
use serde::Serialize;
use thiserror::Error;

use crate::engine::persistence::PersistedFlowState;

/// File name for the flow state inside the version repository.
const FLOW_FILE: &str = "flow.json";

/// Error type for version store operations.
#[derive(Debug, Error)]
pub enum VersionError {
    #[error("git error: {0}")]
    Git(#[from] git2::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("version not found: {0}")]
    NotFound(String),

    #[error("no versions saved yet")]
    NoVersions,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Metadata for a saved flow version.
#[derive(Debug, Clone, Serialize)]
pub struct FlowVersion {
    /// Short (8-char) commit SHA used as the version identifier.
    pub id: String,
    /// Full commit SHA.
    pub full_id: String,
    /// Version comment (the commit message).
    pub comment: String,
    /// Unix timestamp (seconds) when the version was saved.
    pub timestamp: i64,
    /// Number of processors in this version.
    pub processor_count: usize,
    /// Number of connections in this version.
    pub connection_count: usize,
}

/// Git-backed version store for flow configurations.
///
/// Stores each version as a commit in a bare-like git repository.
/// The repository contains a single file (`flow.json`) with the serialized
/// flow state. Keys are sorted for deterministic output and meaningful diffs.
pub struct FlowVersionStore {
    repo_path: PathBuf,
}

impl FlowVersionStore {
    /// Create or open a version store at the given path.
    ///
    /// If the directory does not exist, initializes a new git repository.
    pub fn open(repo_path: impl Into<PathBuf>) -> Result<Self, VersionError> {
        let repo_path = repo_path.into();

        if !repo_path.exists() {
            std::fs::create_dir_all(&repo_path)?;
            Repository::init(&repo_path)?;
            tracing::info!(
                path = %repo_path.display(),
                "Initialized flow version repository"
            );
        } else if !repo_path.join(".git").exists() && !repo_path.join("HEAD").exists() {
            // Directory exists but is not a git repo — initialize it.
            Repository::init(&repo_path)?;
            tracing::info!(
                path = %repo_path.display(),
                "Initialized flow version repository in existing directory"
            );
        }

        Ok(Self { repo_path })
    }

    /// Return the repository path.
    pub fn repo_path(&self) -> &Path {
        &self.repo_path
    }

    /// Save the current flow state as a new version.
    ///
    /// Returns the version metadata including the short commit SHA.
    pub fn save_version(
        &self,
        state: &PersistedFlowState,
        comment: &str,
    ) -> Result<FlowVersion, VersionError> {
        let repo = Repository::open(&self.repo_path)?;

        // Serialize with sorted keys for deterministic output.
        let json = serialize_sorted(state)?;

        // Create a blob from the JSON content.
        let blob_oid = repo.blob(json.as_bytes())?;

        // Build a tree with the single flow.json file.
        let mut tree_builder = repo.treebuilder(None)?;
        tree_builder.insert(FLOW_FILE, blob_oid, 0o100644)?;
        let tree_oid = tree_builder.write()?;
        let tree = repo.find_tree(tree_oid)?;

        // Create the commit signature.
        let sig = Signature::new("RuniFi", "runifi@localhost", &Time::new(now_unix(), 0))?;

        // Find the parent commit (if any).
        let parent_commit = repo.head().ok().and_then(|head| head.peel_to_commit().ok());

        let parents: Vec<&git2::Commit<'_>> =
            parent_commit.as_ref().map(|c| vec![c]).unwrap_or_default();

        let oid = repo.commit(Some("HEAD"), &sig, &sig, comment, &tree, &parents)?;

        let short_id = short_sha(oid);
        let version = FlowVersion {
            id: short_id,
            full_id: oid.to_string(),
            comment: comment.to_string(),
            timestamp: now_unix(),
            processor_count: state.processors.len(),
            connection_count: state.connections.len(),
        };

        tracing::info!(
            version_id = %version.id,
            comment = %comment,
            processors = state.processors.len(),
            connections = state.connections.len(),
            "Flow version saved"
        );

        Ok(version)
    }

    /// List all saved versions, most recent first.
    pub fn list_versions(&self) -> Result<Vec<FlowVersion>, VersionError> {
        let repo = Repository::open(&self.repo_path)?;

        let head = match repo.head() {
            Ok(head) => head,
            Err(_) => return Ok(vec![]), // No commits yet.
        };

        let head_commit = head.peel_to_commit()?;

        let mut versions = Vec::new();
        let mut revwalk = repo.revwalk()?;
        revwalk.push(head_commit.id())?;
        revwalk.set_sorting(git2::Sort::TIME)?;

        for oid_result in revwalk {
            let oid = oid_result?;
            let commit = repo.find_commit(oid)?;
            let state = self.load_state_from_commit(&repo, &commit)?;

            versions.push(FlowVersion {
                id: short_sha(oid),
                full_id: oid.to_string(),
                comment: commit.message().unwrap_or("").to_string(),
                timestamp: commit.time().seconds(),
                processor_count: state.processors.len(),
                connection_count: state.connections.len(),
            });
        }

        Ok(versions)
    }

    /// Get a specific version by its short or full SHA.
    pub fn get_version(&self, version_id: &str) -> Result<FlowVersion, VersionError> {
        let repo = Repository::open(&self.repo_path)?;
        let commit = self.find_commit(&repo, version_id)?;
        let state = self.load_state_from_commit(&repo, &commit)?;

        Ok(FlowVersion {
            id: short_sha(commit.id()),
            full_id: commit.id().to_string(),
            comment: commit.message().unwrap_or("").to_string(),
            timestamp: commit.time().seconds(),
            processor_count: state.processors.len(),
            connection_count: state.connections.len(),
        })
    }

    /// Load the flow state from a specific version.
    pub fn load_version(&self, version_id: &str) -> Result<PersistedFlowState, VersionError> {
        let repo = Repository::open(&self.repo_path)?;
        let commit = self.find_commit(&repo, version_id)?;
        self.load_state_from_commit(&repo, &commit)
    }

    /// Load the flow state from the most recent version.
    pub fn load_latest(&self) -> Result<PersistedFlowState, VersionError> {
        let repo = Repository::open(&self.repo_path)?;
        let head = repo.head().map_err(|_| VersionError::NoVersions)?;
        let commit = head.peel_to_commit()?;
        self.load_state_from_commit(&repo, &commit)
    }

    /// Find a commit by short or full SHA prefix.
    fn find_commit<'a>(
        &self,
        repo: &'a Repository,
        version_id: &str,
    ) -> Result<git2::Commit<'a>, VersionError> {
        // Try exact OID first.
        if let Ok(oid) = Oid::from_str(version_id)
            && let Ok(commit) = repo.find_commit(oid)
        {
            return Ok(commit);
        }

        // Try prefix match (for short SHAs).
        let obj = repo
            .revparse_single(version_id)
            .map_err(|_| VersionError::NotFound(version_id.to_string()))?;

        obj.into_commit()
            .map_err(|_| VersionError::NotFound(version_id.to_string()))
    }

    /// Load the PersistedFlowState from a commit's tree.
    fn load_state_from_commit(
        &self,
        repo: &Repository,
        commit: &git2::Commit<'_>,
    ) -> Result<PersistedFlowState, VersionError> {
        let tree = commit.tree()?;
        let entry = tree
            .get_name(FLOW_FILE)
            .ok_or_else(|| VersionError::NotFound("flow.json not found in commit".to_string()))?;

        let blob = repo.find_blob(entry.id())?;
        let content = std::str::from_utf8(blob.content())
            .map_err(|e| VersionError::Serialization(serde_json::Error::custom(e.to_string())))?;

        let state: PersistedFlowState = serde_json::from_str(content)?;
        Ok(state)
    }
}

/// Serialize a `PersistedFlowState` with sorted keys for deterministic output.
///
/// This ensures that two identical flow states produce byte-identical JSON,
/// which makes git diffs meaningful and avoids spurious changes.
fn serialize_sorted(state: &PersistedFlowState) -> Result<String, serde_json::Error> {
    // Convert to serde_json::Value, which we can then sort.
    let value = serde_json::to_value(state)?;
    let sorted = sort_json_value(&value);
    serde_json::to_string_pretty(&sorted)
}

/// Recursively sort all object keys in a JSON value.
fn sort_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), sort_json_value(v)))
                .collect();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sort_json_value).collect())
        }
        other => other.clone(),
    }
}

/// Get the current Unix timestamp in seconds.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Convert a git OID to an 8-character short SHA.
fn short_sha(oid: Oid) -> String {
    oid.to_string()[..8].to_string()
}

// ── Custom Error trait for serde_json ────────────────────────────────────

/// Extension trait to create custom serde_json errors from strings.
trait JsonErrorExt {
    fn custom(msg: String) -> Self;
}

impl JsonErrorExt for serde_json::Error {
    fn custom(msg: String) -> Self {
        serde_json::from_str::<()>(&msg).unwrap_err()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::engine::persistence::{
        PersistedConnection, PersistedFlowState, PersistedProcessor, PersistedScheduling,
        PersistedService,
    };

    use super::*;

    fn make_test_state() -> PersistedFlowState {
        PersistedFlowState {
            version: 1,
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
                },
            ],
            connections: vec![PersistedConnection {
                source: "gen".to_string(),
                relationship: "success".to_string(),
                destination: "log".to_string(),
                back_pressure: None,
                expiration: None,
                priority: None,
                priority_attribute: None,
            }],
            positions: HashMap::new(),
            services: vec![],
            labels: vec![],
            process_groups: vec![],
        }
    }

    #[test]
    fn test_open_creates_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_path = tmp.path().join("versions");

        let store = FlowVersionStore::open(&repo_path).unwrap();
        assert!(repo_path.join(".git").exists() || repo_path.join("HEAD").exists());
        assert_eq!(store.repo_path(), repo_path);
    }

    #[test]
    fn test_open_existing_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let repo_path = tmp.path().join("versions");

        // Open twice — should succeed both times.
        let _store1 = FlowVersionStore::open(&repo_path).unwrap();
        let _store2 = FlowVersionStore::open(&repo_path).unwrap();
    }

    #[test]
    fn test_save_and_list_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let state = make_test_state();

        let v1 = store.save_version(&state, "Initial version").unwrap();
        assert_eq!(v1.comment, "Initial version");
        assert_eq!(v1.processor_count, 2);
        assert_eq!(v1.connection_count, 1);
        assert_eq!(v1.id.len(), 8);

        let v2 = store.save_version(&state, "Second save").unwrap();
        assert_ne!(v1.id, v2.id); // Different commits even for same state.

        let versions = store.list_versions().unwrap();
        assert_eq!(versions.len(), 2);
        // Most recent first.
        assert_eq!(versions[0].id, v2.id);
        assert_eq!(versions[1].id, v1.id);
    }

    #[test]
    fn test_get_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let state = make_test_state();
        let saved = store.save_version(&state, "Test version").unwrap();

        // Get by short ID.
        let retrieved = store.get_version(&saved.id).unwrap();
        assert_eq!(retrieved.id, saved.id);
        assert_eq!(retrieved.comment, "Test version");

        // Get by full ID.
        let retrieved_full = store.get_version(&saved.full_id).unwrap();
        assert_eq!(retrieved_full.id, saved.id);
    }

    #[test]
    fn test_load_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let state = make_test_state();
        let saved = store.save_version(&state, "Loadable version").unwrap();

        let loaded = store.load_version(&saved.id).unwrap();
        assert_eq!(loaded.flow_name, "test-flow");
        assert_eq!(loaded.processors.len(), 2);
        assert_eq!(loaded.processors[0].name, "gen");
        assert_eq!(loaded.connections.len(), 1);
    }

    #[test]
    fn test_load_latest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let mut state1 = make_test_state();
        state1.flow_name = "version-1".to_string();
        store.save_version(&state1, "First").unwrap();

        let mut state2 = make_test_state();
        state2.flow_name = "version-2".to_string();
        store.save_version(&state2, "Second").unwrap();

        let latest = store.load_latest().unwrap();
        assert_eq!(latest.flow_name, "version-2");
    }

    #[test]
    fn test_version_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let result = store.get_version("deadbeef");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VersionError::NotFound(_)));
    }

    #[test]
    fn test_list_empty_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let versions = store.list_versions().unwrap();
        assert!(versions.is_empty());
    }

    #[test]
    fn test_deterministic_serialization() {
        let state = make_test_state();

        // Serialize twice — should produce identical output.
        let json1 = serialize_sorted(&state).unwrap();
        let json2 = serialize_sorted(&state).unwrap();
        assert_eq!(json1, json2);
    }

    #[test]
    fn test_sorted_keys_in_output() {
        let mut state = make_test_state();
        state.processors[0]
            .properties
            .insert("Z Property".to_string(), "z".to_string());
        state.processors[0]
            .properties
            .insert("A Property".to_string(), "a".to_string());

        let json = serialize_sorted(&state).unwrap();

        // "A Property" should appear before "Z Property" in the output.
        let a_pos = json.find("A Property").unwrap();
        let z_pos = json.find("Z Property").unwrap();
        assert!(a_pos < z_pos);
    }

    #[test]
    fn test_services_preserved_in_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FlowVersionStore::open(tmp.path().join("versions")).unwrap();

        let mut state = make_test_state();
        state.services.push(PersistedService {
            name: "cache".to_string(),
            type_name: "DistributedMapCacheServer".to_string(),
            properties: HashMap::from([("Port".to_string(), "4557".to_string())]),
        });

        let saved = store.save_version(&state, "With services").unwrap();
        let loaded = store.load_version(&saved.id).unwrap();

        assert_eq!(loaded.services.len(), 1);
        assert_eq!(loaded.services[0].name, "cache");
    }
}
