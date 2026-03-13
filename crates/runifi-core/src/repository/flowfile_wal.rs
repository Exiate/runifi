//! WAL-backed `FlowFileRepository` implementation.
//!
//! Persists FlowFile metadata and queue assignments to a write-ahead log on disk.
//! On crash recovery, replays the WAL (optionally from a checkpoint) to rebuild
//! in-flight state with at-least-once delivery semantics.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use runifi_plugin_api::FlowFile;

use super::flowfile_repo::{FlowFileOp, FlowFileRepository, RecoveryState};
use super::wal_format::{
    self, CheckpointState, TAG_BATCH_END, TAG_DELETE, TAG_UPSERT, WalRecord, encode_batch_end,
    encode_delete, encode_upsert, to_flowfile, write_record, write_wal_header,
};
use crate::error::{Result, RuniFiError};

/// fsync behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// fsync after every batch commit (safest, slower).
    Always,
    /// Never fsync (fastest, data loss window = OS buffer cache).
    Never,
}

/// Configuration for the WAL-backed FlowFile repository.
#[derive(Debug, Clone)]
pub struct WalFlowFileRepoConfig {
    /// Directory for WAL and checkpoint files.
    pub dir: PathBuf,
    /// fsync behaviour.
    pub fsync_mode: FsyncMode,
    /// Checkpoint interval in seconds (used by the engine's background task).
    pub checkpoint_interval_secs: u64,
}

impl Default for WalFlowFileRepoConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data/flowfile-repo"),
            fsync_mode: FsyncMode::Always,
            checkpoint_interval_secs: 120,
        }
    }
}

/// Internal mutable state protected by a mutex.
struct WalState {
    /// In-memory mirror: flowfile_id -> (FlowFile, queue_id).
    entries: CheckpointState,
    /// Active WAL writer.
    writer: BufWriter<File>,
    /// Highest FlowFile ID seen.
    max_id: u64,
}

/// WAL-backed FlowFile repository.
///
/// All operations are serialized through a `parking_lot::Mutex` protecting
/// the WAL file writer and in-memory state mirror.
pub struct WalFlowFileRepository {
    config: WalFlowFileRepoConfig,
    state: Mutex<WalState>,
}

impl WalFlowFileRepository {
    /// Create a new WAL repository. The WAL directory is created if it doesn't exist.
    pub fn new(config: WalFlowFileRepoConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir).map_err(|e| RuniFiError::WalError {
            path: config.dir.display().to_string(),
            reason: format!("failed to create dir: {e}"),
        })?;

        let wal_path = config.dir.join("wal.dat");
        let file = open_or_create_wal(&wal_path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            config,
            state: Mutex::new(WalState {
                entries: HashMap::new(),
                writer,
                max_id: 0,
            }),
        })
    }

    /// Path to the WAL file.
    fn wal_path(&self) -> PathBuf {
        self.config.dir.join("wal.dat")
    }

    /// Path to the checkpoint file.
    fn checkpoint_path(&self) -> PathBuf {
        self.config.dir.join("checkpoint.dat")
    }

    /// Get the configured checkpoint interval.
    pub fn checkpoint_interval_secs(&self) -> u64 {
        self.config.checkpoint_interval_secs
    }
}

impl FlowFileRepository for WalFlowFileRepository {
    fn commit_batch(&self, ops: &[FlowFileOp<'_>]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock();
        let mut op_count: u32 = 0;

        for op in ops {
            match op {
                FlowFileOp::Upsert { flowfile, queue_id } => {
                    let payload = encode_upsert(flowfile, queue_id);
                    write_record(&mut state.writer, TAG_UPSERT, &payload).map_err(|e| {
                        RuniFiError::WalError {
                            path: self.wal_path().display().to_string(),
                            reason: format!("write upsert failed: {e}"),
                        }
                    })?;
                    if flowfile.id > state.max_id {
                        state.max_id = flowfile.id;
                    }
                    state
                        .entries
                        .insert(flowfile.id, ((*flowfile).clone(), queue_id.to_string()));
                }
                FlowFileOp::Delete { id } => {
                    let payload = encode_delete(*id);
                    write_record(&mut state.writer, TAG_DELETE, &payload).map_err(|e| {
                        RuniFiError::WalError {
                            path: self.wal_path().display().to_string(),
                            reason: format!("write delete failed: {e}"),
                        }
                    })?;
                    state.entries.remove(id);
                }
            }
            op_count += 1;
        }

        // Write BATCH_END marker.
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let batch_payload = encode_batch_end(timestamp, op_count);
        write_record(&mut state.writer, TAG_BATCH_END, &batch_payload).map_err(|e| {
            RuniFiError::WalError {
                path: self.wal_path().display().to_string(),
                reason: format!("write batch_end failed: {e}"),
            }
        })?;

        // Flush the BufWriter.
        state.writer.flush().map_err(|e| RuniFiError::WalError {
            path: self.wal_path().display().to_string(),
            reason: format!("flush failed: {e}"),
        })?;

        // Optionally fsync.
        if self.config.fsync_mode == FsyncMode::Always {
            state
                .writer
                .get_ref()
                .sync_all()
                .map_err(|e| RuniFiError::WalError {
                    path: self.wal_path().display().to_string(),
                    reason: format!("fsync failed: {e}"),
                })?;
        }

        Ok(())
    }

    fn recover(&self) -> Result<RecoveryState> {
        let checkpoint_path = self.checkpoint_path();
        let wal_path = self.wal_path();

        // Phase 1: Load checkpoint if present.
        let (mut entries, mut max_id) = match wal_format::read_checkpoint(&checkpoint_path)? {
            Some((state, mid)) => (state, mid),
            None => (HashMap::new(), 0u64),
        };

        // Phase 2: Replay WAL on top.
        if wal_path.exists() {
            let records = match wal_format::read_wal_records(&wal_path) {
                Ok(r) => r,
                Err(RuniFiError::WalCorrupted { .. }) => {
                    // Truncated/corrupted tail — use what we have from checkpoint.
                    tracing::warn!("WAL corrupted during recovery, using checkpoint state only");
                    Vec::new()
                }
                Err(e) => return Err(e),
            };

            for record in records {
                match record {
                    WalRecord::Upsert(sff) => {
                        if sff.id > max_id {
                            max_id = sff.id;
                        }
                        let queue_id = sff.queue_id.clone();
                        let ff = to_flowfile(&sff);
                        entries.insert(ff.id, (ff, queue_id));
                    }
                    WalRecord::Delete(id) => {
                        entries.remove(&id);
                    }
                    WalRecord::BatchEnd(_) => {
                        // No action needed — just a commit marker.
                    }
                }
            }
        }

        // Build RecoveryState grouped by queue_id.
        let mut queued: HashMap<String, Vec<FlowFile>> = HashMap::new();
        for (ff, queue_id) in entries.values() {
            queued.entry(queue_id.clone()).or_default().push(ff.clone());
        }

        // Update in-memory state.
        let mut state = self.state.lock();
        state.entries = entries;
        state.max_id = max_id;

        // Truncate WAL for a fresh start (checkpoint has the full state).
        drop(state);
        self.truncate_wal()?;

        Ok(RecoveryState { queued, max_id })
    }

    fn checkpoint(&self) -> Result<()> {
        let state = self.state.lock();
        wal_format::write_checkpoint(&self.checkpoint_path(), &state.entries, state.max_id)?;
        drop(state);

        // Truncate WAL since checkpoint has all state.
        self.truncate_wal()?;

        Ok(())
    }

    fn shutdown(&self) {
        let mut state = self.state.lock();
        let _ = state.writer.flush();
    }
}

impl WalFlowFileRepository {
    /// Truncate the WAL file and re-open it with a fresh header.
    fn truncate_wal(&self) -> Result<()> {
        let wal_path = self.wal_path();
        let file = create_fresh_wal(&wal_path)?;
        let writer = BufWriter::new(file);
        self.state.lock().writer = writer;
        Ok(())
    }
}

/// Open an existing WAL for appending, or create a fresh one with a header.
fn open_or_create_wal(path: &Path) -> Result<File> {
    if path.exists() {
        // Open for appending — recovery will replay from the beginning.
        let file =
            OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(|e| RuniFiError::WalError {
                    path: path.display().to_string(),
                    reason: format!("failed to open WAL for append: {e}"),
                })?;
        Ok(file)
    } else {
        create_fresh_wal(path)
    }
}

/// Create/truncate the WAL file and write the header.
fn create_fresh_wal(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| RuniFiError::WalError {
            path: path.display().to_string(),
            reason: format!("failed to create WAL: {e}"),
        })?;

    let mut writer = BufWriter::new(file);
    write_wal_header(&mut writer).map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("failed to write WAL header: {e}"),
    })?;
    writer.flush().map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("flush failed: {e}"),
    })?;

    writer.into_inner().map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("into_inner failed: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runifi_plugin_api::ContentClaim;

    use super::*;

    fn test_ff(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: vec![(Arc::from("key"), Arc::from("val"))],
            content_claim: Some(ContentClaim {
                resource_id: id * 10,
                offset: 0,
                length: 100,
            }),
            size: 100,
            created_at_nanos: 1000 + id,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    fn make_repo(dir: &Path) -> WalFlowFileRepository {
        WalFlowFileRepository::new(WalFlowFileRepoConfig {
            dir: dir.to_path_buf(),
            fsync_mode: FsyncMode::Never,
            checkpoint_interval_secs: 60,
        })
        .unwrap()
    }

    #[test]
    fn write_batch_and_recover() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        let ff2 = test_ff(2);
        repo.commit_batch(&[
            FlowFileOp::Upsert {
                flowfile: &ff1,
                queue_id: "conn-0",
            },
            FlowFileOp::Upsert {
                flowfile: &ff2,
                queue_id: "conn-1",
            },
        ])
        .unwrap();

        // Create a fresh repo to simulate restart.
        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();

        assert_eq!(state.max_id, 2);
        assert_eq!(state.queued["conn-0"].len(), 1);
        assert_eq!(state.queued["conn-1"].len(), 1);
        assert_eq!(state.queued["conn-0"][0].id, 1);
        assert_eq!(state.queued["conn-1"][0].id, 2);
    }

    #[test]
    fn multiple_batches_recover_cumulatively() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();

        let ff2 = test_ff(2);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff2,
            queue_id: "q",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 2);
        assert_eq!(state.max_id, 2);
    }

    #[test]
    fn upsert_same_id_latest_wins() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q1",
        }])
        .unwrap();

        // Upsert same ID to a different queue.
        let mut ff1_moved = test_ff(1);
        ff1_moved.size = 999;
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1_moved,
            queue_id: "q2",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();

        // Should be in q2, not q1.
        assert!(!state.queued.contains_key("q1"));
        assert_eq!(state.queued["q2"].len(), 1);
        assert_eq!(state.queued["q2"][0].size, 999);
    }

    #[test]
    fn delete_removes_entry() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        let ff2 = test_ff(2);
        repo.commit_batch(&[
            FlowFileOp::Upsert {
                flowfile: &ff1,
                queue_id: "q",
            },
            FlowFileOp::Upsert {
                flowfile: &ff2,
                queue_id: "q",
            },
        ])
        .unwrap();

        repo.commit_batch(&[FlowFileOp::Delete { id: 1 }]).unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 1);
        assert_eq!(state.queued["q"][0].id, 2);
    }

    #[test]
    fn checkpoint_then_more_writes() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();

        repo.checkpoint().unwrap();

        let ff2 = test_ff(2);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff2,
            queue_id: "q",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 2);
        assert_eq!(state.max_id, 2);
    }

    #[test]
    fn truncated_wal_recovers_up_to_last_good() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();

        // Write a checkpoint so ff1 is safe.
        repo.checkpoint().unwrap();

        let ff2 = test_ff(2);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff2,
            queue_id: "q",
        }])
        .unwrap();
        repo.shutdown();

        // Truncate the WAL to simulate a crash mid-write.
        let wal_path = dir.path().join("wal.dat");
        let data = fs::read(&wal_path).unwrap();
        // Keep only the header (8 bytes) + a few bytes of the record.
        fs::write(&wal_path, &data[..12]).unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();
        // ff1 from checkpoint is recovered, ff2 is lost due to truncation.
        assert_eq!(state.queued["q"].len(), 1);
        assert_eq!(state.queued["q"][0].id, 1);
    }

    #[test]
    fn empty_dir_recovery_returns_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());
        let state = repo.recover().unwrap();
        assert!(state.queued.is_empty());
        assert_eq!(state.max_id, 0);
    }

    #[test]
    fn max_id_tracking() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());

        let ff10 = test_ff(10);
        let ff5 = test_ff(5);
        repo.commit_batch(&[
            FlowFileOp::Upsert {
                flowfile: &ff10,
                queue_id: "q",
            },
            FlowFileOp::Upsert {
                flowfile: &ff5,
                queue_id: "q",
            },
        ])
        .unwrap();

        let repo2 = make_repo(dir.path());
        let state = repo2.recover().unwrap();
        assert_eq!(state.max_id, 10);
    }

    #[test]
    fn empty_batch_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = make_repo(dir.path());
        repo.commit_batch(&[]).unwrap();

        let state = repo.recover().unwrap();
        assert!(state.queued.is_empty());
    }
}
