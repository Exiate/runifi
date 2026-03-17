use std::collections::HashMap;

use runifi_plugin_api::FlowFile;

use crate::error::Result;

/// An operation in a WAL batch.
pub enum FlowFileOp<'a> {
    /// Persist a FlowFile with its queue assignment.
    Upsert {
        flowfile: &'a FlowFile,
        queue_id: &'a str,
    },
    /// Remove a FlowFile from persistence.
    Delete { id: u64 },
}

/// State recovered from the WAL on startup.
pub struct RecoveryState {
    /// FlowFiles grouped by their connection queue ID.
    pub queued: HashMap<String, Vec<FlowFile>>,
    /// Highest FlowFile ID seen — used to re-seed the ID generator.
    pub max_id: u64,
}

/// Storage statistics for a FlowFile repository.
pub struct FlowFileRepoStats {
    pub entry_count: usize,
    pub storage_bytes: u64,
    pub storage_type: &'static str,
}

/// Trait for FlowFile persistence (WAL-based durability).
///
/// Implementations must be `Send + Sync` for use across async tasks.
pub trait FlowFileRepository: Send + Sync {
    /// Atomically persist a batch of operations.
    fn commit_batch(&self, ops: &[FlowFileOp<'_>]) -> Result<()>;

    /// Recover all persisted FlowFiles, grouped by queue ID.
    fn recover(&self) -> Result<RecoveryState>;

    /// Write a compacted checkpoint of current state and truncate the WAL.
    fn checkpoint(&self) -> Result<()>;

    /// Write a batch of operations WITHOUT fsyncing (for batch optimization).
    /// Data is flushed to OS buffers but not durably persisted until `flush_deferred()`.
    fn commit_batch_deferred(&self, ops: &[FlowFileOp<'_>]) -> Result<()> {
        // Default: delegate to commit_batch (maintains backward compat).
        self.commit_batch(ops)
    }

    /// Fsync all deferred writes. Called at the end of a batch run.
    fn flush_deferred(&self) -> Result<()> {
        Ok(())
    }

    /// Graceful shutdown hook (e.g. flush buffers).
    fn shutdown(&self) {}

    /// Return storage statistics for the FlowFile repository.
    fn stats(&self) -> FlowFileRepoStats {
        FlowFileRepoStats {
            entry_count: 0,
            storage_bytes: 0,
            storage_type: "unknown",
        }
    }
}

/// In-memory FlowFile repository (no persistence, for development/testing).
pub struct InMemoryFlowFileRepository;

impl FlowFileRepository for InMemoryFlowFileRepository {
    fn commit_batch(&self, _ops: &[FlowFileOp<'_>]) -> Result<()> {
        Ok(())
    }

    fn recover(&self) -> Result<RecoveryState> {
        Ok(RecoveryState {
            queued: HashMap::new(),
            max_id: 0,
        })
    }

    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> FlowFileRepoStats {
        FlowFileRepoStats {
            entry_count: 0,
            storage_bytes: 0,
            storage_type: "memory",
        }
    }
}
