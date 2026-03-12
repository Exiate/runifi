use runifi_plugin_api::FlowFile;

use crate::error::Result;

/// Trait for FlowFile persistence (WAL-based durability).
///
/// In-memory implementation is provided for now; WAL-backed impl is a future phase.
pub trait FlowFileRepository: Send + Sync {
    /// Store a FlowFile for durability.
    fn store(&self, flowfile: &FlowFile) -> Result<()>;

    /// Remove a FlowFile from the repository.
    fn remove(&self, id: u64) -> Result<()>;

    /// Load all FlowFiles (used on recovery).
    fn load_all(&self) -> Result<Vec<FlowFile>>;
}

/// In-memory FlowFile repository (no persistence, for development/testing).
pub struct InMemoryFlowFileRepository;

impl FlowFileRepository for InMemoryFlowFileRepository {
    fn store(&self, _flowfile: &FlowFile) -> Result<()> {
        // No-op: in-memory only
        Ok(())
    }

    fn remove(&self, _id: u64) -> Result<()> {
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<FlowFile>> {
        Ok(Vec::new())
    }
}
