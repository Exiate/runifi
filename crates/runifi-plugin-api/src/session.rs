use bytes::Bytes;

use crate::flowfile::FlowFile;
use crate::relationship::Relationship;
use crate::result::ProcessResult;

/// A transactional session for processor FlowFile operations.
///
/// All changes are buffered until `commit()`. On `rollback()` (or drop without commit),
/// all changes are reverted and FlowFiles are returned to their input queues.
///
/// Implemented by the engine — processors receive this as a `&mut dyn ProcessSession`.
pub trait ProcessSession: Send {
    /// Get a single FlowFile from the input queue. Returns `None` if queue is empty.
    fn get(&mut self) -> Option<FlowFile>;

    /// Get up to `max` FlowFiles from the input queue.
    fn get_batch(&mut self, max: usize) -> Vec<FlowFile>;

    /// Read the content of a FlowFile. Returns the bytes, or an error if content is missing.
    fn read_content(&self, flowfile: &FlowFile) -> ProcessResult<Bytes>;

    /// Replace the content of a FlowFile. Returns the FlowFile with updated content claim.
    fn write_content(&mut self, flowfile: FlowFile, data: Bytes) -> ProcessResult<FlowFile>;

    /// Create a new empty FlowFile (no content, no attributes).
    fn create(&mut self) -> FlowFile;

    /// Clone a FlowFile (new ID, shared content claim with ref counting).
    fn clone_flowfile(&mut self, flowfile: &FlowFile) -> FlowFile;

    /// Transfer a FlowFile to a relationship. The FlowFile is consumed.
    fn transfer(&mut self, flowfile: FlowFile, relationship: &Relationship);

    /// Remove a FlowFile (drop it, decrement content ref count).
    fn remove(&mut self, flowfile: FlowFile);

    /// Penalize a FlowFile (delay re-processing by the yield duration).
    fn penalize(&mut self, flowfile: FlowFile) -> FlowFile;

    /// Commit all buffered operations (transfers, removes, content writes).
    fn commit(&mut self);

    /// Rollback all buffered operations. FlowFiles return to input queues.
    fn rollback(&mut self);
}
