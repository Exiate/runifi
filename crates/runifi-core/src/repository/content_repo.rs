use bytes::Bytes;
use runifi_plugin_api::ContentClaim;

use crate::error::Result;

/// Trait for content storage backends.
///
/// Content is reference-counted: multiple FlowFiles can share the same content
/// via `ContentClaim`. Content is freed when ref count reaches zero.
pub trait ContentRepository: Send + Sync {
    /// Store new content and return a claim to it. Initial ref count = 1.
    fn create(&self, data: Bytes) -> Result<ContentClaim>;

    /// Read content by claim. Returns the slice defined by the claim's offset + length.
    fn read(&self, claim: &ContentClaim) -> Result<Bytes>;

    /// Increment the reference count for a content resource (e.g. when cloning a FlowFile).
    fn increment_ref(&self, resource_id: u64) -> Result<()>;

    /// Decrement the reference count. If it reaches zero, the content is freed.
    fn decrement_ref(&self, resource_id: u64) -> Result<()>;
}
