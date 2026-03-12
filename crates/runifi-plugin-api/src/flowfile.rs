use std::sync::Arc;

/// A reference to content stored in the content repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentClaim {
    /// Unique identifier for the content resource.
    pub resource_id: u64,
    /// Byte offset within the resource (for slicing).
    pub offset: u64,
    /// Length of this claim in bytes.
    pub length: u64,
}

/// A FlowFile is the atomic unit of data moving through RuniFi.
///
/// Designed for high throughput:
/// - `id` is `u64` (slab-allocated) instead of UUID for cheaper allocation at 5000/sec
/// - `attributes` is `Vec<(Arc<str>, Arc<str>)>` — cache-friendly for <16 attrs, Arc shares repeated keys
/// - Content is referenced via `ContentClaim`, not embedded — enables zero-copy sharing
#[derive(Debug, Clone)]
pub struct FlowFile {
    /// Monotonic slab-allocated ID.
    pub id: u64,
    /// Key-value attributes. Vec is cache-friendly for typical attribute counts (<16).
    /// Arc<str> allows sharing common keys (e.g. "filename", "mime.type") across FlowFiles.
    pub attributes: Vec<(Arc<str>, Arc<str>)>,
    /// Reference to content in the content repository. `None` for attribute-only FlowFiles.
    pub content_claim: Option<ContentClaim>,
    /// Cached content size in bytes.
    pub size: u64,
    /// Creation timestamp in nanoseconds since epoch.
    pub created_at_nanos: u64,
    /// ID of the original FlowFile that started this lineage.
    pub lineage_start_id: u64,
    /// If > 0, this FlowFile is penalized until this timestamp (nanos since epoch).
    pub penalized_until_nanos: u64,
}

impl FlowFile {
    /// Get an attribute value by key.
    pub fn get_attribute(&self, key: &str) -> Option<&Arc<str>> {
        self.attributes
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v)
    }

    /// Set an attribute, replacing if it already exists.
    pub fn set_attribute(&mut self, key: Arc<str>, value: Arc<str>) {
        if let Some(entry) = self.attributes.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = value;
        } else {
            self.attributes.push((key, value));
        }
    }

    /// Remove an attribute by key, returning the value if it existed.
    pub fn remove_attribute(&mut self, key: &str) -> Option<Arc<str>> {
        if let Some(pos) = self.attributes.iter().position(|(k, _)| k.as_ref() == key) {
            Some(self.attributes.swap_remove(pos).1)
        } else {
            None
        }
    }

    /// Check if this FlowFile is currently penalized.
    pub fn is_penalized(&self, now_nanos: u64) -> bool {
        self.penalized_until_nanos > now_nanos
    }
}
