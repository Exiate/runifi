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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_flowfile(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    #[test]
    fn creation_has_correct_defaults() {
        let ff = make_flowfile(1);
        assert_eq!(ff.id, 1);
        assert!(ff.attributes.is_empty());
        assert!(ff.content_claim.is_none());
        assert_eq!(ff.size, 0);
        assert_eq!(ff.lineage_start_id, 1);
        assert_eq!(ff.penalized_until_nanos, 0);
    }

    #[test]
    fn set_and_get_attribute() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("filename"), Arc::from("test.txt"));
        assert_eq!(ff.get_attribute("filename").unwrap().as_ref(), "test.txt");
    }

    #[test]
    fn get_missing_attribute_returns_none() {
        let ff = make_flowfile(1);
        assert!(ff.get_attribute("nonexistent").is_none());
    }

    #[test]
    fn set_attribute_overwrites_existing() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("key"), Arc::from("value1"));
        ff.set_attribute(Arc::from("key"), Arc::from("value2"));
        assert_eq!(ff.get_attribute("key").unwrap().as_ref(), "value2");
        assert_eq!(ff.attributes.len(), 1);
    }

    #[test]
    fn remove_attribute_returns_value() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("key"), Arc::from("value"));
        let removed = ff.remove_attribute("key");
        assert_eq!(removed.unwrap().as_ref(), "value");
        assert!(ff.get_attribute("key").is_none());
    }

    #[test]
    fn remove_missing_attribute_returns_none() {
        let mut ff = make_flowfile(1);
        assert!(ff.remove_attribute("nonexistent").is_none());
    }

    #[test]
    fn multiple_attributes() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("a"), Arc::from("1"));
        ff.set_attribute(Arc::from("b"), Arc::from("2"));
        ff.set_attribute(Arc::from("c"), Arc::from("3"));
        assert_eq!(ff.attributes.len(), 3);
        assert_eq!(ff.get_attribute("a").unwrap().as_ref(), "1");
        assert_eq!(ff.get_attribute("b").unwrap().as_ref(), "2");
        assert_eq!(ff.get_attribute("c").unwrap().as_ref(), "3");
    }

    #[test]
    fn clone_preserves_all_fields() {
        let mut ff = make_flowfile(1);
        ff.size = 1024;
        ff.created_at_nanos = 42;
        ff.set_attribute(Arc::from("key"), Arc::from("value"));
        ff.content_claim = Some(ContentClaim {
            resource_id: 10,
            offset: 0,
            length: 1024,
        });

        let cloned = ff.clone();
        assert_eq!(cloned.id, ff.id);
        assert_eq!(cloned.size, ff.size);
        assert_eq!(cloned.created_at_nanos, ff.created_at_nanos);
        assert_eq!(cloned.lineage_start_id, ff.lineage_start_id);
        assert_eq!(cloned.get_attribute("key").unwrap().as_ref(), "value");
        assert_eq!(cloned.content_claim, ff.content_claim);
    }

    #[test]
    fn clone_attribute_independence() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("key"), Arc::from("original"));

        let mut cloned = ff.clone();
        cloned.set_attribute(Arc::from("key"), Arc::from("modified"));

        // Original should be unchanged.
        assert_eq!(ff.get_attribute("key").unwrap().as_ref(), "original");
        assert_eq!(cloned.get_attribute("key").unwrap().as_ref(), "modified");
    }

    #[test]
    fn is_penalized_returns_true_when_penalized() {
        let mut ff = make_flowfile(1);
        ff.penalized_until_nanos = 1000;
        assert!(ff.is_penalized(500));
        assert!(!ff.is_penalized(1000));
        assert!(!ff.is_penalized(1500));
    }

    #[test]
    fn content_claim_equality() {
        let claim1 = ContentClaim {
            resource_id: 1,
            offset: 0,
            length: 100,
        };
        let claim2 = ContentClaim {
            resource_id: 1,
            offset: 0,
            length: 100,
        };
        let claim3 = ContentClaim {
            resource_id: 2,
            offset: 0,
            length: 100,
        };
        assert_eq!(claim1, claim2);
        assert_ne!(claim1, claim3);
    }

    #[test]
    fn remove_attribute_with_swap_remove_preserves_others() {
        let mut ff = make_flowfile(1);
        ff.set_attribute(Arc::from("a"), Arc::from("1"));
        ff.set_attribute(Arc::from("b"), Arc::from("2"));
        ff.set_attribute(Arc::from("c"), Arc::from("3"));

        ff.remove_attribute("a");
        assert_eq!(ff.attributes.len(), 2);
        // swap_remove moves the last element to the removed position
        assert!(ff.get_attribute("b").is_some());
        assert!(ff.get_attribute("c").is_some());
    }

    #[test]
    fn arc_str_sharing() {
        // Verify Arc<str> can be shared across FlowFiles for common keys.
        let key: Arc<str> = Arc::from("filename");
        let mut ff1 = make_flowfile(1);
        let mut ff2 = make_flowfile(2);
        ff1.set_attribute(key.clone(), Arc::from("file1.txt"));
        ff2.set_attribute(key.clone(), Arc::from("file2.txt"));

        // Both FlowFiles share the same key Arc.
        assert!(Arc::ptr_eq(&ff1.attributes[0].0, &ff2.attributes[0].0));
    }
}
