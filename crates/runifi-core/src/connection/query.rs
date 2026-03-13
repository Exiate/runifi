use std::sync::Arc;

use super::flow_connection::{FlowConnection, FlowFileSnapshot};

/// Trait for querying metadata and state of a connection between processors.
///
/// Abstracts over the concrete `FlowConnection` so that API consumers and
/// engine internals do not need a direct dependency on the concrete type.
/// Security components (encrypted repos, audit middleware) can implement
/// this interface and plug in cleanly without exposing internal queue state.
pub trait ConnectionQuery: Send + Sync {
    /// Unique ID of this connection.
    fn id(&self) -> &str;
    /// Name of the source processor.
    fn source_name(&self) -> &str;
    /// Relationship name on the source processor.
    fn relationship(&self) -> &str;
    /// Name of the destination processor.
    fn dest_name(&self) -> &str;
    /// Number of FlowFiles currently queued.
    fn queue_count(&self) -> usize;
    /// Total bytes of queued FlowFile content.
    fn queue_size_bytes(&self) -> u64;
    /// Whether back-pressure thresholds are currently exceeded.
    fn is_back_pressured(&self) -> bool;

    // ── Queue inspection ──────────────────────────────────────────────────────

    /// Return a paginated snapshot of FlowFiles in the queue.
    fn queue_snapshot(&self, offset: usize, limit: usize) -> Vec<FlowFileSnapshot>;
    /// Return the total number of snapshots in the queue.
    fn queue_snapshot_count(&self) -> usize;
    /// Look up a single FlowFile snapshot by ID.
    fn queue_get(&self, flowfile_id: u64) -> Option<FlowFileSnapshot>;
    /// Look up a FlowFile snapshot by ID with its queue position.
    fn queue_get_with_position(&self, flowfile_id: u64) -> Option<(usize, FlowFileSnapshot)>;
    /// Remove a specific FlowFile from the queue by ID. Returns true if found.
    fn remove_flowfile(&self, flowfile_id: u64) -> bool;
    /// Clear all FlowFiles from the queue. Returns the number removed.
    fn clear_queue(&self) -> usize;
}

/// Standard implementation of `ConnectionQuery` that wraps a `FlowConnection`
/// together with the source/destination/relationship metadata.
pub struct FlowConnectionQuery {
    pub source_name: String,
    pub relationship: String,
    pub dest_name: String,
    pub connection: Arc<FlowConnection>,
}

impl FlowConnectionQuery {
    pub fn new(
        source_name: String,
        relationship: String,
        dest_name: String,
        connection: Arc<FlowConnection>,
    ) -> Self {
        Self {
            source_name,
            relationship,
            dest_name,
            connection,
        }
    }
}

impl ConnectionQuery for FlowConnectionQuery {
    fn id(&self) -> &str {
        &self.connection.id
    }

    fn source_name(&self) -> &str {
        &self.source_name
    }

    fn relationship(&self) -> &str {
        &self.relationship
    }

    fn dest_name(&self) -> &str {
        &self.dest_name
    }

    fn queue_count(&self) -> usize {
        self.connection.count()
    }

    fn queue_size_bytes(&self) -> u64 {
        self.connection.bytes()
    }

    fn is_back_pressured(&self) -> bool {
        self.connection.is_back_pressured()
    }

    fn queue_snapshot(&self, offset: usize, limit: usize) -> Vec<FlowFileSnapshot> {
        self.connection.queue_snapshot(offset, limit)
    }

    fn queue_snapshot_count(&self) -> usize {
        self.connection.queue_snapshot_count()
    }

    fn queue_get(&self, flowfile_id: u64) -> Option<FlowFileSnapshot> {
        self.connection.queue_get(flowfile_id)
    }

    fn queue_get_with_position(&self, flowfile_id: u64) -> Option<(usize, FlowFileSnapshot)> {
        self.connection.queue_get_with_position(flowfile_id)
    }

    fn remove_flowfile(&self, flowfile_id: u64) -> bool {
        self.connection.remove_flowfile(flowfile_id)
    }

    fn clear_queue(&self) -> usize {
        self.connection.clear_queue()
    }
}
