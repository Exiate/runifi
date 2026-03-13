use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

/// A provenance event recording data lineage.
#[derive(Debug, Clone)]
pub struct ProvenanceEvent {
    /// Unique event ID (monotonic).
    pub event_id: u64,
    /// The FlowFile this event pertains to.
    pub flowfile_id: u64,
    /// Type of event.
    pub event_type: ProvenanceEventType,
    /// Name of the processor that produced this event.
    pub processor_name: String,
    /// Processor type (e.g. "GenerateFlowFile", "PutFile").
    pub processor_type: String,
    /// Timestamp in nanoseconds since epoch.
    pub timestamp_nanos: u64,
    /// FlowFile attributes snapshot at event time.
    pub attributes: Vec<(String, String)>,
    /// Content size at event time.
    pub content_size: u64,
    /// Lineage start FlowFile ID (ancestor chain root).
    pub lineage_start_id: u64,
    /// Relationship the FlowFile was transferred on (if applicable).
    pub relationship: Option<String>,
    /// For CLONE events, the ID of the source FlowFile.
    pub source_flowfile_id: Option<u64>,
    /// Human-readable details about the event.
    pub details: String,
}

/// Types of provenance events, mirroring NiFi's provenance event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProvenanceEventType {
    /// FlowFile was created (e.g. by GenerateFlowFile).
    Create,
    /// FlowFile was received from an upstream connection.
    Receive,
    /// FlowFile was sent to a downstream connection.
    Send,
    /// FlowFile was cloned.
    Clone,
    /// FlowFile content was modified (write_content).
    ContentModified,
    /// FlowFile attributes were modified.
    AttributesModified,
    /// FlowFile was routed to a relationship.
    Route,
    /// FlowFile was removed/dropped.
    Drop,
}

impl ProvenanceEventType {
    /// Return a string representation of the event type.
    pub fn as_str(&self) -> &'static str {
        match self {
            ProvenanceEventType::Create => "CREATE",
            ProvenanceEventType::Receive => "RECEIVE",
            ProvenanceEventType::Send => "SEND",
            ProvenanceEventType::Clone => "CLONE",
            ProvenanceEventType::ContentModified => "CONTENT_MODIFIED",
            ProvenanceEventType::AttributesModified => "ATTRIBUTES_MODIFIED",
            ProvenanceEventType::Route => "ROUTE",
            ProvenanceEventType::Drop => "DROP",
        }
    }

    /// Parse from a string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CREATE" => Some(ProvenanceEventType::Create),
            "RECEIVE" => Some(ProvenanceEventType::Receive),
            "SEND" => Some(ProvenanceEventType::Send),
            "CLONE" => Some(ProvenanceEventType::Clone),
            "CONTENT_MODIFIED" => Some(ProvenanceEventType::ContentModified),
            "ATTRIBUTES_MODIFIED" => Some(ProvenanceEventType::AttributesModified),
            "ROUTE" => Some(ProvenanceEventType::Route),
            "DROP" => Some(ProvenanceEventType::Drop),
            _ => None,
        }
    }
}

/// Configuration for provenance repository retention.
#[derive(Debug, Clone)]
pub struct ProvenanceConfig {
    /// Maximum number of events to retain. Oldest are evicted when exceeded.
    pub max_events: usize,
    /// Maximum age of events in nanoseconds. Events older than this are pruned.
    /// 0 means no age limit.
    pub max_age_nanos: u64,
}

impl Default for ProvenanceConfig {
    fn default() -> Self {
        Self {
            // Default: keep up to 1 million events.
            max_events: 1_000_000,
            // Default: keep events for 24 hours.
            max_age_nanos: 24 * 60 * 60 * 1_000_000_000,
        }
    }
}

/// Search criteria for querying provenance events.
#[derive(Debug, Default)]
pub struct ProvenanceQuery {
    /// Filter by FlowFile ID.
    pub flowfile_id: Option<u64>,
    /// Filter by processor name.
    pub processor_name: Option<String>,
    /// Filter by event type.
    pub event_type: Option<ProvenanceEventType>,
    /// Filter events after this timestamp (inclusive).
    pub start_time_nanos: Option<u64>,
    /// Filter events before this timestamp (inclusive).
    pub end_time_nanos: Option<u64>,
    /// Maximum number of results to return.
    pub max_results: usize,
    /// Offset for pagination.
    pub offset: usize,
}

/// Result of a provenance search.
#[derive(Debug)]
pub struct ProvenanceSearchResult {
    /// Matching events.
    pub events: Vec<ProvenanceEvent>,
    /// Total matching count (before pagination).
    pub total_count: usize,
}

/// Trait for provenance event storage.
pub trait ProvenanceRepository: Send + Sync {
    /// Record a single provenance event.
    fn record(&self, event: ProvenanceEvent);

    /// Record a batch of provenance events atomically.
    fn record_batch(&self, events: Vec<ProvenanceEvent>);

    /// Search for provenance events matching the given criteria.
    fn search(&self, query: &ProvenanceQuery) -> ProvenanceSearchResult;

    /// Get the full lineage (all events) for a given FlowFile ID.
    /// This includes events for the FlowFile and all ancestors/descendants
    /// sharing the same lineage_start_id.
    fn get_lineage(&self, flowfile_id: u64) -> Vec<ProvenanceEvent>;

    /// Get a single event by ID.
    fn get_event(&self, event_id: u64) -> Option<ProvenanceEvent>;

    /// Get the current number of stored events.
    fn event_count(&self) -> usize;

    /// Prune events older than the configured retention.
    fn prune(&self);
}

/// Thread-safe, bounded, in-memory provenance repository.
///
/// Uses DashMap for concurrent access and a RwLock-protected Vec for
/// ordered event storage. Designed for low overhead on the hot path:
/// events are appended lock-free via DashMap, with periodic pruning.
pub struct InMemoryProvenanceRepository {
    /// All events indexed by event_id for O(1) lookup.
    events_by_id: DashMap<u64, ProvenanceEvent>,
    /// Events indexed by FlowFile ID for lineage queries.
    events_by_flowfile: DashMap<u64, Vec<u64>>,
    /// Events indexed by lineage_start_id for full lineage graph.
    events_by_lineage: DashMap<u64, Vec<u64>>,
    /// Ordered list of event IDs for time-based queries and eviction.
    ordered_ids: RwLock<Vec<u64>>,
    /// Monotonic event ID generator.
    next_event_id: AtomicU64,
    /// Retention configuration.
    config: ProvenanceConfig,
}

impl InMemoryProvenanceRepository {
    /// Create a new in-memory provenance repository with default configuration.
    pub fn new() -> Self {
        Self::with_config(ProvenanceConfig::default())
    }

    /// Create a new in-memory provenance repository with the given configuration.
    pub fn with_config(config: ProvenanceConfig) -> Self {
        Self {
            events_by_id: DashMap::new(),
            events_by_flowfile: DashMap::new(),
            events_by_lineage: DashMap::new(),
            ordered_ids: RwLock::new(Vec::new()),
            next_event_id: AtomicU64::new(1),
            config,
        }
    }

    /// Allocate the next event ID.
    fn allocate_event_id(&self) -> u64 {
        self.next_event_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Store a single event, updating all indices.
    fn store_event(&self, mut event: ProvenanceEvent) {
        if event.event_id == 0 {
            event.event_id = self.allocate_event_id();
        }
        let eid = event.event_id;
        let ffid = event.flowfile_id;
        let lineage_id = event.lineage_start_id;

        self.events_by_id.insert(eid, event);

        self.events_by_flowfile.entry(ffid).or_default().push(eid);

        self.events_by_lineage
            .entry(lineage_id)
            .or_default()
            .push(eid);

        self.ordered_ids.write().push(eid);
    }

    /// Evict oldest events when over capacity.
    fn evict_if_needed(&self) {
        let max = self.config.max_events;
        if max == 0 {
            return;
        }

        let mut ordered = self.ordered_ids.write();
        while ordered.len() > max {
            if let Some(old_id) = ordered.first().copied() {
                ordered.remove(0);
                if let Some((_, event)) = self.events_by_id.remove(&old_id) {
                    // Remove from flowfile index.
                    if let Some(mut ids) = self.events_by_flowfile.get_mut(&event.flowfile_id) {
                        ids.retain(|&id| id != old_id);
                    }
                    // Remove from lineage index.
                    if let Some(mut ids) = self.events_by_lineage.get_mut(&event.lineage_start_id) {
                        ids.retain(|&id| id != old_id);
                    }
                }
            } else {
                break;
            }
        }
    }
}

impl Default for InMemoryProvenanceRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ProvenanceRepository for InMemoryProvenanceRepository {
    fn record(&self, event: ProvenanceEvent) {
        self.store_event(event);
        self.evict_if_needed();
    }

    fn record_batch(&self, events: Vec<ProvenanceEvent>) {
        for event in events {
            self.store_event(event);
        }
        self.evict_if_needed();
    }

    fn search(&self, query: &ProvenanceQuery) -> ProvenanceSearchResult {
        let ordered = self.ordered_ids.read();

        // Collect matching events.
        let mut matching: Vec<ProvenanceEvent> = Vec::new();

        for &eid in ordered.iter().rev() {
            if let Some(event) = self.events_by_id.get(&eid) {
                let event = event.value();

                // Apply filters.
                if let Some(ffid) = query.flowfile_id
                    && event.flowfile_id != ffid
                {
                    continue;
                }
                if let Some(ref proc_name) = query.processor_name
                    && event.processor_name != *proc_name
                {
                    continue;
                }
                if let Some(et) = query.event_type
                    && event.event_type != et
                {
                    continue;
                }
                if let Some(start) = query.start_time_nanos
                    && event.timestamp_nanos < start
                {
                    continue;
                }
                if let Some(end) = query.end_time_nanos
                    && event.timestamp_nanos > end
                {
                    continue;
                }

                matching.push(event.clone());
            }
        }

        let total_count = matching.len();

        // Apply pagination.
        let max_results = if query.max_results == 0 {
            100
        } else {
            query.max_results
        };
        let events: Vec<ProvenanceEvent> = matching
            .into_iter()
            .skip(query.offset)
            .take(max_results)
            .collect();

        ProvenanceSearchResult {
            events,
            total_count,
        }
    }

    fn get_lineage(&self, flowfile_id: u64) -> Vec<ProvenanceEvent> {
        // First, find the lineage_start_id for this FlowFile.
        let lineage_start_id = if let Some(ids) = self.events_by_flowfile.get(&flowfile_id) {
            ids.iter()
                .filter_map(|&eid| self.events_by_id.get(&eid))
                .map(|e| e.lineage_start_id)
                .next()
                .unwrap_or(flowfile_id)
        } else {
            flowfile_id
        };

        // Get all events in this lineage.
        let mut events: Vec<ProvenanceEvent> =
            if let Some(ids) = self.events_by_lineage.get(&lineage_start_id) {
                ids.iter()
                    .filter_map(|&eid| self.events_by_id.get(&eid).map(|e| e.value().clone()))
                    .collect()
            } else {
                Vec::new()
            };

        // Sort by timestamp for chronological order.
        events.sort_by_key(|e| e.timestamp_nanos);
        events
    }

    fn get_event(&self, event_id: u64) -> Option<ProvenanceEvent> {
        self.events_by_id.get(&event_id).map(|e| e.value().clone())
    }

    fn event_count(&self) -> usize {
        self.events_by_id.len()
    }

    fn prune(&self) {
        if self.config.max_age_nanos == 0 {
            return;
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let cutoff = now_nanos.saturating_sub(self.config.max_age_nanos);

        let mut ordered = self.ordered_ids.write();
        let mut to_remove = Vec::new();

        // Events are ordered chronologically, so we can stop early.
        for &eid in ordered.iter() {
            if let Some(event) = self.events_by_id.get(&eid) {
                if event.timestamp_nanos < cutoff {
                    to_remove.push(eid);
                } else {
                    break;
                }
            }
        }

        for old_id in &to_remove {
            if let Some((_, event)) = self.events_by_id.remove(old_id) {
                if let Some(mut ids) = self.events_by_flowfile.get_mut(&event.flowfile_id) {
                    ids.retain(|&id| id != *old_id);
                }
                if let Some(mut ids) = self.events_by_lineage.get_mut(&event.lineage_start_id) {
                    ids.retain(|&id| id != *old_id);
                }
            }
        }
        ordered.retain(|id| !to_remove.contains(id));
    }
}

/// A shared, thread-safe reference to a provenance repository.
pub type SharedProvenanceRepository = Arc<dyn ProvenanceRepository>;

/// No-op provenance repository for when provenance is disabled.
pub struct NullProvenanceRepository;

impl ProvenanceRepository for NullProvenanceRepository {
    fn record(&self, _event: ProvenanceEvent) {}
    fn record_batch(&self, _events: Vec<ProvenanceEvent>) {}

    fn search(&self, _query: &ProvenanceQuery) -> ProvenanceSearchResult {
        ProvenanceSearchResult {
            events: Vec::new(),
            total_count: 0,
        }
    }

    fn get_lineage(&self, _flowfile_id: u64) -> Vec<ProvenanceEvent> {
        Vec::new()
    }

    fn get_event(&self, _event_id: u64) -> Option<ProvenanceEvent> {
        None
    }

    fn event_count(&self) -> usize {
        0
    }

    fn prune(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_nanos() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn make_event(
        flowfile_id: u64,
        event_type: ProvenanceEventType,
        processor: &str,
        lineage_id: u64,
    ) -> ProvenanceEvent {
        ProvenanceEvent {
            event_id: 0,
            flowfile_id,
            event_type,
            processor_name: processor.to_string(),
            processor_type: "TestProcessor".to_string(),
            timestamp_nanos: now_nanos(),
            attributes: Vec::new(),
            content_size: 0,
            lineage_start_id: lineage_id,
            relationship: None,
            source_flowfile_id: None,
            details: String::new(),
        }
    }

    #[test]
    fn record_and_retrieve_event() {
        let repo = InMemoryProvenanceRepository::new();
        let event = make_event(1, ProvenanceEventType::Create, "gen", 1);
        repo.record(event);

        assert_eq!(repo.event_count(), 1);

        let result = repo.search(&ProvenanceQuery {
            flowfile_id: Some(1),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 1);
        assert_eq!(result.events[0].flowfile_id, 1);
        assert_eq!(result.events[0].event_type, ProvenanceEventType::Create);
    }

    #[test]
    fn record_batch() {
        let repo = InMemoryProvenanceRepository::new();
        let events = vec![
            make_event(1, ProvenanceEventType::Create, "gen", 1),
            make_event(1, ProvenanceEventType::Send, "gen", 1),
            make_event(1, ProvenanceEventType::Receive, "log", 1),
        ];
        repo.record_batch(events);
        assert_eq!(repo.event_count(), 3);
    }

    #[test]
    fn search_by_processor() {
        let repo = InMemoryProvenanceRepository::new();
        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Create, "get-file", 2));
        repo.record(make_event(3, ProvenanceEventType::Send, "gen", 3));

        let result = repo.search(&ProvenanceQuery {
            processor_name: Some("gen".to_string()),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 2);
    }

    #[test]
    fn search_by_event_type() {
        let repo = InMemoryProvenanceRepository::new();
        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Send, "gen", 2));
        repo.record(make_event(3, ProvenanceEventType::Drop, "log", 3));

        let result = repo.search(&ProvenanceQuery {
            event_type: Some(ProvenanceEventType::Create),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 1);
        assert_eq!(result.events[0].flowfile_id, 1);
    }

    #[test]
    fn search_pagination() {
        let repo = InMemoryProvenanceRepository::new();
        for i in 1..=10 {
            repo.record(make_event(i, ProvenanceEventType::Create, "gen", i));
        }

        let result = repo.search(&ProvenanceQuery {
            max_results: 3,
            offset: 0,
            ..Default::default()
        });
        assert_eq!(result.total_count, 10);
        assert_eq!(result.events.len(), 3);

        let result2 = repo.search(&ProvenanceQuery {
            max_results: 3,
            offset: 3,
            ..Default::default()
        });
        assert_eq!(result2.total_count, 10);
        assert_eq!(result2.events.len(), 3);
    }

    #[test]
    fn get_lineage() {
        let repo = InMemoryProvenanceRepository::new();
        // FlowFile 1 is created, then cloned to FlowFile 2.
        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(1, ProvenanceEventType::Send, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Clone, "route", 1));
        repo.record(make_event(1, ProvenanceEventType::Receive, "log", 1));

        let lineage = repo.get_lineage(1);
        assert_eq!(lineage.len(), 4);

        // Querying lineage by the clone should return the same lineage.
        let lineage2 = repo.get_lineage(2);
        assert_eq!(lineage2.len(), 4);
    }

    #[test]
    fn get_event_by_id() {
        let repo = InMemoryProvenanceRepository::new();
        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));

        let event = repo.get_event(1).unwrap();
        assert_eq!(event.flowfile_id, 1);

        assert!(repo.get_event(999).is_none());
    }

    #[test]
    fn eviction_on_max_events() {
        let config = ProvenanceConfig {
            max_events: 5,
            max_age_nanos: 0,
        };
        let repo = InMemoryProvenanceRepository::with_config(config);

        for i in 1..=10 {
            repo.record(make_event(i, ProvenanceEventType::Create, "gen", i));
        }

        // Should have at most 5 events.
        assert!(repo.event_count() <= 5);
    }

    #[test]
    fn event_type_parse_roundtrip() {
        let types = [
            ProvenanceEventType::Create,
            ProvenanceEventType::Receive,
            ProvenanceEventType::Send,
            ProvenanceEventType::Clone,
            ProvenanceEventType::ContentModified,
            ProvenanceEventType::AttributesModified,
            ProvenanceEventType::Route,
            ProvenanceEventType::Drop,
        ];

        for et in &types {
            let s = et.as_str();
            let parsed = ProvenanceEventType::parse(s).unwrap();
            assert_eq!(*et, parsed);
        }
    }

    #[test]
    fn null_repo_is_noop() {
        let repo = NullProvenanceRepository;
        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        assert_eq!(repo.event_count(), 0);
        assert!(repo.get_event(1).is_none());
        assert!(repo.get_lineage(1).is_empty());

        let result = repo.search(&ProvenanceQuery::default());
        assert_eq!(result.total_count, 0);
    }

    #[test]
    fn search_by_time_range() {
        let repo = InMemoryProvenanceRepository::new();
        let t1 = now_nanos();

        let mut e1 = make_event(1, ProvenanceEventType::Create, "gen", 1);
        e1.timestamp_nanos = t1;
        repo.record(e1);

        let t2 = t1 + 1_000_000_000; // +1 second
        let mut e2 = make_event(2, ProvenanceEventType::Create, "gen", 2);
        e2.timestamp_nanos = t2;
        repo.record(e2);

        let t3 = t2 + 1_000_000_000; // +2 seconds
        let mut e3 = make_event(3, ProvenanceEventType::Create, "gen", 3);
        e3.timestamp_nanos = t3;
        repo.record(e3);

        // Search for events between t1 and t2.
        let result = repo.search(&ProvenanceQuery {
            start_time_nanos: Some(t1),
            end_time_nanos: Some(t2),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 2);
    }
}
