use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::FlowFile;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;

use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;
use crate::repository::provenance_repo::{
    ProvenanceEvent, ProvenanceEventType, SharedProvenanceRepository,
};

/// Pending transfer: a FlowFile waiting to be routed to a relationship.
struct PendingTransfer {
    flowfile: FlowFile,
    relationship_name: &'static str,
}

/// The engine's implementation of `ProcessSession`.
///
/// Mediates between processors and the content repository.
/// All operations are buffered until `commit()` — on `rollback()` or drop,
/// changes are reverted and FlowFiles return to input queues.
pub struct CoreProcessSession {
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    input_connections: Vec<Arc<FlowConnection>>,

    // Buffered state
    pending_transfers: Vec<PendingTransfer>,
    pending_removes: Vec<FlowFile>,
    acquired_flowfiles: Vec<FlowFile>,
    created_content_claims: Vec<u64>,
    committed: bool,
    yield_duration_ms: u64,
    /// IDs of FlowFiles removed during `commit()`, for WAL DELETE ops.
    committed_remove_ids: Vec<u64>,

    // Provenance tracking
    provenance_repo: SharedProvenanceRepository,
    /// Processor name context for provenance events.
    processor_name: String,
    /// Processor type context for provenance events.
    processor_type: String,
    /// Buffered provenance events — flushed on commit, discarded on rollback.
    pending_provenance: Vec<ProvenanceEvent>,
}

impl CoreProcessSession {
    pub fn new(
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        input_connections: Vec<Arc<FlowConnection>>,
        yield_duration_ms: u64,
    ) -> Self {
        Self {
            content_repo,
            id_gen,
            input_connections,
            pending_transfers: Vec::new(),
            pending_removes: Vec::new(),
            acquired_flowfiles: Vec::new(),
            created_content_claims: Vec::new(),
            committed: false,
            yield_duration_ms,
            committed_remove_ids: Vec::new(),
            provenance_repo: Arc::new(crate::repository::provenance_repo::NullProvenanceRepository),
            processor_name: String::new(),
            processor_type: String::new(),
            pending_provenance: Vec::new(),
        }
    }

    /// Set the provenance repository for this session.
    pub fn set_provenance(
        &mut self,
        repo: SharedProvenanceRepository,
        processor_name: String,
        processor_type: String,
    ) {
        self.provenance_repo = repo;
        self.processor_name = processor_name;
        self.processor_type = processor_type;
    }

    /// Get pending transfers for routing by the engine after commit.
    pub fn take_transfers(&mut self) -> Vec<(FlowFile, &'static str)> {
        self.pending_transfers
            .drain(..)
            .map(|t| (t.flowfile, t.relationship_name))
            .collect()
    }

    /// Check if the session was committed.
    pub fn is_committed(&self) -> bool {
        self.committed
    }

    /// Number of FlowFiles acquired from input connections this session.
    pub fn acquired_count(&self) -> usize {
        self.acquired_flowfiles.len()
    }

    /// Total bytes of FlowFiles acquired from input connections this session.
    pub fn acquired_bytes(&self) -> u64 {
        self.acquired_flowfiles.iter().map(|ff| ff.size).sum()
    }

    /// Take the IDs of FlowFiles removed during `commit()` for WAL DELETE ops.
    pub fn take_committed_remove_ids(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.committed_remove_ids)
    }

    /// Create a provenance event for the given FlowFile.
    fn make_provenance_event(
        &self,
        ff: &FlowFile,
        event_type: ProvenanceEventType,
    ) -> ProvenanceEvent {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        ProvenanceEvent {
            event_id: 0, // Will be assigned by the repository.
            flowfile_id: ff.id,
            event_type,
            processor_name: self.processor_name.clone(),
            processor_type: self.processor_type.clone(),
            timestamp_nanos: now_nanos,
            attributes: ff
                .attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            content_size: ff.size,
            lineage_start_id: ff.lineage_start_id,
            relationship: None,
            source_flowfile_id: None,
            details: String::new(),
        }
    }
}

impl ProcessSession for CoreProcessSession {
    fn get(&mut self) -> Option<FlowFile> {
        for conn in &self.input_connections {
            if let Some(ff) = conn.try_recv() {
                // Record RECEIVE provenance event.
                let mut event = self.make_provenance_event(&ff, ProvenanceEventType::Receive);
                event.details = format!("Received from connection '{}'", conn.id);
                self.pending_provenance.push(event);

                self.acquired_flowfiles.push(ff.clone());
                return Some(ff);
            }
        }
        None
    }

    fn get_batch(&mut self, max: usize) -> Vec<FlowFile> {
        let mut batch = Vec::with_capacity(max);
        let mut remaining = max;

        for conn in &self.input_connections {
            if remaining == 0 {
                break;
            }
            let received = conn.try_recv_batch(remaining);
            remaining -= received.len();
            for ff in &received {
                // Record RECEIVE provenance event for each FlowFile.
                let mut event = self.make_provenance_event(ff, ProvenanceEventType::Receive);
                event.details = format!("Received from connection '{}'", conn.id);
                self.pending_provenance.push(event);

                self.acquired_flowfiles.push(ff.clone());
            }
            batch.extend(received);
        }
        batch
    }

    fn read_content(&self, flowfile: &FlowFile) -> ProcessResult<Bytes> {
        match &flowfile.content_claim {
            Some(claim) => self
                .content_repo
                .read(claim)
                .map_err(|_| PluginError::ContentNotFound(claim.resource_id)),
            None => Ok(Bytes::new()),
        }
    }

    fn write_content(&mut self, mut flowfile: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
        let claim = self
            .content_repo
            .create(data.clone())
            .map_err(|e| PluginError::ProcessingFailed(e.to_string()))?;

        self.created_content_claims.push(claim.resource_id);
        flowfile.size = data.len() as u64;
        flowfile.content_claim = Some(claim);

        // Record CONTENT_MODIFIED provenance event.
        let mut event = self.make_provenance_event(&flowfile, ProvenanceEventType::ContentModified);
        event.details = format!("Content written ({} bytes)", data.len());
        self.pending_provenance.push(event);

        Ok(flowfile)
    }

    fn create(&mut self) -> FlowFile {
        let id = self.id_gen.next_id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: now,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };

        // Record CREATE provenance event.
        let event = self.make_provenance_event(&ff, ProvenanceEventType::Create);
        self.pending_provenance.push(event);

        ff
    }

    fn clone_flowfile(&mut self, flowfile: &FlowFile) -> FlowFile {
        let new_id = self.id_gen.next_id();

        // Increment content ref count if there's a content claim.
        if let Some(claim) = &flowfile.content_claim {
            let _ = self.content_repo.increment_ref(claim.resource_id);
        }

        let cloned = FlowFile {
            id: new_id,
            attributes: flowfile.attributes.clone(),
            content_claim: flowfile.content_claim.clone(),
            size: flowfile.size,
            created_at_nanos: flowfile.created_at_nanos,
            lineage_start_id: flowfile.lineage_start_id,
            penalized_until_nanos: 0,
        };

        // Record CLONE provenance event.
        let mut event = self.make_provenance_event(&cloned, ProvenanceEventType::Clone);
        event.source_flowfile_id = Some(flowfile.id);
        event.details = format!("Cloned from FlowFile {}", flowfile.id);
        self.pending_provenance.push(event);

        cloned
    }

    fn transfer(&mut self, flowfile: FlowFile, relationship: &Relationship) {
        // Record ROUTE provenance event.
        let mut event = self.make_provenance_event(&flowfile, ProvenanceEventType::Route);
        event.relationship = Some(relationship.name.to_string());
        event.details = format!("Transferred to relationship '{}'", relationship.name);
        self.pending_provenance.push(event);

        self.pending_transfers.push(PendingTransfer {
            flowfile,
            relationship_name: relationship.name,
        });
    }

    fn remove(&mut self, flowfile: FlowFile) {
        // Record DROP provenance event.
        let event = self.make_provenance_event(&flowfile, ProvenanceEventType::Drop);
        self.pending_provenance.push(event);

        self.pending_removes.push(flowfile);
    }

    fn penalize(&mut self, mut flowfile: FlowFile) -> FlowFile {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let penalty_nanos = self.yield_duration_ms * 1_000_000;
        flowfile.penalized_until_nanos = now + penalty_nanos;
        flowfile
    }

    fn commit(&mut self) {
        // Capture remove IDs before decrementing refs (for WAL DELETE ops).
        self.committed_remove_ids = self.pending_removes.iter().map(|ff| ff.id).collect();

        // Decrement ref counts for removed FlowFiles.
        for ff in self.pending_removes.drain(..) {
            if let Some(claim) = &ff.content_claim {
                let _ = self.content_repo.decrement_ref(claim.resource_id);
            }
        }

        // Flush provenance events on commit.
        if !self.pending_provenance.is_empty() {
            let events = std::mem::take(&mut self.pending_provenance);
            self.provenance_repo.record_batch(events);
        }

        // Note: acquired_flowfiles is intentionally NOT cleared here.
        // ProcessorNode reads acquired_count()/acquired_bytes() after commit
        // to track input metrics. The Vec is freed when the session is dropped.
        self.committed = true;
    }

    fn rollback(&mut self) {
        // Return acquired FlowFiles to input connections.
        for ff in self.acquired_flowfiles.drain(..) {
            for conn in &self.input_connections {
                if conn.try_send(ff.clone()).is_ok() {
                    break;
                }
            }
        }

        // Clean up any content we created during this session.
        for resource_id in self.created_content_claims.drain(..) {
            let _ = self.content_repo.decrement_ref(resource_id);
        }

        self.pending_transfers.clear();
        self.pending_removes.clear();
        // Discard provenance events on rollback.
        self.pending_provenance.clear();
        self.committed = false;
    }
}

impl Drop for CoreProcessSession {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::back_pressure::BackPressureConfig;
    use crate::repository::content_memory::InMemoryContentRepository;
    use crate::repository::provenance_repo::{InMemoryProvenanceRepository, ProvenanceRepository};

    fn make_session(input_connections: Vec<Arc<FlowConnection>>) -> CoreProcessSession {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        CoreProcessSession::new(content_repo, id_gen, input_connections, 1000)
    }

    fn make_session_with_provenance(
        input_connections: Vec<Arc<FlowConnection>>,
    ) -> (CoreProcessSession, Arc<InMemoryProvenanceRepository>) {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let provenance_repo = Arc::new(InMemoryProvenanceRepository::new());
        let mut session = CoreProcessSession::new(content_repo, id_gen, input_connections, 1000);
        session.set_provenance(
            provenance_repo.clone(),
            "test-processor".to_string(),
            "TestProcessor".to_string(),
        );
        (session, provenance_repo)
    }

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
    fn create_returns_unique_ids() {
        let mut session = make_session(vec![]);
        let ff1 = session.create();
        let ff2 = session.create();
        assert_ne!(ff1.id, ff2.id);
        assert!(ff1.id > 0);
        assert!(ff2.id > 0);
    }

    #[test]
    fn write_and_read_content() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        let data = Bytes::from_static(b"hello world");
        let ff = session.write_content(ff, data.clone()).unwrap();
        assert_eq!(ff.size, 11);
        assert!(ff.content_claim.is_some());

        let read_back = session.read_content(&ff).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn read_content_empty_when_no_claim() {
        let session = make_session(vec![]);
        let ff = make_flowfile(1);
        let content = session.read_content(&ff).unwrap();
        assert!(content.is_empty());
    }

    #[test]
    fn transfer_buffers_until_commit() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        session.transfer(ff, &runifi_plugin_api::REL_SUCCESS);

        assert!(!session.is_committed());
        session.commit();
        assert!(session.is_committed());
    }

    #[test]
    fn take_transfers_returns_buffered() {
        let mut session = make_session(vec![]);
        let ff1 = session.create();
        let ff2 = session.create();
        session.transfer(ff1, &runifi_plugin_api::REL_SUCCESS);
        session.transfer(ff2, &runifi_plugin_api::REL_FAILURE);
        session.commit();

        let transfers = session.take_transfers();
        assert_eq!(transfers.len(), 2);
        assert_eq!(transfers[0].1, "success");
        assert_eq!(transfers[1].1, "failure");
    }

    #[test]
    fn get_from_input_connection() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile(42)).unwrap();

        let mut session = make_session(vec![conn]);
        let ff = session.get().unwrap();
        assert_eq!(ff.id, 42);
    }

    #[test]
    fn get_returns_none_when_empty() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        let mut session = make_session(vec![conn]);
        assert!(session.get().is_none());
    }

    #[test]
    fn get_batch_from_input() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        for i in 0..5 {
            conn.try_send(make_flowfile(i)).unwrap();
        }

        let mut session = make_session(vec![conn]);
        let batch = session.get_batch(3);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn clone_flowfile_shares_content() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        let data = Bytes::from_static(b"shared content");
        let ff = session.write_content(ff, data.clone()).unwrap();

        let cloned = session.clone_flowfile(&ff);
        assert_ne!(cloned.id, ff.id);
        assert_eq!(cloned.content_claim, ff.content_claim);
        assert_eq!(cloned.size, ff.size);

        // Both should read the same content.
        let original_content = session.read_content(&ff).unwrap();
        let cloned_content = session.read_content(&cloned).unwrap();
        assert_eq!(original_content, cloned_content);
    }

    #[test]
    fn remove_decrements_ref_on_commit() {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let mut session = CoreProcessSession::new(content_repo.clone(), id_gen, vec![], 1000);

        let ff = session.create();
        let data = Bytes::from_static(b"to be removed");
        let ff = session.write_content(ff, data).unwrap();
        let claim = ff.content_claim.clone().unwrap();

        session.remove(ff);
        // Before commit, content should still be accessible.
        assert!(content_repo.read(&claim).is_ok());

        session.commit();
        // After commit, content should be freed (ref count 0).
        assert!(content_repo.read(&claim).is_err());
    }

    #[test]
    fn rollback_returns_flowfiles_to_input() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile(42)).unwrap();
        assert_eq!(conn.count(), 1);

        let mut session = make_session(vec![conn.clone()]);
        let ff = session.get().unwrap();
        assert_eq!(ff.id, 42);
        assert_eq!(conn.count(), 0);

        session.rollback();
        // FlowFile should be back in the connection.
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn rollback_cleans_up_created_content() {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let mut session = CoreProcessSession::new(content_repo.clone(), id_gen, vec![], 1000);

        let ff = session.create();
        let data = Bytes::from_static(b"will be rolled back");
        let ff = session.write_content(ff, data).unwrap();
        let claim = ff.content_claim.clone().unwrap();

        // Before rollback, content is accessible.
        assert!(content_repo.read(&claim).is_ok());

        session.rollback();
        // After rollback, content should be freed.
        assert!(content_repo.read(&claim).is_err());
    }

    #[test]
    fn drop_without_commit_triggers_rollback() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile(99)).unwrap();

        {
            let mut session = make_session(vec![conn.clone()]);
            let _ff = session.get().unwrap();
            assert_eq!(conn.count(), 0);
            // Drop without commit.
        }

        // FlowFile should be back in the connection via auto-rollback.
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn penalize_sets_future_timestamp() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        assert_eq!(ff.penalized_until_nanos, 0);

        let penalized = session.penalize(ff);
        assert!(penalized.penalized_until_nanos > 0);
    }

    #[test]
    fn acquired_count_tracks_gets() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        for i in 0..3 {
            conn.try_send(make_flowfile(i)).unwrap();
        }

        let mut session = make_session(vec![conn]);
        session.get();
        session.get();
        assert_eq!(session.acquired_count(), 2);
    }

    #[test]
    fn committed_remove_ids_tracked() {
        let mut session = make_session(vec![]);
        let ff1 = session.create();
        let ff2 = session.create();
        let id1 = ff1.id;
        let id2 = ff2.id;

        session.remove(ff1);
        session.remove(ff2);
        session.commit();

        let remove_ids = session.take_committed_remove_ids();
        assert_eq!(remove_ids.len(), 2);
        assert!(remove_ids.contains(&id1));
        assert!(remove_ids.contains(&id2));
    }

    // ── Provenance tests ──────────────────────────────────────────────────

    #[test]
    fn provenance_create_records_event() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let _ff = session.create();
        session.commit();

        assert_eq!(prov_repo.event_count(), 1);
        let event = prov_repo.get_event(1).unwrap();
        assert_eq!(event.event_type, ProvenanceEventType::Create);
        assert_eq!(event.processor_name, "test-processor");
    }

    #[test]
    fn provenance_receive_records_event() {
        let conn = Arc::new(FlowConnection::new(
            "test-conn",
            BackPressureConfig::default(),
        ));
        conn.try_send(make_flowfile(42)).unwrap();

        let (mut session, prov_repo) = make_session_with_provenance(vec![conn]);
        let _ff = session.get().unwrap();
        session.commit();

        assert_eq!(prov_repo.event_count(), 1);
        let event = prov_repo.get_event(1).unwrap();
        assert_eq!(event.event_type, ProvenanceEventType::Receive);
        assert_eq!(event.flowfile_id, 42);
    }

    #[test]
    fn provenance_write_content_records_event() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let ff = session.create();
        let data = Bytes::from_static(b"test data");
        let _ff = session.write_content(ff, data).unwrap();
        session.commit();

        // Should have CREATE and CONTENT_MODIFIED events.
        assert_eq!(prov_repo.event_count(), 2);
    }

    #[test]
    fn provenance_clone_records_event() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let ff = session.create();
        let cloned = session.clone_flowfile(&ff);
        session.commit();

        // CREATE + CLONE.
        assert_eq!(prov_repo.event_count(), 2);

        // The clone event should reference the source FlowFile.
        let events = prov_repo.get_lineage(cloned.id);
        let clone_event = events
            .iter()
            .find(|e| e.event_type == ProvenanceEventType::Clone)
            .unwrap();
        assert_eq!(clone_event.source_flowfile_id, Some(ff.id));
    }

    #[test]
    fn provenance_transfer_records_route_event() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let ff = session.create();
        session.transfer(ff, &runifi_plugin_api::REL_SUCCESS);
        session.commit();

        // CREATE + ROUTE.
        assert_eq!(prov_repo.event_count(), 2);
    }

    #[test]
    fn provenance_remove_records_drop_event() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let ff = session.create();
        session.remove(ff);
        session.commit();

        // CREATE + DROP.
        assert_eq!(prov_repo.event_count(), 2);
    }

    #[test]
    fn provenance_discarded_on_rollback() {
        let (mut session, prov_repo) = make_session_with_provenance(vec![]);
        let ff = session.create();
        session.transfer(ff, &runifi_plugin_api::REL_SUCCESS);
        session.rollback();

        // No events should be recorded after rollback.
        assert_eq!(prov_repo.event_count(), 0);
    }

    #[test]
    fn provenance_discarded_on_drop_without_commit() {
        let prov_repo = Arc::new(InMemoryProvenanceRepository::new());
        {
            let content_repo = Arc::new(InMemoryContentRepository::new());
            let id_gen = Arc::new(IdGenerator::new());
            let mut session = CoreProcessSession::new(content_repo, id_gen, vec![], 1000);
            session.set_provenance(
                prov_repo.clone(),
                "test-processor".to_string(),
                "TestProcessor".to_string(),
            );
            let _ff = session.create();
            // Drop without commit.
        }

        assert_eq!(prov_repo.event_count(), 0);
    }
}
