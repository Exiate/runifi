use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::FlowFile;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;

use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;

/// Pending transfer within a group session.
struct GroupPendingTransfer {
    flowfile: FlowFile,
    relationship_name: &'static str,
}

/// Default penalty duration in milliseconds (30 seconds).
const DEFAULT_PENALTY_DURATION_MS: u64 = 30_000;

/// A transactional session spanning all processors in a stateless process group.
///
/// All operations are buffered until the entire group completes successfully.
/// On any failure, everything rolls back and source FlowFiles return to input.
///
/// Key behavior:
/// - `commit()` is a no-op — actual commit is deferred to `group_commit()`.
/// - `rollback()` is a no-op — the executor handles group-level rollback.
/// - `get()` pulls from the internal queue first (inter-processor transfers),
///   then from input connections.
pub struct GroupSession {
    content_repo: Arc<dyn ContentRepository>,
    id_gen: Arc<IdGenerator>,
    input_connections: Vec<Arc<FlowConnection>>,

    // Buffered state across all processors in the group
    pending_transfers: Vec<GroupPendingTransfer>,
    pending_removes: Vec<FlowFile>,
    acquired_flowfiles: Vec<FlowFile>,
    created_content_claims: Vec<u64>,
    committed: bool,

    /// FlowFiles transferred between processors within the group.
    /// Available for the next processor via `get()`.
    internal_queue: Vec<FlowFile>,
}

impl GroupSession {
    pub fn new(
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        input_connections: Vec<Arc<FlowConnection>>,
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
            internal_queue: Vec::new(),
        }
    }

    /// Feed FlowFiles into the internal queue for the next processor to consume.
    pub fn feed_internal(&mut self, flowfiles: Vec<FlowFile>) {
        self.internal_queue.extend(flowfiles);
    }

    /// Take pending transfers (FlowFile + relationship name).
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

    /// Commit the entire group transaction.
    ///
    /// Decrements ref counts for removed FlowFiles and marks the session as committed.
    pub fn group_commit(&mut self) {
        for ff in self.pending_removes.drain(..) {
            if let Some(claim) = &ff.content_claim {
                let _ = self.content_repo.decrement_ref(claim.resource_id);
            }
        }
        self.committed = true;
    }

    /// Rollback the entire group transaction.
    ///
    /// Returns acquired FlowFiles to input connections and cleans up created content.
    pub fn group_rollback(&mut self) {
        // Return acquired FlowFiles to input connections.
        for ff in self.acquired_flowfiles.drain(..) {
            for conn in &self.input_connections {
                if conn.try_send(ff.clone()).is_ok() {
                    break;
                }
            }
        }
        // Clean up created content.
        for resource_id in self.created_content_claims.drain(..) {
            let _ = self.content_repo.decrement_ref(resource_id);
        }
        self.pending_transfers.clear();
        self.pending_removes.clear();
        self.internal_queue.clear();
        self.committed = false;
    }

    /// Number of FlowFiles acquired from input connections.
    pub fn acquired_count(&self) -> usize {
        self.acquired_flowfiles.len()
    }

    /// Total bytes of FlowFiles acquired from input connections.
    pub fn acquired_bytes(&self) -> u64 {
        self.acquired_flowfiles.iter().map(|ff| ff.size).sum()
    }
}

impl ProcessSession for GroupSession {
    fn get(&mut self) -> Option<FlowFile> {
        // Try internal queue first (inter-processor transfers within group).
        if !self.internal_queue.is_empty() {
            return Some(self.internal_queue.remove(0));
        }

        // Fall back to input connections using FIFO (oldest-first) logic.
        if self.input_connections.is_empty() {
            return None;
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let oldest_idx = self
            .input_connections
            .iter()
            .enumerate()
            .filter(|(_, conn)| conn.is_front_penalized(now_nanos) != Some(true))
            .filter_map(|(i, conn)| conn.peek_oldest_timestamp().map(|ts| (i, ts)))
            .min_by_key(|(_, ts)| *ts)
            .map(|(i, _)| i);

        if let Some(idx) = oldest_idx {
            let conn = &self.input_connections[idx];
            if let Some(ff) = conn.try_recv() {
                if ff.is_penalized(now_nanos) {
                    let _ = conn.try_send(ff);
                    return None;
                }
                self.acquired_flowfiles.push(ff.clone());
                return Some(ff);
            }
        }
        None
    }

    fn get_batch(&mut self, max: usize) -> Vec<FlowFile> {
        let mut batch = Vec::with_capacity(max);

        // Drain internal queue first.
        while batch.len() < max && !self.internal_queue.is_empty() {
            batch.push(self.internal_queue.remove(0));
        }

        if batch.len() >= max || self.input_connections.is_empty() {
            return batch;
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Then from input connections using FIFO ordering.
        while batch.len() < max {
            let oldest_idx = self
                .input_connections
                .iter()
                .enumerate()
                .filter(|(_, conn)| conn.is_front_penalized(now_nanos) != Some(true))
                .filter_map(|(i, conn)| conn.peek_oldest_timestamp().map(|ts| (i, ts)))
                .min_by_key(|(_, ts)| *ts)
                .map(|(i, _)| i);

            match oldest_idx {
                Some(idx) => {
                    let conn = &self.input_connections[idx];
                    if let Some(ff) = conn.try_recv() {
                        if ff.is_penalized(now_nanos) {
                            let _ = conn.try_send(ff);
                            continue;
                        }
                        self.acquired_flowfiles.push(ff.clone());
                        batch.push(ff);
                    } else {
                        continue;
                    }
                }
                None => break,
            }
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
        Ok(flowfile)
    }

    fn create(&mut self) -> FlowFile {
        let id = self.id_gen.next_id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: now,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    fn clone_flowfile(&mut self, flowfile: &FlowFile) -> FlowFile {
        let new_id = self.id_gen.next_id();
        if let Some(claim) = &flowfile.content_claim {
            let _ = self.content_repo.increment_ref(claim.resource_id);
        }
        FlowFile {
            id: new_id,
            attributes: flowfile.attributes.clone(),
            content_claim: flowfile.content_claim.clone(),
            size: flowfile.size,
            created_at_nanos: flowfile.created_at_nanos,
            lineage_start_id: flowfile.lineage_start_id,
            penalized_until_nanos: 0,
        }
    }

    fn transfer(&mut self, flowfile: FlowFile, relationship: &Relationship) {
        self.pending_transfers.push(GroupPendingTransfer {
            flowfile,
            relationship_name: relationship.name,
        });
    }

    fn remove(&mut self, flowfile: FlowFile) {
        self.pending_removes.push(flowfile);
    }

    fn penalize(&mut self, mut flowfile: FlowFile) -> FlowFile {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let penalty_nanos = DEFAULT_PENALTY_DURATION_MS * 1_000_000;
        flowfile.penalized_until_nanos = now + penalty_nanos;
        flowfile
    }

    fn commit(&mut self) {
        // No-op in group session — actual commit deferred to group_commit().
    }

    fn rollback(&mut self) {
        // No-op for individual processor rollback in group session.
        // The executor handles group-level rollback.
    }
}

impl Drop for GroupSession {
    fn drop(&mut self) {
        if !self.committed {
            self.group_rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::back_pressure::BackPressureConfig;
    use crate::repository::content_memory::InMemoryContentRepository;

    fn make_session(input_connections: Vec<Arc<FlowConnection>>) -> GroupSession {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        GroupSession::new(content_repo, id_gen, input_connections)
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

    fn make_flowfile_with_time(id: u64, created_at_nanos: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos,
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
    fn transfer_buffers_pending() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        session.transfer(ff, &runifi_plugin_api::REL_SUCCESS);

        let transfers = session.take_transfers();
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].1, "success");
    }

    #[test]
    fn commit_is_noop() {
        let mut session = make_session(vec![]);
        let ff = session.create();
        session.transfer(ff, &runifi_plugin_api::REL_SUCCESS);
        session.commit();
        // commit() is a no-op — session should NOT be marked committed.
        assert!(!session.is_committed());
    }

    #[test]
    fn group_commit_marks_committed() {
        let mut session = make_session(vec![]);
        session.group_commit();
        assert!(session.is_committed());
    }

    #[test]
    fn group_commit_decrements_refs_for_removed() {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let mut session = GroupSession::new(content_repo.clone(), id_gen, vec![]);

        let ff = session.create();
        let data = Bytes::from_static(b"to be removed");
        let ff = session.write_content(ff, data).unwrap();
        let claim = ff.content_claim.clone().unwrap();

        session.remove(ff);
        // Before group_commit, content should still be accessible.
        assert!(content_repo.read(&claim).is_ok());

        session.group_commit();
        // After group_commit, content should be freed.
        assert!(content_repo.read(&claim).is_err());
    }

    #[test]
    fn group_rollback_returns_flowfiles_to_input() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile(42)).unwrap();
        assert_eq!(conn.count(), 1);

        let mut session = make_session(vec![conn.clone()]);
        let ff = session.get().unwrap();
        assert_eq!(ff.id, 42);
        assert_eq!(conn.count(), 0);

        session.group_rollback();
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn group_rollback_cleans_up_created_content() {
        let content_repo = Arc::new(InMemoryContentRepository::new());
        let id_gen = Arc::new(IdGenerator::new());
        let mut session = GroupSession::new(content_repo.clone(), id_gen, vec![]);

        let ff = session.create();
        let data = Bytes::from_static(b"will be rolled back");
        let ff = session.write_content(ff, data).unwrap();
        let claim = ff.content_claim.clone().unwrap();

        assert!(content_repo.read(&claim).is_ok());

        session.group_rollback();
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
            // Drop without group_commit.
        }

        // FlowFile should be back in the connection via auto-rollback.
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn internal_queue_fed_and_consumed() {
        let mut session = make_session(vec![]);

        let ff1 = make_flowfile(1);
        let ff2 = make_flowfile(2);
        session.feed_internal(vec![ff1, ff2]);

        let got1 = session.get().unwrap();
        assert_eq!(got1.id, 1);
        let got2 = session.get().unwrap();
        assert_eq!(got2.id, 2);
        assert!(session.get().is_none());
    }

    #[test]
    fn internal_queue_consumed_before_input_connections() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile_with_time(10, 100)).unwrap();

        let mut session = make_session(vec![conn]);
        session.feed_internal(vec![make_flowfile(1)]);

        // Internal queue item should come first.
        let got = session.get().unwrap();
        assert_eq!(got.id, 1);

        // Then from input connection.
        let got = session.get().unwrap();
        assert_eq!(got.id, 10);
    }

    #[test]
    fn get_batch_drains_internal_queue_first() {
        let conn = Arc::new(FlowConnection::new("test", BackPressureConfig::default()));
        conn.try_send(make_flowfile_with_time(10, 100)).unwrap();
        conn.try_send(make_flowfile_with_time(11, 200)).unwrap();

        let mut session = make_session(vec![conn]);
        session.feed_internal(vec![make_flowfile(1), make_flowfile(2)]);

        let batch = session.get_batch(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].id, 1); // internal
        assert_eq!(batch[1].id, 2); // internal
        assert_eq!(batch[2].id, 10); // from connection
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

        let original_content = session.read_content(&ff).unwrap();
        let cloned_content = session.read_content(&cloned).unwrap();
        assert_eq!(original_content, cloned_content);
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
    fn acquired_count_tracks_gets_from_connections() {
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
    fn internal_queue_does_not_count_as_acquired() {
        let mut session = make_session(vec![]);
        session.feed_internal(vec![make_flowfile(1), make_flowfile(2)]);
        session.get();
        session.get();
        // Internal queue items are not from input connections.
        assert_eq!(session.acquired_count(), 0);
    }

    #[test]
    fn group_rollback_clears_internal_queue() {
        let mut session = make_session(vec![]);
        session.feed_internal(vec![make_flowfile(1), make_flowfile(2)]);

        session.group_rollback();

        assert!(session.get().is_none());
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
}
