use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::FlowFile;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;

use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;

/// Engine-internal extension of `ProcessSession` with routing and metrics methods.
///
/// Implemented by `CoreProcessSession`. `ProcessorNode` uses this supertrait so
/// it can access pending transfer data and session metrics after `on_trigger`
/// without depending on the concrete type. Security components that wrap the
/// session (e.g. audit middleware) should implement this trait as well.
pub trait EngineSession: ProcessSession {
    /// Take pending transfers for routing. Drains the internal buffer.
    fn take_transfers(&mut self) -> Vec<(FlowFile, &'static str)>;
    /// Whether this session has been committed.
    fn is_committed(&self) -> bool;
    /// Number of FlowFiles acquired from input connections this session.
    fn acquired_count(&self) -> usize;
    /// Total bytes of FlowFiles acquired from input connections this session.
    fn acquired_bytes(&self) -> u64;
    /// Upcast to `&mut dyn ProcessSession` for passing to the processor supervisor.
    ///
    /// Required because Rust does not automatically coerce `dyn SubTrait` to
    /// `dyn SuperTrait` at trait object boundaries.
    fn as_process_session_mut(&mut self) -> &mut dyn ProcessSession;
}

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
        }
    }
}

impl ProcessSession for CoreProcessSession {
    fn get(&mut self) -> Option<FlowFile> {
        for conn in &self.input_connections {
            if let Some(ff) = conn.try_recv() {
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

        // Increment content ref count if there's a content claim.
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
        self.pending_transfers.push(PendingTransfer {
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
        let penalty_nanos = self.yield_duration_ms * 1_000_000;
        flowfile.penalized_until_nanos = now + penalty_nanos;
        flowfile
    }

    fn commit(&mut self) {
        // Decrement ref counts for removed FlowFiles.
        for ff in self.pending_removes.drain(..) {
            if let Some(claim) = &ff.content_claim {
                let _ = self.content_repo.decrement_ref(claim.resource_id);
            }
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
        self.committed = false;
    }
}

impl EngineSession for CoreProcessSession {
    fn take_transfers(&mut self) -> Vec<(FlowFile, &'static str)> {
        self.pending_transfers
            .drain(..)
            .map(|t| (t.flowfile, t.relationship_name))
            .collect()
    }

    fn is_committed(&self) -> bool {
        self.committed
    }

    fn acquired_count(&self) -> usize {
        self.acquired_flowfiles.len()
    }

    fn acquired_bytes(&self) -> u64 {
        self.acquired_flowfiles.iter().map(|ff| ff.size).sum()
    }

    fn as_process_session_mut(&mut self) -> &mut dyn ProcessSession {
        self
    }
}

impl Drop for CoreProcessSession {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}
