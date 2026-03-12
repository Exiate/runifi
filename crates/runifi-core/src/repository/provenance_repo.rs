use crate::error::Result;

/// A provenance event recording data lineage.
#[derive(Debug, Clone)]
pub struct ProvenanceEvent {
    pub event_id: u64,
    pub flowfile_id: u64,
    pub event_type: ProvenanceEventType,
    pub processor_id: String,
    pub timestamp_nanos: u64,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvenanceEventType {
    Create,
    Receive,
    Send,
    Clone,
    Modify,
    Route,
    Drop,
}

/// Trait for provenance event storage (append-only log).
pub trait ProvenanceRepository: Send + Sync {
    fn record(&self, event: ProvenanceEvent) -> Result<()>;
}

/// In-memory provenance repository (for development/testing).
pub struct InMemoryProvenanceRepository;

impl ProvenanceRepository for InMemoryProvenanceRepository {
    fn record(&self, _event: ProvenanceEvent) -> Result<()> {
        // No-op for now
        Ok(())
    }
}
