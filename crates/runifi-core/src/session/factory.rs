use std::sync::Arc;

use super::process_session::{CoreProcessSession, EngineSession};
use crate::connection::flow_connection::FlowConnection;
use crate::id::IdGenerator;
use crate::repository::content_repo::ContentRepository;

/// Factory trait for creating engine session instances.
///
/// Abstracting session creation behind a trait allows security components
/// (e.g. encrypted content repos, audit middleware) to inject custom session
/// implementations without modifying the engine or processor nodes.
pub trait SessionFactory: Send + Sync {
    /// Create a new session for a single processor invocation.
    fn create_session(
        &self,
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        input_connections: Vec<Arc<FlowConnection>>,
        yield_duration_ms: u64,
    ) -> Box<dyn EngineSession>;
}

/// Default session factory that creates `CoreProcessSession` instances.
pub struct DefaultSessionFactory;

impl SessionFactory for DefaultSessionFactory {
    fn create_session(
        &self,
        content_repo: Arc<dyn ContentRepository>,
        id_gen: Arc<IdGenerator>,
        input_connections: Vec<Arc<FlowConnection>>,
        yield_duration_ms: u64,
    ) -> Box<dyn EngineSession> {
        Box::new(CoreProcessSession::new(
            content_repo,
            id_gen,
            input_connections,
            yield_duration_ms,
        ))
    }
}
