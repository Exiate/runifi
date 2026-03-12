use std::sync::Arc;

use runifi_core::engine::handle::EngineHandle;

/// Shared API state, cheap to clone via Arc.
#[derive(Clone)]
pub struct ApiState {
    pub handle: Arc<EngineHandle>,
}

impl ApiState {
    pub fn new(handle: EngineHandle) -> Self {
        Self {
            handle: Arc::new(handle),
        }
    }
}
