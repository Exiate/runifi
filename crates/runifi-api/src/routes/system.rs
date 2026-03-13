use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crate::dto::SystemResponse;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/system", get(get_system))
}

async fn get_system(State(state): State<ApiState>) -> Json<SystemResponse> {
    let handle = &state.handle;
    Json(SystemResponse {
        flow_name: handle.flow_name.clone(),
        uptime_secs: handle.started_at.elapsed().as_secs(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        processor_count: handle.processors.read().len(),
        connection_count: handle.connections.read().len(),
    })
}
