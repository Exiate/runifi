use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crate::dto::ConnectionResponse;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/connections", get(list_connections))
}

async fn list_connections(State(state): State<ApiState>) -> Json<Vec<ConnectionResponse>> {
    let connections: Vec<ConnectionResponse> = state
        .handle
        .connections
        .iter()
        .map(|info| ConnectionResponse {
            id: info.id.clone(),
            source_name: info.source_name.clone(),
            relationship: info.relationship.clone(),
            dest_name: info.dest_name.clone(),
            queued_count: info.connection.count(),
            queued_bytes: info.connection.bytes(),
            back_pressured: info.connection.is_back_pressured(),
        })
        .collect();
    Json(connections)
}
