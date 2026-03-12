use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crate::dto::{FlowEdgeResponse, FlowNodeResponse, FlowResponse};
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/flow", get(get_flow))
}

async fn get_flow(State(state): State<ApiState>) -> Json<FlowResponse> {
    let handle = &state.handle;

    let processors: Vec<FlowNodeResponse> = handle
        .processors
        .iter()
        .map(|p| FlowNodeResponse {
            name: p.name.clone(),
            type_name: p.type_name.clone(),
        })
        .collect();

    let connections: Vec<FlowEdgeResponse> = handle
        .connections
        .iter()
        .map(|c| FlowEdgeResponse {
            source: c.source_name.clone(),
            relationship: c.relationship.clone(),
            destination: c.dest_name.clone(),
        })
        .collect();

    Json(FlowResponse {
        name: handle.flow_name.clone(),
        processors,
        connections,
    })
}
