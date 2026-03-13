use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use crate::dto::{FlowEdgeResponse, FlowNodeResponse, FlowResponse, PositionResponse};
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new().route("/api/v1/flow", get(get_flow))
}

async fn get_flow(State(state): State<ApiState>) -> Json<FlowResponse> {
    let handle = &state.handle;

    let processors: Vec<FlowNodeResponse> = handle
        .processors
        .read()
        .iter()
        .map(|p| FlowNodeResponse {
            name: p.name.clone(),
            type_name: p.type_name.clone(),
            position: handle
                .get_position(&p.name)
                .map(|pos| PositionResponse { x: pos.x, y: pos.y }),
        })
        .collect();

    let connections: Vec<FlowEdgeResponse> = handle
        .connections
        .read()
        .iter()
        .map(|c| FlowEdgeResponse {
            id: c.id.clone(),
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
