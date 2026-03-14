use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router, middleware};

use crate::dto::{
    FlowEdgeResponse, FlowLabelResponse, FlowNodeResponse, FlowProcessGroupResponse, FlowResponse,
    PositionResponse,
};
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/flow", get(get_flow))
        .layer(middleware::from_fn(rbac::require_view_flow))
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

    let labels: Vec<FlowLabelResponse> = handle
        .list_labels()
        .into_iter()
        .map(|l| FlowLabelResponse {
            id: l.id,
            text: l.text,
            x: l.x,
            y: l.y,
            width: l.width,
            height: l.height,
            background_color: l.background_color,
            font_size: l.font_size,
        })
        .collect();

    let groups = handle.process_groups.read();
    let process_group_count = groups.len();

    let process_groups: Vec<FlowProcessGroupResponse> = groups
        .iter()
        .filter(|g| g.parent_group_id.is_none())
        .map(|g| {
            let pos = handle
                .get_position(&g.id)
                .map(|p| PositionResponse { x: p.x, y: p.y });
            FlowProcessGroupResponse {
                id: g.id.clone(),
                name: g.name.clone(),
                processor_count: g.processor_names.len(),
                input_port_count: g.input_ports.len(),
                output_port_count: g.output_ports.len(),
                position: pos,
            }
        })
        .collect();
    drop(groups);

    Json(FlowResponse {
        name: handle.flow_name.clone(),
        processors,
        connections,
        labels,
        process_group_count,
        process_groups,
    })
}
