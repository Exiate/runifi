use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/process-groups", get(list_process_groups))
        .route("/api/v1/process-groups/{id}", get(get_process_group))
        .route(
            "/api/v1/process-groups/{id}/flow",
            get(get_process_group_flow),
        )
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route("/api/v1/process-groups", post(create_process_group))
        .route("/api/v1/process-groups/{id}", put(update_process_group))
        .route(
            "/api/v1/process-groups/{id}",
            delete_method(delete_process_group),
        )
        .route(
            "/api/v1/process-groups/{id}/processors/{processor_name}",
            put(add_processor_to_group),
        )
        .route(
            "/api/v1/process-groups/{id}/processors/{processor_name}",
            delete_method(remove_processor_from_group),
        )
        .route(
            "/api/v1/process-groups/{id}/connections/{connection_id}",
            put(add_connection_to_group),
        )
        .route(
            "/api/v1/process-groups/{id}/connections/{connection_id}",
            delete_method(remove_connection_from_group),
        )
        .route(
            "/api/v1/process-groups/{id}/input-ports",
            post(add_input_port),
        )
        .route(
            "/api/v1/process-groups/{id}/output-ports",
            post(add_output_port),
        )
        .route(
            "/api/v1/process-groups/{id}/ports/{port_id}",
            delete_method(remove_port),
        )
        .route(
            "/api/v1/process-groups/{id}/position",
            put(update_process_group_position),
        )
        .layer(middleware::from_fn(rbac::require_modify_flow));

    view_routes.merge(modify_routes)
}

// ── DTOs ──────────────────────────────────────────────────────────────────────

/// Request body for creating a process group.
#[derive(Deserialize)]
pub struct CreateProcessGroupRequest {
    /// Human-readable name for the process group.
    pub name: String,
    /// Optional parent group ID for nesting.
    #[serde(default)]
    pub parent_group_id: Option<String>,
    /// Input port names.
    #[serde(default)]
    pub input_ports: Vec<String>,
    /// Output port names.
    #[serde(default)]
    pub output_ports: Vec<String>,
    /// Group-scoped variables.
    #[serde(default)]
    pub variables: HashMap<String, String>,
}

/// Request body for updating a process group.
#[derive(Deserialize)]
pub struct UpdateProcessGroupRequest {
    /// New name for the group (optional).
    #[serde(default)]
    pub name: Option<String>,
    /// New variables for the group (replaces existing, optional).
    #[serde(default)]
    pub variables: Option<HashMap<String, String>>,
    /// Free-form documentation text.
    #[serde(default)]
    pub comments: Option<String>,
    /// Default back-pressure FlowFile count for new connections in this group.
    #[serde(default)]
    pub default_back_pressure_count: Option<Option<usize>>,
    /// Default back-pressure byte threshold for new connections in this group.
    #[serde(default)]
    pub default_back_pressure_bytes: Option<Option<u64>>,
    /// Default FlowFile expiration in milliseconds for connections in this group.
    #[serde(default)]
    pub default_flowfile_expiration_ms: Option<Option<u64>>,
}

/// Request body for adding a port.
#[derive(Deserialize)]
pub struct AddPortRequest {
    /// Name for the new port.
    pub name: String,
}

/// Response for a process group.
#[derive(Serialize)]
pub struct ProcessGroupResponse {
    pub id: String,
    pub name: String,
    pub input_ports: Vec<PortResponse>,
    pub output_ports: Vec<PortResponse>,
    pub processor_names: Vec<String>,
    pub connection_ids: Vec<String>,
    pub child_group_ids: Vec<String>,
    pub parent_group_id: Option<String>,
    pub variables: HashMap<String, String>,
    pub comments: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_back_pressure_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_back_pressure_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_flowfile_expiration_ms: Option<u64>,
}

/// Response for a port.
#[derive(Serialize)]
pub struct PortResponse {
    pub id: String,
    pub name: String,
    pub port_type: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn list_process_groups(State(state): State<ApiState>) -> Json<Vec<ProcessGroupResponse>> {
    let groups = state.handle.list_process_groups();
    let response: Vec<ProcessGroupResponse> = groups.into_iter().map(to_response).collect();
    Json(response)
}

async fn get_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessGroupResponse>, ApiError> {
    let group = state
        .handle
        .get_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Process group not found: {}", id)))?;
    Ok(Json(to_response(group)))
}

async fn create_process_group(
    State(state): State<ApiState>,
    Json(body): Json<CreateProcessGroupRequest>,
) -> Result<impl IntoResponse, ApiError> {
    validate_group_name(&body.name)?;

    let group_id = state
        .handle
        .create_process_group(
            body.name,
            body.parent_group_id,
            body.input_ports,
            body.output_ports,
            body.variables,
        )
        .map_err(ApiError::BadRequest)?;

    let group = state
        .handle
        .get_process_group(&group_id)
        .ok_or_else(|| ApiError::BadRequest("Failed to retrieve created group".to_string()))?;

    Ok((StatusCode::CREATED, Json(to_response(group))).into_response())
}

async fn update_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateProcessGroupRequest>,
) -> Result<Json<ProcessGroupResponse>, ApiError> {
    if let Some(ref name) = body.name {
        validate_group_name(name)?;
    }

    state
        .handle
        .update_process_group(
            &id,
            body.name,
            body.variables,
            body.comments,
            body.default_back_pressure_count,
            body.default_back_pressure_bytes,
            body.default_flowfile_expiration_ms,
        )
        .map_err(ApiError::BadRequest)?;

    let group = state
        .handle
        .get_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Process group not found: {}", id)))?;

    Ok(Json(to_response(group)))
}

async fn delete_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_process_group(&id)
        .map_err(ApiError::Conflict)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_processor_to_group(
    State(state): State<ApiState>,
    Path((id, processor_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .add_processor_to_group(&id, &processor_name)
        .map_err(ApiError::BadRequest)?;

    Ok(Json(serde_json::json!({
        "status": "added",
        "group_id": id,
        "processor": processor_name,
    })))
}

async fn remove_processor_from_group(
    State(state): State<ApiState>,
    Path((id, processor_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_processor_from_group(&id, &processor_name)
        .map_err(ApiError::BadRequest)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_connection_to_group(
    State(state): State<ApiState>,
    Path((id, connection_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .add_connection_to_group(&id, &connection_id)
        .map_err(ApiError::BadRequest)?;

    Ok(Json(serde_json::json!({
        "status": "added",
        "group_id": id,
        "connection_id": connection_id,
    })))
}

async fn remove_connection_from_group(
    State(state): State<ApiState>,
    Path((id, connection_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_connection_from_group(&id, &connection_id)
        .map_err(ApiError::BadRequest)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn add_input_port(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<AddPortRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let port_id = state
        .handle
        .add_input_port(&id, body.name.clone())
        .map_err(ApiError::BadRequest)?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": port_id,
            "name": body.name,
            "port_type": "input",
            "group_id": id,
        })),
    )
        .into_response())
}

async fn add_output_port(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<AddPortRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let port_id = state
        .handle
        .add_output_port(&id, body.name.clone())
        .map_err(ApiError::BadRequest)?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": port_id,
            "name": body.name,
            "port_type": "output",
            "group_id": id,
        })),
    )
        .into_response())
}

async fn remove_port(
    State(state): State<ApiState>,
    Path((id, port_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_port(&id, &port_id)
        .map_err(ApiError::BadRequest)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn update_process_group_position(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<crate::dto::UpdatePositionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify the group exists.
    state
        .handle
        .get_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Process group not found: {}", id)))?;

    state.handle.positions.insert(
        id.clone(),
        runifi_core::engine::handle::Position {
            x: body.x,
            y: body.y,
        },
    );
    state.handle.notify_persist();

    Ok(Json(serde_json::json!({
        "id": id,
        "x": body.x,
        "y": body.y,
    })))
}

// ── Scoped flow topology ─────────────────────────────────────────────────────

/// Summary of a child process group in the scoped flow response.
#[derive(Serialize)]
pub struct ProcessGroupSummary {
    pub id: String,
    pub name: String,
    pub processor_count: usize,
    pub input_port_count: usize,
    pub output_port_count: usize,
    pub position: Option<crate::dto::PositionResponse>,
}

/// A breadcrumb segment for navigation.
#[derive(Serialize)]
pub struct BreadcrumbSegment {
    pub id: String,
    pub name: String,
}

/// Port summary in the scoped flow response.
#[derive(Serialize)]
pub struct PortSummary {
    pub id: String,
    pub name: String,
    pub port_type: String,
}

/// Scoped flow topology for a process group.
#[derive(Serialize)]
pub struct ProcessGroupFlowResponse {
    pub id: String,
    pub name: String,
    pub processors: Vec<crate::dto::FlowNodeResponse>,
    pub connections: Vec<crate::dto::FlowEdgeResponse>,
    pub child_groups: Vec<ProcessGroupSummary>,
    pub input_ports: Vec<PortSummary>,
    pub output_ports: Vec<PortSummary>,
    pub breadcrumb: Vec<BreadcrumbSegment>,
}

async fn get_process_group_flow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessGroupFlowResponse>, ApiError> {
    let flow_info = state
        .handle
        .get_group_flow(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Process group not found: {}", id)))?;

    let breadcrumb_path = state.handle.get_group_breadcrumb_path(&id);

    let processors: Vec<crate::dto::FlowNodeResponse> = flow_info
        .processors
        .iter()
        .map(|p| {
            let pos = state
                .handle
                .positions
                .get(&p.name)
                .map(|e| crate::dto::PositionResponse {
                    x: e.value().x,
                    y: e.value().y,
                });
            crate::dto::FlowNodeResponse {
                name: p.name.clone(),
                type_name: p.type_name.clone(),
                position: pos,
            }
        })
        .collect();

    let connections: Vec<crate::dto::FlowEdgeResponse> = flow_info
        .connections
        .iter()
        .map(|c| crate::dto::FlowEdgeResponse {
            id: c.id.clone(),
            source: c.source_name.clone(),
            relationship: c.relationship.clone(),
            destination: c.dest_name.clone(),
        })
        .collect();

    let child_groups: Vec<ProcessGroupSummary> = flow_info
        .child_groups
        .iter()
        .map(|g| {
            let pos = state
                .handle
                .positions
                .get(&g.id)
                .map(|e| crate::dto::PositionResponse {
                    x: e.value().x,
                    y: e.value().y,
                });
            ProcessGroupSummary {
                id: g.id.clone(),
                name: g.name.clone(),
                processor_count: g.processor_names.len(),
                input_port_count: g.input_ports.len(),
                output_port_count: g.output_ports.len(),
                position: pos,
            }
        })
        .collect();

    let input_ports: Vec<PortSummary> = flow_info
        .group
        .input_ports
        .iter()
        .map(|p| PortSummary {
            id: p.id.clone(),
            name: p.name.clone(),
            port_type: "input".to_string(),
        })
        .collect();

    let output_ports: Vec<PortSummary> = flow_info
        .group
        .output_ports
        .iter()
        .map(|p| PortSummary {
            id: p.id.clone(),
            name: p.name.clone(),
            port_type: "output".to_string(),
        })
        .collect();

    let breadcrumb: Vec<BreadcrumbSegment> = breadcrumb_path
        .into_iter()
        .map(|(seg_id, seg_name)| BreadcrumbSegment {
            id: seg_id,
            name: seg_name,
        })
        .collect();

    Ok(Json(ProcessGroupFlowResponse {
        id: flow_info.group.id,
        name: flow_info.group.name,
        processors,
        connections,
        child_groups,
        input_ports,
        output_ports,
        breadcrumb,
    }))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn validate_group_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Process group name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest(
            "Process group name must not exceed 128 characters".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ' ')
    {
        return Err(ApiError::BadRequest(
            "Process group name may only contain letters, digits, underscores, hyphens, and spaces"
                .to_string(),
        ));
    }
    Ok(())
}

fn to_response(
    group: runifi_core::engine::process_group::ProcessGroupInfo,
) -> ProcessGroupResponse {
    ProcessGroupResponse {
        id: group.id,
        name: group.name,
        input_ports: group
            .input_ports
            .into_iter()
            .map(|p| PortResponse {
                id: p.id,
                name: p.name,
                port_type: match p.port_type {
                    runifi_core::engine::process_group::PortType::Input => "input".to_string(),
                    runifi_core::engine::process_group::PortType::Output => "output".to_string(),
                },
            })
            .collect(),
        output_ports: group
            .output_ports
            .into_iter()
            .map(|p| PortResponse {
                id: p.id,
                name: p.name,
                port_type: match p.port_type {
                    runifi_core::engine::process_group::PortType::Input => "input".to_string(),
                    runifi_core::engine::process_group::PortType::Output => "output".to_string(),
                },
            })
            .collect(),
        processor_names: group.processor_names,
        connection_ids: group.connection_ids,
        child_group_ids: group.child_group_ids,
        parent_group_id: group.parent_group_id,
        variables: group.variables,
        comments: group.comments,
        default_back_pressure_count: group.default_back_pressure_count,
        default_back_pressure_bytes: group.default_back_pressure_bytes,
        default_flowfile_expiration_ms: group.default_flowfile_expiration_ms,
    }
}
