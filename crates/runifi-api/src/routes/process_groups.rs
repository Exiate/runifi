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
        .update_process_group(&id, body.name, body.variables)
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
    }
}
