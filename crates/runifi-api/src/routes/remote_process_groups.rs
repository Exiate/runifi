//! API routes for Remote Process Groups and Site-to-Site port listing.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};

use runifi_core::engine::remote_process_group::{
    RemotePortInfo, RemoteProcessGroup, RemoteProcessGroupInfo,
};

use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route(
            "/api/v1/remote-process-groups",
            get(list_remote_process_groups),
        )
        .route(
            "/api/v1/remote-process-groups/{id}",
            get(get_remote_process_group),
        )
        .route(
            "/api/v1/remote-process-groups/{id}/ports",
            get(list_remote_ports),
        )
        .route("/api/v1/site-to-site/ports", get(list_s2s_ports))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route(
            "/api/v1/remote-process-groups",
            post(create_remote_process_group),
        )
        .route(
            "/api/v1/remote-process-groups/{id}",
            put(update_remote_process_group),
        )
        .route(
            "/api/v1/remote-process-groups/{id}",
            delete_method(delete_remote_process_group),
        )
        .route(
            "/api/v1/remote-process-groups/{id}/transmission",
            put(set_transmission),
        )
        .route(
            "/api/v1/remote-process-groups/{id}/ports/{port_id}/transmission",
            put(set_port_transmission),
        )
        .layer(middleware::from_fn(rbac::require_modify_flow));

    view_routes.merge(modify_routes)
}

// ── DTOs ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateRpgRequest {
    pub name: String,
    pub target_uris: Vec<String>,
    #[serde(default)]
    pub transport_protocol: Option<String>,
    #[serde(default)]
    pub communications_timeout_ms: Option<u64>,
    #[serde(default)]
    pub yield_duration_ms: Option<u64>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub batch_size_bytes: Option<u64>,
    #[serde(default)]
    pub batch_duration_ms: Option<u64>,
    #[serde(default)]
    pub proxy_host: Option<String>,
    #[serde(default)]
    pub proxy_port: Option<u16>,
    #[serde(default)]
    pub parent_group_id: Option<String>,
    #[serde(default)]
    pub position: Option<PositionDto>,
    #[serde(default)]
    pub comments: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateRpgRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub target_uris: Option<Vec<String>>,
    #[serde(default)]
    pub transport_protocol: Option<String>,
    #[serde(default)]
    pub communications_timeout_ms: Option<u64>,
    #[serde(default)]
    pub yield_duration_ms: Option<u64>,
    #[serde(default)]
    pub batch_count: Option<usize>,
    #[serde(default)]
    pub batch_size_bytes: Option<u64>,
    #[serde(default)]
    pub batch_duration_ms: Option<u64>,
    #[serde(default)]
    pub proxy_host: Option<Option<String>>,
    #[serde(default)]
    pub proxy_port: Option<Option<u16>>,
    #[serde(default)]
    pub comments: Option<String>,
}

#[derive(Deserialize)]
pub struct TransmissionRequest {
    pub transmitting: bool,
}

#[derive(Deserialize, Serialize)]
pub struct PositionDto {
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize)]
pub struct RpgResponse {
    pub id: String,
    pub name: String,
    pub target_uris: Vec<String>,
    pub transport_protocol: String,
    pub communications_timeout_ms: u64,
    pub yield_duration_ms: u64,
    pub batch_count: usize,
    pub batch_size_bytes: u64,
    pub batch_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_port: Option<u16>,
    pub transmitting: bool,
    pub input_ports: Vec<RemotePortDto>,
    pub output_ports: Vec<RemotePortDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<PositionDto>,
    pub comments: String,
    pub metrics: MetricsDto,
}

#[derive(Serialize)]
pub struct RemotePortDto {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    pub connected: bool,
    pub transmitting: bool,
    pub exists_on_remote: bool,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub flow_files_sent: u64,
    pub flow_files_received: u64,
}

#[derive(Serialize)]
pub struct MetricsDto {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub flow_files_sent: u64,
    pub flow_files_received: u64,
    pub active_thread_count: u32,
    pub transmission_status: String,
}

/// Response for the S2S ports endpoint (local ports available for S2S).
#[derive(Serialize)]
pub struct S2sPortDto {
    pub id: String,
    pub name: String,
    pub port_type: String,
    pub group_id: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn list_remote_process_groups(State(state): State<ApiState>) -> Json<Vec<RpgResponse>> {
    let rpgs = state.handle.list_remote_process_groups();
    Json(rpgs.into_iter().map(to_response).collect())
}

async fn get_remote_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<RpgResponse>, ApiError> {
    let rpg = state
        .handle
        .get_remote_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Remote process group not found: {}", id)))?;
    Ok(Json(to_response(rpg)))
}

async fn create_remote_process_group(
    State(state): State<ApiState>,
    Json(body): Json<CreateRpgRequest>,
) -> Result<impl IntoResponse, ApiError> {
    validate_rpg_name(&body.name)?;

    if body.target_uris.is_empty() {
        return Err(ApiError::BadRequest(
            "At least one target URI is required".to_string(),
        ));
    }

    let mut rpg = RemoteProcessGroup::new(body.name, body.target_uris);

    if let Some(tp) = body.transport_protocol {
        rpg.transport_protocol = tp;
    }
    if let Some(ct) = body.communications_timeout_ms {
        rpg.communications_timeout_ms = ct;
    }
    if let Some(yd) = body.yield_duration_ms {
        rpg.yield_duration_ms = yd;
    }
    if let Some(bc) = body.batch_count {
        rpg.batch_count = bc;
    }
    if let Some(bs) = body.batch_size_bytes {
        rpg.batch_size_bytes = bs;
    }
    if let Some(bd) = body.batch_duration_ms {
        rpg.batch_duration_ms = bd;
    }
    if let Some(ph) = body.proxy_host {
        rpg.proxy_host = Some(ph);
    }
    if let Some(pp) = body.proxy_port {
        rpg.proxy_port = Some(pp);
    }
    if let Some(pg) = body.parent_group_id {
        rpg.parent_group_id = Some(pg);
    }
    if let Some(pos) = body.position {
        rpg.position = Some((pos.x, pos.y));
    }
    if let Some(c) = body.comments {
        rpg.comments = c;
    }

    let id = state.handle.add_remote_process_group(rpg);

    let info = state
        .handle
        .get_remote_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest("Failed to retrieve created RPG".to_string()))?;

    Ok((StatusCode::CREATED, Json(to_response(info))).into_response())
}

async fn update_remote_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateRpgRequest>,
) -> Result<Json<RpgResponse>, ApiError> {
    if let Some(ref name) = body.name {
        validate_rpg_name(name)?;
    }

    state
        .handle
        .update_remote_process_group(
            &id,
            body.name,
            body.target_uris,
            body.transport_protocol,
            body.communications_timeout_ms,
            body.yield_duration_ms,
            body.batch_count,
            body.batch_size_bytes,
            body.batch_duration_ms,
            body.proxy_host,
            body.proxy_port,
            body.comments,
        )
        .map_err(ApiError::BadRequest)?;

    let info = state
        .handle
        .get_remote_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Remote process group not found: {}", id)))?;

    Ok(Json(to_response(info)))
}

async fn delete_remote_process_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_remote_process_group(&id)
        .map_err(ApiError::BadRequest)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_transmission(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<TransmissionRequest>,
) -> Result<Json<RpgResponse>, ApiError> {
    state
        .handle
        .set_rpg_transmitting(&id, body.transmitting)
        .map_err(ApiError::BadRequest)?;

    let info = state
        .handle
        .get_remote_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Remote process group not found: {}", id)))?;

    Ok(Json(to_response(info)))
}

async fn list_remote_ports(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<RemotePortDto>>, ApiError> {
    let info = state
        .handle
        .get_remote_process_group(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Remote process group not found: {}", id)))?;

    let mut ports: Vec<RemotePortDto> = info
        .input_ports
        .into_iter()
        .chain(info.output_ports)
        .map(port_to_dto)
        .collect();

    ports.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(ports))
}

async fn set_port_transmission(
    State(state): State<ApiState>,
    Path((id, port_id)): Path<(String, String)>,
    Json(body): Json<TransmissionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .set_rpg_port_transmitting(&id, &port_id, body.transmitting)
        .map_err(ApiError::BadRequest)?;

    Ok(Json(serde_json::json!({
        "rpg_id": id,
        "port_id": port_id,
        "transmitting": body.transmitting,
    })))
}

/// List local ports available for Site-to-Site (inbound endpoint).
/// Returns all input/output ports on all process groups.
async fn list_s2s_ports(State(state): State<ApiState>) -> Json<Vec<S2sPortDto>> {
    let groups = state.handle.list_process_groups();
    let mut ports = Vec::new();

    for group in groups {
        for port in &group.input_ports {
            ports.push(S2sPortDto {
                id: port.id.clone(),
                name: port.name.clone(),
                port_type: "INPUT_PORT".to_string(),
                group_id: group.id.clone(),
            });
        }
        for port in &group.output_ports {
            ports.push(S2sPortDto {
                id: port.id.clone(),
                name: port.name.clone(),
                port_type: "OUTPUT_PORT".to_string(),
                group_id: group.id.clone(),
            });
        }
    }

    Json(ports)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn validate_rpg_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Remote process group name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ApiError::BadRequest(
            "Remote process group name must not exceed 128 characters".to_string(),
        ));
    }
    Ok(())
}

fn to_response(info: RemoteProcessGroupInfo) -> RpgResponse {
    RpgResponse {
        id: info.id,
        name: info.name,
        target_uris: info.target_uris,
        transport_protocol: info.transport_protocol,
        communications_timeout_ms: info.communications_timeout_ms,
        yield_duration_ms: info.yield_duration_ms,
        batch_count: info.batch_count,
        batch_size_bytes: info.batch_size_bytes,
        batch_duration_ms: info.batch_duration_ms,
        proxy_host: info.proxy_host,
        proxy_port: info.proxy_port,
        transmitting: info.transmitting,
        input_ports: info.input_ports.into_iter().map(port_to_dto).collect(),
        output_ports: info.output_ports.into_iter().map(port_to_dto).collect(),
        parent_group_id: info.parent_group_id,
        position: info.position.map(|(x, y)| PositionDto { x, y }),
        comments: info.comments,
        metrics: MetricsDto {
            bytes_sent: info.metrics.bytes_sent,
            bytes_received: info.metrics.bytes_received,
            flow_files_sent: info.metrics.flow_files_sent,
            flow_files_received: info.metrics.flow_files_received,
            active_thread_count: info.metrics.active_thread_count,
            transmission_status: info.metrics.transmission_status.to_string(),
        },
    }
}

fn port_to_dto(port: RemotePortInfo) -> RemotePortDto {
    RemotePortDto {
        id: port.id,
        name: port.name,
        target_id: port.target_id,
        connected: port.connected,
        transmitting: port.transmitting,
        exists_on_remote: port.exists_on_remote,
        bytes_sent: port.bytes_sent,
        bytes_received: port.bytes_received,
        flow_files_sent: port.flow_files_sent,
        flow_files_received: port.flow_files_received,
    }
}
