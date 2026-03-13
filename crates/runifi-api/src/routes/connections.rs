use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;

use runifi_core::connection::back_pressure::BackPressureConfig;

use crate::dto::{
    ConnectionDetailResponse, ConnectionResponse, CreateConnectionRequest,
    FlowFileAttributeResponse, QueueListingResponse, QueuedFlowFileResponse,
};
use crate::error::ApiError;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/connections", get(list_connections).post(create_connection))
        .route("/api/v1/connections/{id}", delete(delete_connection))
        .route(
            "/api/v1/connections/{id}/queue",
            get(list_queue).delete(empty_queue),
        )
        .route(
            "/api/v1/connections/{id}/queue/{flowfile_id}",
            get(get_flowfile).delete(remove_flowfile),
        )
        .route(
            "/api/v1/connections/{id}/queue/{flowfile_id}/content",
            get(download_content),
        )
}

async fn list_connections(State(state): State<ApiState>) -> Json<Vec<ConnectionResponse>> {
    let connections: Vec<ConnectionResponse> = state
        .handle
        .connections
        .read()
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

/// Create a new connection between two processors at runtime.
async fn create_connection(
    State(state): State<ApiState>,
    Json(body): Json<CreateConnectionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let bp_config = BackPressureConfig::new(
        body.max_queue_size
            .unwrap_or(BackPressureConfig::DEFAULT_MAX_COUNT),
        body.max_queue_bytes
            .unwrap_or(BackPressureConfig::DEFAULT_MAX_BYTES),
    );

    let conn_id = state
        .handle
        .add_connection(
            body.source.clone(),
            body.relationship.clone(),
            body.destination.clone(),
            bp_config,
        )
        .await
        .map_err(ApiError::from)?;

    // Build response from the newly registered connection info.
    let conn_detail = {
        let conns = state.handle.connections.read();
        conns
            .iter()
            .find(|c| c.id == conn_id)
            .map(|info| ConnectionDetailResponse {
                id: info.id.clone(),
                source_name: info.source_name.clone(),
                relationship: info.relationship.clone(),
                dest_name: info.dest_name.clone(),
                queued_count: info.connection.count(),
                queued_bytes: info.connection.bytes(),
                back_pressured: info.connection.is_back_pressured(),
            })
    };

    match conn_detail {
        Some(detail) => Ok((StatusCode::CREATED, Json(detail)).into_response()),
        None => Err(ApiError::ConnectionNotFound(conn_id)),
    }
}

/// Remove a connection at runtime.
#[derive(Deserialize)]
struct DeleteConnectionParams {
    force: Option<bool>,
}

async fn delete_connection(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(params): Query<DeleteConnectionParams>,
) -> Result<impl IntoResponse, ApiError> {
    let force = params.force.unwrap_or(false);
    state
        .handle
        .remove_connection(id, force)
        .await
        .map_err(ApiError::from)?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct QueueListParams {
    offset: Option<usize>,
    limit: Option<usize>,
}

/// List FlowFiles in a connection queue (paginated).
async fn list_queue(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(params): Query<QueueListParams>,
) -> Result<Json<QueueListingResponse>, ApiError> {
    let handle = &state.handle;
    let conns = handle.connections.read();
    let conn_info = conns
        .iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::ConnectionNotFound(id.clone()))?;

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(1000);

    let total_count = conn_info.connection.queue_snapshot_count();
    let snapshots = conn_info.connection.queue_snapshot(offset, limit);

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let flowfiles: Vec<QueuedFlowFileResponse> = snapshots
        .iter()
        .enumerate()
        .map(|(i, snap)| {
            let age_ms = if snap.created_at_nanos > 0 && now_nanos > snap.created_at_nanos {
                (now_nanos - snap.created_at_nanos) / 1_000_000
            } else {
                0
            };

            QueuedFlowFileResponse {
                id: snap.id,
                attributes: snap
                    .attributes
                    .iter()
                    .map(|(k, v)| FlowFileAttributeResponse {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
                size: snap.size,
                age_ms,
                has_content: snap.content_claim.is_some(),
                position: offset + i,
            }
        })
        .collect();

    Ok(Json(QueueListingResponse {
        connection_id: id,
        total_count,
        offset,
        limit,
        flowfiles,
    }))
}

/// Get a single FlowFile's details from the queue.
async fn get_flowfile(
    State(state): State<ApiState>,
    Path((id, flowfile_id)): Path<(String, u64)>,
) -> Result<Json<QueuedFlowFileResponse>, ApiError> {
    let handle = &state.handle;
    let conns = handle.connections.read();
    let conn_info = conns
        .iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::ConnectionNotFound(id.clone()))?;

    let (position, snap) = conn_info
        .connection
        .queue_get_with_position(flowfile_id)
        .ok_or(ApiError::FlowFileNotFound(flowfile_id))?;

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let age_ms = if snap.created_at_nanos > 0 && now_nanos > snap.created_at_nanos {
        (now_nanos - snap.created_at_nanos) / 1_000_000
    } else {
        0
    };

    Ok(Json(QueuedFlowFileResponse {
        id: snap.id,
        attributes: snap
            .attributes
            .iter()
            .map(|(k, v)| FlowFileAttributeResponse {
                key: k.to_string(),
                value: v.to_string(),
            })
            .collect(),
        size: snap.size,
        age_ms,
        has_content: snap.content_claim.is_some(),
        position,
    }))
}

/// Download the content of a FlowFile in the queue.
async fn download_content(
    State(state): State<ApiState>,
    Path((id, flowfile_id)): Path<(String, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = &state.handle;

    let (snap, content) = {
        let conns = handle.connections.read();
        let conn_info = conns
            .iter()
            .find(|c| c.id == id)
            .ok_or(ApiError::ConnectionNotFound(id.clone()))?;

        let snap = conn_info
            .connection
            .queue_get(flowfile_id)
            .ok_or(ApiError::FlowFileNotFound(flowfile_id))?;

        let claim = snap
            .content_claim
            .as_ref()
            .ok_or(ApiError::ContentNotAvailable(flowfile_id))?;

        let content = handle
            .content_repo
            .read(claim)
            .map_err(|_| ApiError::ContentNotAvailable(flowfile_id))?;

        (snap, content)
    };

    // Determine filename from attributes if available, then sanitize.
    let raw_filename = snap
        .attributes
        .iter()
        .find(|(k, _)| k.as_ref() == "filename")
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| format!("flowfile-{}", flowfile_id));

    let safe_filename: String = raw_filename
        .chars()
        .filter(|c| !c.is_control() && c.is_ascii())
        .map(|c| match c {
            '"' | '\\' => '_',
            _ => c,
        })
        .collect();

    let safe_filename = if safe_filename.is_empty() {
        format!("flowfile-{}", flowfile_id)
    } else {
        safe_filename
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", safe_filename),
            ),
            (header::CONTENT_LENGTH, content.len().to_string()),
        ],
        content,
    ))
}

/// Empty all FlowFiles from a connection queue.
async fn empty_queue(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = &state.handle;
    let conns = handle.connections.read();
    let conn_info = conns
        .iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::ConnectionNotFound(id.clone()))?;

    let removed = conn_info.connection.clear_queue();

    Ok(Json(serde_json::json!({
        "connection_id": id,
        "removed_count": removed,
    })))
}

/// Remove a specific FlowFile from the queue.
async fn remove_flowfile(
    State(state): State<ApiState>,
    Path((id, flowfile_id)): Path<(String, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let handle = &state.handle;
    let conns = handle.connections.read();
    let conn_info = conns
        .iter()
        .find(|c| c.id == id)
        .ok_or(ApiError::ConnectionNotFound(id.clone()))?;

    if conn_info.connection.remove_flowfile(flowfile_id) {
        Ok(Json(serde_json::json!({
            "connection_id": id,
            "flowfile_id": flowfile_id,
            "status": "removed",
        })))
    } else {
        Err(ApiError::FlowFileNotFound(flowfile_id))
    }
}
