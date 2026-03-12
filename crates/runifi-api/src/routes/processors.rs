use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::dto::{ProcessorConfigResponse, ProcessorConfigUpdateRequest, ProcessorResponse};
use crate::error::ApiError;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/processors", get(list_processors))
        .route("/api/v1/processors/{name}", get(get_processor))
        .route(
            "/api/v1/processors/{name}/config",
            get(get_processor_config).put(update_processor_config),
        )
        .route(
            "/api/v1/processors/{name}/reset-circuit",
            post(reset_circuit),
        )
        .route("/api/v1/processors/{name}/stop", post(stop_processor))
        .route("/api/v1/processors/{name}/start", post(start_processor))
        .route("/api/v1/processors/{name}/pause", post(pause_processor))
        .route("/api/v1/processors/{name}/resume", post(resume_processor))
}

async fn list_processors(State(state): State<ApiState>) -> Json<Vec<ProcessorResponse>> {
    let processors: Vec<ProcessorResponse> = state
        .handle
        .processors
        .iter()
        .map(ProcessorResponse::from_info)
        .collect();
    Json(processors)
}

async fn get_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ProcessorResponse>, ApiError> {
    let info = state
        .handle
        .processors
        .iter()
        .find(|p| p.name == name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorResponse::from_info(info)))
}

async fn get_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ProcessorConfigResponse>, ApiError> {
    let info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorConfigResponse::from_info(info)))
}

async fn update_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<ProcessorConfigUpdateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify processor exists.
    let _ = state
        .handle
        .get_processor_info(&name)
        .ok_or_else(|| ApiError::ProcessorNotFound(name.clone()))?;

    if let Some(properties) = body.properties {
        state
            .handle
            .update_processor_properties(&name, properties)
            .map_err(ApiError::ConfigError)?;
    }

    Ok(Json(
        serde_json::json!({ "status": "updated", "processor": name }),
    ))
}

async fn reset_circuit(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.request_circuit_reset(&name) {
        Ok(Json(
            serde_json::json!({ "status": "reset_requested", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn stop_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.stop_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "stopped", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn start_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.start_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "started", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn pause_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.pause_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "paused", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}

async fn resume_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if state.handle.resume_processor(&name) {
        Ok(Json(
            serde_json::json!({ "status": "resumed", "processor": name }),
        ))
    } else {
        Err(ApiError::ProcessorNotFound(name))
    }
}
