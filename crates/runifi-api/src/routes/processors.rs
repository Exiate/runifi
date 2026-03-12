use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::dto::ProcessorResponse;
use crate::error::ApiError;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/processors", get(list_processors))
        .route("/api/v1/processors/{name}", get(get_processor))
        .route(
            "/api/v1/processors/{name}/reset-circuit",
            post(reset_circuit),
        )
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
