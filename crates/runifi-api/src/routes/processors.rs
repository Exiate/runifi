use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Json, Router};

use runifi_core::engine::processor_node::SchedulingStrategy;

use crate::dto::{
    CreateProcessorRequest, ProcessorConfigResponse, ProcessorConfigUpdateRequest,
    ProcessorDetailResponse, ProcessorResponse, RelationshipResponse,
    UpdatePositionRequest,
};
use crate::error::ApiError;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/processors", get(list_processors).post(create_processor))
        .route("/api/v1/processors/{name}", get(get_processor).delete(delete_processor))
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
        .route("/api/v1/processors/{name}/position", put(update_position))
}

async fn list_processors(State(state): State<ApiState>) -> Json<Vec<ProcessorResponse>> {
    let processors: Vec<ProcessorResponse> = state
        .handle
        .processors
        .read()
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
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorResponse::from_info(&info)))
}

/// Create a new processor instance at runtime.
async fn create_processor(
    State(state): State<ApiState>,
    Json(body): Json<CreateProcessorRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .add_processor(
            body.name.clone(),
            body.type_name.clone(),
            body.properties.clone(),
            body.scheduling_strategy.clone(),
            body.interval_ms,
        )
        .await
        .map_err(ApiError::from)?;

    // Store canvas position if provided.
    if let Some(pos) = body.position {
        state.handle.set_position(&body.name, pos.x, pos.y);
    }

    // Build detailed response.
    let info = state
        .handle
        .get_processor_info(&body.name)
        .ok_or_else(|| ApiError::ProcessorNotFound(body.name.clone()))?;

    let snapshot = info.metrics.snapshot();
    let state_str = snapshot.state.as_str().to_string();

    let scheduling_str = match &info.scheduling {
        SchedulingStrategy::TimerDriven { interval_ms } => format!("timer ({}ms)", interval_ms),
        SchedulingStrategy::EventDriven => "event".to_string(),
    };

    let relationships: Vec<RelationshipResponse> = info
        .relationships
        .iter()
        .map(|r| RelationshipResponse {
            name: r.name.clone(),
            description: r.description.clone(),
            auto_terminated: r.auto_terminated,
        })
        .collect();

    let properties = info.properties.read().clone();

    let detail = ProcessorDetailResponse {
        name: info.name.clone(),
        type_name: info.type_name.clone(),
        state: state_str,
        scheduling: scheduling_str,
        position: body.position,
        relationships,
        properties,
    };

    Ok((StatusCode::CREATED, Json(detail)).into_response())
}

/// Remove a processor at runtime.
async fn delete_processor(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .handle
        .remove_processor(name.clone())
        .await
        .map_err(ApiError::from)?;

    // Remove stored position.
    state.handle.positions.remove(&name);

    Ok(StatusCode::NO_CONTENT)
}

/// Update the canvas position for a processor.
async fn update_position(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<UpdatePositionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate processor exists.
    if state.handle.get_processor_info(&name).is_none() {
        return Err(ApiError::ProcessorNotFound(name));
    }

    state.handle.set_position(&name, body.x, body.y);

    Ok(Json(serde_json::json!({
        "processor": name,
        "position": { "x": body.x, "y": body.y },
    })))
}

async fn get_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<ProcessorConfigResponse>, ApiError> {
    let info = state
        .handle
        .get_processor_info(&name)
        .ok_or(ApiError::ProcessorNotFound(name))?;

    Ok(Json(ProcessorConfigResponse::from_info(&info)))
}

async fn update_processor_config(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(body): Json<ProcessorConfigUpdateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let properties = body
        .properties
        .ok_or_else(|| ApiError::BadRequest("Missing 'properties' field in request body".into()))?;

    state
        .handle
        .update_processor_properties(&name, properties)?;

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
