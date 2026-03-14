use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete as delete_method, get, post, put};
use axum::{Json, Router, middleware};

use runifi_core::engine::handle::{LabelInfo, LabelUpdate, next_label_id};

use crate::dto::{CreateLabelRequest, LabelResponse, UpdateLabelRequest};
use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    // GET endpoints — ViewFlow (Viewer+)
    let view_routes = Router::new()
        .route("/api/v1/process-groups/root/labels", get(list_labels))
        .route("/api/v1/process-groups/root/labels/{id}", get(get_label))
        .layer(middleware::from_fn(rbac::require_view_flow));

    // Mutation endpoints — ModifyFlow (Admin only)
    let modify_routes = Router::new()
        .route("/api/v1/process-groups/root/labels", post(create_label))
        .route("/api/v1/process-groups/root/labels/{id}", put(update_label))
        .route(
            "/api/v1/process-groups/root/labels/{id}",
            delete_method(delete_label),
        )
        .layer(middleware::from_fn(rbac::require_modify_flow));

    view_routes.merge(modify_routes)
}

fn label_to_response(label: &LabelInfo) -> LabelResponse {
    LabelResponse {
        id: label.id.clone(),
        text: label.text.clone(),
        x: label.x,
        y: label.y,
        width: label.width,
        height: label.height,
        background_color: label.background_color.clone(),
        font_size: label.font_size,
    }
}

async fn list_labels(State(state): State<ApiState>) -> Json<Vec<LabelResponse>> {
    let labels = state.handle.list_labels();
    Json(labels.iter().map(label_to_response).collect())
}

async fn get_label(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<LabelResponse>, ApiError> {
    let label = state
        .handle
        .get_label(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Label not found: {}", id)))?;

    Ok(Json(label_to_response(&label)))
}

async fn create_label(
    State(state): State<ApiState>,
    Json(body): Json<CreateLabelRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let label = LabelInfo {
        id: next_label_id(),
        text: body.text,
        x: body.x,
        y: body.y,
        width: body.width,
        height: body.height,
        background_color: body.background_color,
        font_size: body.font_size,
    };

    let response = label_to_response(&label);

    state.handle.add_label(label).map_err(ApiError::Conflict)?;

    Ok((StatusCode::CREATED, Json(response)).into_response())
}

async fn update_label(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateLabelRequest>,
) -> Result<Json<LabelResponse>, ApiError> {
    let update = LabelUpdate {
        text: body.text,
        x: body.x,
        y: body.y,
        width: body.width,
        height: body.height,
        background_color: body.background_color,
        font_size: body.font_size,
    };

    if !state.handle.update_label(&id, update) {
        return Err(ApiError::BadRequest(format!("Label not found: {}", id)));
    }

    let label = state
        .handle
        .get_label(&id)
        .ok_or_else(|| ApiError::BadRequest(format!("Label not found: {}", id)))?;

    Ok(Json(label_to_response(&label)))
}

async fn delete_label(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.handle.remove_label(&id) {
        return Err(ApiError::BadRequest(format!("Label not found: {}", id)));
    }

    Ok(StatusCode::NO_CONTENT)
}
