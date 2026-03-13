//! User CRUD API routes.
//!
//! Endpoints:
//!   - `GET    /api/v1/tenants/users`       — list all users
//!   - `POST   /api/v1/tenants/users`       — create a user
//!   - `GET    /api/v1/tenants/users/{id}`   — get a user by ID
//!   - `PUT    /api/v1/tenants/users/{id}`   — update a user
//!   - `DELETE /api/v1/tenants/users/{id}`   — delete a user

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/tenants/users", get(list_users).post(create_user))
        .route(
            "/api/v1/tenants/users/{id}",
            get(get_user).put(update_user).delete(delete_user),
        )
        .layer(middleware::from_fn(rbac::require_manage_config))
}

/// Response for a user (password hash is never included).
#[derive(Serialize)]
struct UserResponse {
    id: String,
    username: String,
    enabled: bool,
    created_at: String,
    updated_at: String,
}

impl UserResponse {
    fn from_model(user: &runifi_core::auth::models::User) -> Self {
        Self {
            id: user.id.to_string(),
            username: user.username.clone(),
            enabled: user.enabled,
            created_at: user.created_at.to_rfc3339(),
            updated_at: user.updated_at.to_rfc3339(),
        }
    }
}

/// Request body for creating a user.
#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
}

/// Request body for updating a user.
#[derive(Deserialize)]
struct UpdateUserRequest {
    username: Option<String>,
    enabled: Option<bool>,
    password: Option<String>,
}

/// GET /api/v1/tenants/users
async fn list_users(State(state): State<ApiState>) -> impl IntoResponse {
    let users: Vec<UserResponse> = state
        .user_store
        .list_users()
        .iter()
        .map(UserResponse::from_model)
        .collect();

    (StatusCode::OK, Json(serde_json::to_value(users).unwrap())).into_response()
}

/// POST /api/v1/tenants/users
async fn create_user(
    State(state): State<ApiState>,
    Json(body): Json<CreateUserRequest>,
) -> impl IntoResponse {
    if body.username.is_empty() {
        let body = serde_json::json!({ "error": "Username cannot be empty" });
        return (StatusCode::BAD_REQUEST, Json(body)).into_response();
    }
    if body.password.is_empty() {
        let body = serde_json::json!({ "error": "Password cannot be empty" });
        return (StatusCode::BAD_REQUEST, Json(body)).into_response();
    }

    match state.user_store.create_user(body.username, &body.password) {
        Ok(user) => {
            let response = UserResponse::from_model(&user);
            (
                StatusCode::CREATED,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::DuplicateUsername(_) => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            (status, Json(body)).into_response()
        }
    }
}

/// GET /api/v1/tenants/users/{id}
async fn get_user(State(state): State<ApiState>, Path(id): Path<String>) -> impl IntoResponse {
    let user_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid user ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    match state.user_store.get_user(user_id) {
        Some(user) => {
            let response = UserResponse::from_model(&user);
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        None => {
            let body = serde_json::json!({ "error": "User not found" });
            (StatusCode::NOT_FOUND, Json(body)).into_response()
        }
    }
}

/// PUT /api/v1/tenants/users/{id}
async fn update_user(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateUserRequest>,
) -> impl IntoResponse {
    let user_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid user ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    // Update basic fields.
    match state
        .user_store
        .update_user(user_id, body.username, body.enabled)
    {
        Ok(_) => {}
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::UserNotFound(_) => StatusCode::NOT_FOUND,
                runifi_core::auth::store::UserStoreError::DuplicateUsername(_) => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            return (status, Json(body)).into_response();
        }
    }

    // Update password if provided.
    if let Some(ref password) = body.password
        && let Err(e) = state.user_store.change_password(user_id, password)
    {
        let body = serde_json::json!({ "error": e.to_string() });
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
    }

    // Return the updated user.
    match state.user_store.get_user(user_id) {
        Some(user) => {
            let response = UserResponse::from_model(&user);
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        None => {
            let body = serde_json::json!({ "error": "User not found" });
            (StatusCode::NOT_FOUND, Json(body)).into_response()
        }
    }
}

/// DELETE /api/v1/tenants/users/{id}
async fn delete_user(State(state): State<ApiState>, Path(id): Path<String>) -> impl IntoResponse {
    let user_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid user ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    match state.user_store.delete_user(user_id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::UserNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            (status, Json(body)).into_response()
        }
    }
}
