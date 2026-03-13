//! User Group CRUD API routes.
//!
//! Endpoints:
//!   - `GET    /api/v1/tenants/user-groups`       — list all groups
//!   - `POST   /api/v1/tenants/user-groups`       — create a group
//!   - `GET    /api/v1/tenants/user-groups/{id}`   — get a group by ID
//!   - `PUT    /api/v1/tenants/user-groups/{id}`   — update a group
//!   - `DELETE /api/v1/tenants/user-groups/{id}`   — delete a group

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
        .route(
            "/api/v1/tenants/user-groups",
            get(list_groups).post(create_group),
        )
        .route(
            "/api/v1/tenants/user-groups/{id}",
            get(get_group).put(update_group).delete(delete_group),
        )
        .layer(middleware::from_fn(rbac::require_manage_config))
}

/// Response for a user group.
#[derive(Serialize)]
struct UserGroupResponse {
    id: String,
    name: String,
    members: Vec<String>,
    created_at: String,
    updated_at: String,
}

impl UserGroupResponse {
    fn from_model(group: &runifi_core::auth::models::UserGroup) -> Self {
        Self {
            id: group.id.to_string(),
            name: group.name.clone(),
            members: group.members.iter().map(|id| id.to_string()).collect(),
            created_at: group.created_at.to_rfc3339(),
            updated_at: group.updated_at.to_rfc3339(),
        }
    }
}

/// Request body for creating a group.
#[derive(Deserialize)]
struct CreateGroupRequest {
    name: String,
    #[serde(default)]
    members: Vec<String>,
}

/// Request body for updating a group.
#[derive(Deserialize)]
struct UpdateGroupRequest {
    name: Option<String>,
    members: Option<Vec<String>>,
}

/// GET /api/v1/tenants/user-groups
async fn list_groups(State(state): State<ApiState>) -> impl IntoResponse {
    let groups: Vec<UserGroupResponse> = state
        .user_store
        .list_groups()
        .iter()
        .map(UserGroupResponse::from_model)
        .collect();

    (StatusCode::OK, Json(serde_json::to_value(groups).unwrap())).into_response()
}

/// POST /api/v1/tenants/user-groups
async fn create_group(
    State(state): State<ApiState>,
    Json(body): Json<CreateGroupRequest>,
) -> impl IntoResponse {
    if body.name.is_empty() {
        let body = serde_json::json!({ "error": "Group name cannot be empty" });
        return (StatusCode::BAD_REQUEST, Json(body)).into_response();
    }

    let group = match state.user_store.create_group(body.name) {
        Ok(group) => group,
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::DuplicateGroupName(_) => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            return (status, Json(body)).into_response();
        }
    };

    // If members were provided, set them.
    if !body.members.is_empty() {
        let member_ids: Vec<Uuid> = match body
            .members
            .iter()
            .map(|s| s.parse::<Uuid>())
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ids) => ids,
            Err(_) => {
                let body = serde_json::json!({ "error": "Invalid member user ID" });
                return (StatusCode::BAD_REQUEST, Json(body)).into_response();
            }
        };

        if let Err(e) = state
            .user_store
            .update_group(group.id, None, Some(member_ids))
        {
            let body = serde_json::json!({ "error": e.to_string() });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    }

    // Fetch the final state.
    let group = state.user_store.get_group(group.id).unwrap();
    let response = UserGroupResponse::from_model(&group);
    (
        StatusCode::CREATED,
        Json(serde_json::to_value(response).unwrap()),
    )
        .into_response()
}

/// GET /api/v1/tenants/user-groups/{id}
async fn get_group(State(state): State<ApiState>, Path(id): Path<String>) -> impl IntoResponse {
    let group_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid group ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    match state.user_store.get_group(group_id) {
        Some(group) => {
            let response = UserGroupResponse::from_model(&group);
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        None => {
            let body = serde_json::json!({ "error": "Group not found" });
            (StatusCode::NOT_FOUND, Json(body)).into_response()
        }
    }
}

/// PUT /api/v1/tenants/user-groups/{id}
async fn update_group(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateGroupRequest>,
) -> impl IntoResponse {
    let group_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid group ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    let member_ids = match body.members {
        Some(ref members) => {
            let ids: Result<Vec<Uuid>, _> = members.iter().map(|s| s.parse::<Uuid>()).collect();
            match ids {
                Ok(ids) => Some(ids),
                Err(_) => {
                    let body = serde_json::json!({ "error": "Invalid member user ID" });
                    return (StatusCode::BAD_REQUEST, Json(body)).into_response();
                }
            }
        }
        None => None,
    };

    match state
        .user_store
        .update_group(group_id, body.name, member_ids)
    {
        Ok(group) => {
            let response = UserGroupResponse::from_model(&group);
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::GroupNotFound(_) => StatusCode::NOT_FOUND,
                runifi_core::auth::store::UserStoreError::DuplicateGroupName(_) => {
                    StatusCode::CONFLICT
                }
                runifi_core::auth::store::UserStoreError::UserNotFound(_) => {
                    StatusCode::BAD_REQUEST
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            (status, Json(body)).into_response()
        }
    }
}

/// DELETE /api/v1/tenants/user-groups/{id}
async fn delete_group(State(state): State<ApiState>, Path(id): Path<String>) -> impl IntoResponse {
    let group_id: Uuid = match id.parse() {
        Ok(id) => id,
        Err(_) => {
            let body = serde_json::json!({ "error": "Invalid group ID" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    match state.user_store.delete_group(group_id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let status = match &e {
                runifi_core::auth::store::UserStoreError::GroupNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let body = serde_json::json!({ "error": e.to_string() });
            (status, Json(body)).into_response()
        }
    }
}
