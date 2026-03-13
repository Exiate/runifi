//! Flow versioning API endpoints.
//!
//! Provides version control for flow configurations:
//! - List version history
//! - Save current flow as a new version
//! - Get a specific version
//! - Diff current state vs a version
//! - Revert to a previous version

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, middleware};

use crate::dto::{
    FlowDiffEntryResponse, FlowDiffResponse, FlowVersionListResponse, FlowVersionResponse,
    RevertResponse, SaveVersionRequest,
};
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/v1/flow/versions",
            get(list_versions).layer(middleware::from_fn(rbac::require_view_flow)),
        )
        .route(
            "/api/v1/flow/versions",
            post(save_version).layer(middleware::from_fn(rbac::require_manage_config)),
        )
        .route(
            "/api/v1/flow/versions/{id}",
            get(get_version).layer(middleware::from_fn(rbac::require_view_flow)),
        )
        .route(
            "/api/v1/flow/versions/{id}/diff",
            get(get_version_diff).layer(middleware::from_fn(rbac::require_view_flow)),
        )
        .route(
            "/api/v1/flow/versions/{id}/revert",
            post(revert_to_version).layer(middleware::from_fn(rbac::require_manage_config)),
        )
}

/// `GET /api/v1/flow/versions` — List all saved versions, most recent first.
async fn list_versions(State(state): State<ApiState>) -> impl IntoResponse {
    let Some(ref store) = state.version_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Flow versioning is not configured"
            })),
        )
            .into_response();
    };

    match store.list_versions() {
        Ok(versions) => {
            let total = versions.len();
            let response = FlowVersionListResponse {
                versions: versions
                    .into_iter()
                    .map(|v| FlowVersionResponse {
                        id: v.id,
                        full_id: v.full_id,
                        comment: v.comment,
                        timestamp: v.timestamp,
                        processor_count: v.processor_count,
                        connection_count: v.connection_count,
                    })
                    .collect(),
                total,
            };
            Json(serde_json::to_value(response).unwrap()).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to list flow versions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to list versions: {}", e) })),
            )
                .into_response()
        }
    }
}

/// `POST /api/v1/flow/versions` — Save the current flow state as a new version.
async fn save_version(
    State(state): State<ApiState>,
    Json(body): Json<SaveVersionRequest>,
) -> impl IntoResponse {
    let Some(ref store) = state.version_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Flow versioning is not configured"
            })),
        )
            .into_response();
    };

    // Capture a snapshot of the current flow state.
    let flow_state = state.handle.snapshot_flow_state();

    match store.save_version(&flow_state, &body.comment) {
        Ok(version) => {
            let response = FlowVersionResponse {
                id: version.id,
                full_id: version.full_id,
                comment: version.comment,
                timestamp: version.timestamp,
                processor_count: version.processor_count,
                connection_count: version.connection_count,
            };
            (
                StatusCode::CREATED,
                Json(serde_json::to_value(response).unwrap()),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to save flow version");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to save version: {}", e) })),
            )
                .into_response()
        }
    }
}

/// `GET /api/v1/flow/versions/{id}` — Get metadata for a specific version.
async fn get_version(
    State(state): State<ApiState>,
    Path(version_id): Path<String>,
) -> impl IntoResponse {
    let Some(ref store) = state.version_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Flow versioning is not configured"
            })),
        )
            .into_response();
    };

    match store.get_version(&version_id) {
        Ok(version) => {
            let response = FlowVersionResponse {
                id: version.id,
                full_id: version.full_id,
                comment: version.comment,
                timestamp: version.timestamp,
                processor_count: version.processor_count,
                connection_count: version.connection_count,
            };
            Json(serde_json::to_value(response).unwrap()).into_response()
        }
        Err(runifi_core::versioning::VersionError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Version not found: {}", version_id) })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, version_id = %version_id, "Failed to get version");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to get version: {}", e) })),
            )
                .into_response()
        }
    }
}

/// `GET /api/v1/flow/versions/{id}/diff` — Diff current flow state vs a version.
async fn get_version_diff(
    State(state): State<ApiState>,
    Path(version_id): Path<String>,
) -> impl IntoResponse {
    let Some(ref store) = state.version_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Flow versioning is not configured"
            })),
        )
            .into_response();
    };

    // Load the version's flow state.
    let version_state = match store.load_version(&version_id) {
        Ok(state) => state,
        Err(runifi_core::versioning::VersionError::NotFound(_)) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": format!("Version not found: {}", version_id) })),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to load version for diff");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load version: {}", e) })),
            )
                .into_response();
        }
    };

    // Capture current state.
    let current_state = state.handle.snapshot_flow_state();

    // Compute the diff.
    let diff = runifi_core::versioning::diff::compute_diff(&current_state, &version_state);

    let response = FlowDiffResponse {
        version_id,
        processors_added: diff.processors_added,
        processors_removed: diff.processors_removed,
        processors_changed: diff
            .processors_changed
            .into_iter()
            .map(|e| FlowDiffEntryResponse {
                name: e.name,
                changes: e.changes,
            })
            .collect(),
        connections_added: diff.connections_added,
        connections_removed: diff.connections_removed,
        services_added: diff.services_added,
        services_removed: diff.services_removed,
        services_changed: diff
            .services_changed
            .into_iter()
            .map(|e| FlowDiffEntryResponse {
                name: e.name,
                changes: e.changes,
            })
            .collect(),
    };

    Json(serde_json::to_value(response).unwrap()).into_response()
}

/// `POST /api/v1/flow/versions/{id}/revert` — Revert the flow to a previous version.
///
/// This loads the version's state and saves it as the latest version with a
/// revert comment. The actual engine state is not hot-reloaded; the persisted
/// state is updated and will take effect on restart. A future enhancement could
/// hot-reload the flow topology.
async fn revert_to_version(
    State(state): State<ApiState>,
    Path(version_id): Path<String>,
) -> impl IntoResponse {
    let Some(ref store) = state.version_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Flow versioning is not configured"
            })),
        )
            .into_response();
    };

    // Load the target version.
    let version_info = match store.get_version(&version_id) {
        Ok(v) => v,
        Err(runifi_core::versioning::VersionError::NotFound(_)) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": format!("Version not found: {}", version_id) })),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to find version for revert");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to find version: {}", e) })),
            )
                .into_response();
        }
    };

    let version_state = match store.load_version(&version_id) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "Failed to load version state for revert");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("Failed to load version: {}", e) })),
            )
                .into_response();
        }
    };

    // Save the reverted state as a new version.
    let revert_comment = format!(
        "Reverted to version {} (\"{}\")",
        version_info.id, version_info.comment
    );

    match store.save_version(&version_state, &revert_comment) {
        Ok(new_version) => {
            tracing::info!(
                reverted_to = %version_id,
                new_version = %new_version.id,
                "Flow reverted to previous version"
            );

            let response = RevertResponse {
                reverted_to: version_info.id,
                comment: version_info.comment,
                message: format!(
                    "Flow state reverted. New version {} created. \
                     Restart the engine to apply the reverted configuration.",
                    new_version.id
                ),
            };

            Json(serde_json::to_value(response).unwrap()).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to save reverted version");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    serde_json::json!({ "error": format!("Failed to save reverted version: {}", e) }),
                ),
            )
                .into_response()
        }
    }
}
