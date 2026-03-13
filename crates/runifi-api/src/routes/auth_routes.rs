//! Authentication API routes: login, logout, get current user.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use runifi_core::auth::jwt::Claims;

use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/auth/me", get(get_current_user))
}

/// Request body for login.
#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

/// Response for a successful login.
#[derive(Serialize)]
struct LoginResponse {
    token: String,
    token_type: String,
    expires_in: i64,
    user: UserSummary,
}

/// Minimal user info returned in auth responses.
#[derive(Serialize)]
struct UserSummary {
    id: String,
    username: String,
    enabled: bool,
}

/// Response for GET /api/v1/auth/me.
#[derive(Serialize)]
struct CurrentUserResponse {
    id: String,
    username: String,
    enabled: bool,
    created_at: String,
}

/// POST /api/v1/auth/login
///
/// Authenticate with username and password, receive a JWT.
async fn login(State(state): State<ApiState>, Json(body): Json<LoginRequest>) -> impl IntoResponse {
    let jwt_config = match &state.jwt_config {
        Some(config) => config,
        None => {
            let body = serde_json::json!({ "error": "User authentication is not enabled" });
            return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
        }
    };

    // Authenticate against the user store.
    let user = match state
        .user_store
        .authenticate(&body.username, &body.password)
    {
        Ok(user) => user,
        Err(e) => {
            tracing::warn!(username = %body.username, error = %e, "Login failed");
            let body = serde_json::json!({ "error": "Invalid credentials" });
            return (StatusCode::UNAUTHORIZED, Json(body)).into_response();
        }
    };

    // Generate JWT.
    let token = match jwt_config.generate_token(user.id, &user.username) {
        Ok(token) => token,
        Err(e) => {
            tracing::error!(error = %e, "Failed to generate JWT");
            let body = serde_json::json!({ "error": "Internal server error" });
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
        }
    };

    tracing::info!(username = %user.username, "User logged in");

    let response = LoginResponse {
        token,
        token_type: "Bearer".to_string(),
        expires_in: state.auth_config.jwt_expiry_secs,
        user: UserSummary {
            id: user.id.to_string(),
            username: user.username,
            enabled: user.enabled,
        },
    };

    (
        StatusCode::OK,
        Json(serde_json::to_value(response).unwrap()),
    )
        .into_response()
}

/// POST /api/v1/auth/logout
///
/// Revoke the current JWT token. Requires a valid JWT in the Authorization header.
async fn logout(State(state): State<ApiState>, req: axum::extract::Request) -> impl IntoResponse {
    // Extract claims from request extensions (set by auth middleware).
    if let Some(claims) = req.extensions().get::<Claims>() {
        state.user_store.revoke_token(&claims.jti);
        tracing::info!(username = %claims.username, "User logged out");
        let body = serde_json::json!({ "status": "logged out" });
        (StatusCode::OK, Json(body)).into_response()
    } else {
        let body = serde_json::json!({ "error": "Not authenticated" });
        (StatusCode::UNAUTHORIZED, Json(body)).into_response()
    }
}

/// GET /api/v1/auth/me
///
/// Get the currently authenticated user's info.
async fn get_current_user(
    State(state): State<ApiState>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    if let Some(claims) = req.extensions().get::<Claims>() {
        // Look up the full user from the store.
        let user_id: uuid::Uuid = match claims.sub.parse() {
            Ok(id) => id,
            Err(_) => {
                let body = serde_json::json!({ "error": "Invalid user ID in token" });
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
            }
        };

        match state.user_store.get_user(user_id) {
            Some(user) => {
                let response = CurrentUserResponse {
                    id: user.id.to_string(),
                    username: user.username,
                    enabled: user.enabled,
                    created_at: user.created_at.to_rfc3339(),
                };
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
    } else {
        // No JWT claims — check if API-key auth is in use (no user identity).
        let body = serde_json::json!({ "error": "Not authenticated with user credentials" });
        (StatusCode::UNAUTHORIZED, Json(body)).into_response()
    }
}
