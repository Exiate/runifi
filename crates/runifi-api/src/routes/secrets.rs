use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router, middleware};
use serde::Deserialize;

use crate::error::ApiError;
use crate::rbac;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/secrets/rotate", post(rotate_key))
        .layer(middleware::from_fn(rbac::require_manage_config))
}

#[derive(Deserialize)]
struct RotateKeyRequest {
    /// Hex-encoded new 256-bit encryption key (64 hex characters).
    new_key_hex: String,
}

async fn rotate_key(
    State(state): State<ApiState>,
    Json(body): Json<RotateKeyRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let new_key = hex::decode(&body.new_key_hex)
        .map_err(|e| ApiError::BadRequest(format!("Invalid hex key: {}", e)))?;

    if new_key.len() != 32 {
        return Err(ApiError::BadRequest(
            "Encryption key must be exactly 32 bytes (64 hex characters)".into(),
        ));
    }

    state.handle.rotate_encryption_key(&new_key);

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "rotated",
            "message": "Encryption key rotated. Sensitive properties will be re-encrypted on next persist."
        })),
    ))
}
