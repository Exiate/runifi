use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use runifi_core::engine::bulletin::BulletinSeverity;

use crate::dto::BulletinResponse;
use crate::error::ApiError;
use crate::state::ApiState;

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/bulletins", get(list_bulletins))
        .route(
            "/api/v1/processors/{name}/bulletins",
            get(list_processor_bulletins),
        )
}

#[derive(Deserialize)]
struct BulletinQuery {
    /// Filter by processor name.
    processor: Option<String>,
    /// Filter by severity: "warn" or "error".
    severity: Option<String>,
}

fn parse_severity(s: &str) -> Option<BulletinSeverity> {
    match s.to_lowercase().as_str() {
        "warn" | "warning" => Some(BulletinSeverity::Warn),
        "error" => Some(BulletinSeverity::Error),
        _ => None,
    }
}

async fn list_bulletins(
    State(state): State<ApiState>,
    Query(query): Query<BulletinQuery>,
) -> Result<Json<Vec<BulletinResponse>>, ApiError> {
    let severity = match query.severity.as_deref() {
        Some(s) => Some(parse_severity(s).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Invalid severity filter '{}'. Valid values: warn, warning, error",
                s
            ))
        })?),
        None => None,
    };

    let bulletins = state
        .handle
        .bulletin_board
        .get_all(query.processor.as_deref(), severity);

    let response: Vec<BulletinResponse> =
        bulletins.into_iter().map(BulletinResponse::from).collect();
    Ok(Json(response))
}

async fn list_processor_bulletins(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<BulletinQuery>,
) -> Result<Json<Vec<BulletinResponse>>, ApiError> {
    // Verify the processor exists.
    let exists = state
        .handle
        .processors
        .read()
        .iter()
        .any(|p| p.name == name);
    if !exists {
        return Err(ApiError::ProcessorNotFound(name));
    }

    let severity = match query.severity.as_deref() {
        Some(s) => Some(parse_severity(s).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Invalid severity filter '{}'. Valid values: warn, warning, error",
                s
            ))
        })?),
        None => None,
    };

    let bulletins = state
        .handle
        .bulletin_board
        .get_for_processor(&name, severity);

    let response: Vec<BulletinResponse> =
        bulletins.into_iter().map(BulletinResponse::from).collect();
    Ok(Json(response))
}
