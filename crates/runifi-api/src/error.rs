use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// API error type with automatic HTTP status mapping.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Processor not found: {0}")]
    ProcessorNotFound(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::ProcessorNotFound(_) => StatusCode::NOT_FOUND,
        };
        let body = serde_json::json!({ "error": self.to_string() });
        (status, axum::Json(body)).into_response()
    }
}
