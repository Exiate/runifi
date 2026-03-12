use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use runifi_core::engine::handle::ConfigUpdateError;

/// API error type with automatic HTTP status mapping.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Processor not found: {0}")]
    ProcessorNotFound(String),

    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("FlowFile not found: {0}")]
    FlowFileNotFound(u64),

    #[error("Content not available for FlowFile {0}")]
    ContentNotAvailable(u64),

    #[error("{0}")]
    ConfigError(String),

    #[error("Bad request: {0}")]
    BadRequest(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::ProcessorNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::ConnectionNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::FlowFileNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::ContentNotAvailable(_) => StatusCode::NOT_FOUND,
            ApiError::ConfigError(_) => StatusCode::CONFLICT,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        };
        let body = serde_json::json!({ "error": self.to_string() });
        (status, axum::Json(body)).into_response()
    }
}

impl From<ConfigUpdateError> for ApiError {
    fn from(err: ConfigUpdateError) -> Self {
        match err {
            ConfigUpdateError::NotFound(msg) => ApiError::ProcessorNotFound(msg),
            ConfigUpdateError::StateConflict(msg) => ApiError::ConfigError(msg),
            ConfigUpdateError::ValidationError(msg) => ApiError::BadRequest(msg),
        }
    }
}
