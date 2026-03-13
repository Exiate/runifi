use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use runifi_core::engine::handle::ConfigUpdateError;
use runifi_core::engine::mutation::MutationError;

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

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Engine not running")]
    EngineNotRunning,
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
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::EngineNotRunning => StatusCode::SERVICE_UNAVAILABLE,
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

impl From<MutationError> for ApiError {
    fn from(err: MutationError) -> Self {
        match err {
            MutationError::UnknownType(msg) => ApiError::BadRequest(format!(
                "Unknown processor type: {}. Check /api/v1/plugins for available types.",
                msg
            )),
            MutationError::DuplicateName(name) => {
                ApiError::Conflict(format!("A processor named '{}' already exists", name))
            }
            MutationError::ProcessorNotFound(name) => ApiError::ProcessorNotFound(name),
            MutationError::ConnectionNotFound(id) => ApiError::ConnectionNotFound(id),
            MutationError::ProcessorNotStopped => {
                ApiError::Conflict("Processor must be stopped before it can be removed".to_string())
            }
            MutationError::ProcessorHasConnections(ids) => ApiError::Conflict(format!(
                "Processor has active connections: {}. Remove them first.",
                ids
            )),
            MutationError::UnknownRelationship(rel, proc) => ApiError::BadRequest(format!(
                "Processor '{}' does not have a relationship named '{}'",
                proc, rel
            )),
            MutationError::DuplicateConnection(src, rel, dst) => ApiError::Conflict(format!(
                "Connection from '{}' via '{}' to '{}' already exists",
                src, rel, dst
            )),
            MutationError::QueueNotEmpty(count) => ApiError::Conflict(format!(
                "Connection queue has {} FlowFile(s). Use ?force=true to discard them.",
                count
            )),
            MutationError::EngineNotRunning => ApiError::EngineNotRunning,
            MutationError::InvalidSchedulingStrategy(s) => ApiError::BadRequest(format!(
                "Invalid scheduling strategy '{}'; must be 'timer' or 'event'",
                s
            )),
            MutationError::Internal(msg) => ApiError::ConfigError(msg),
        }
    }
}
