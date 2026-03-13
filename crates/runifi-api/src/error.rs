use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use runifi_core::engine::handle::ConfigUpdateError;
use runifi_core::engine::mutation::MutationError;
use runifi_core::registry::service_registry::ServiceError;

/// API error type with automatic HTTP status mapping.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum ApiError {
    #[error("Processor not found: {0}")]
    ProcessorNotFound(String),

    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("Service not found: {0}")]
    ServiceNotFound(String),

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

    #[error("Too many requests")]
    TooManyRequests,

    #[error("Forbidden: insufficient permissions")]
    Forbidden,
}

impl ApiError {
    /// Return the HTTP status code for this error variant.
    pub fn status(&self) -> StatusCode {
        match self {
            ApiError::ProcessorNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::ConnectionNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::ServiceNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::FlowFileNotFound(_) => StatusCode::NOT_FOUND,
            ApiError::ContentNotAvailable(_) => StatusCode::NOT_FOUND,
            ApiError::ConfigError(_) => StatusCode::CONFLICT,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::EngineNotRunning => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Forbidden => StatusCode::FORBIDDEN,
        }
    }

    /// Return a sanitized error message that does not leak internal topology details
    /// (processor names, connection IDs, FlowFile IDs, etc.).
    pub fn sanitized_message(&self) -> &'static str {
        match self {
            ApiError::ProcessorNotFound(_) => "Resource not found",
            ApiError::ConnectionNotFound(_) => "Resource not found",
            ApiError::ServiceNotFound(_) => "Resource not found",
            ApiError::FlowFileNotFound(_) => "Resource not found",
            ApiError::ContentNotAvailable(_) => "Content not available",
            ApiError::ConfigError(_) => "Configuration conflict",
            ApiError::BadRequest(_) => "Bad request",
            ApiError::Conflict(_) => "Conflict",
            ApiError::EngineNotRunning => "Service unavailable",
            ApiError::TooManyRequests => "Too many requests",
            ApiError::Forbidden => "Forbidden",
        }
    }

    /// Convert to response, optionally including detailed error messages.
    #[allow(dead_code)]
    pub fn into_response_with_detail(self, detailed: bool) -> Response {
        let status = self.status();
        let message = if detailed {
            self.to_string()
        } else {
            self.sanitized_message().to_string()
        };
        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Default to sanitized messages. Routes that need detailed errors
        // should use `into_response_with_detail` via the state flag.
        let status = self.status();
        let body = serde_json::json!({ "error": self.sanitized_message() });
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
            MutationError::UnknownRelationship(rel, proc_name) => ApiError::BadRequest(format!(
                "Processor '{}' does not have a relationship named '{}'",
                proc_name, rel
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

impl From<ServiceError> for ApiError {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::NotFound(name) => ApiError::ServiceNotFound(name),
            ServiceError::DuplicateName(name) => {
                ApiError::Conflict(format!("A service named '{}' already exists", name))
            }
            ServiceError::UnknownType(name) => {
                ApiError::BadRequest(format!("Unknown service type: {}", name))
            }
            ServiceError::InvalidState {
                name,
                current_state,
                expected_state,
            } => ApiError::Conflict(format!(
                "Service '{}' is in state {}, expected {}",
                name, current_state, expected_state
            )),
            ServiceError::HasReferences { name, processors } => ApiError::Conflict(format!(
                "Service '{}' is referenced by processors: {}",
                name, processors
            )),
            ServiceError::ConfigFailed(msg) => ApiError::BadRequest(msg),
            ServiceError::ValidationFailed(msg) => ApiError::BadRequest(msg),
            ServiceError::EnableFailed(msg) => ApiError::ConfigError(msg),
        }
    }
}
