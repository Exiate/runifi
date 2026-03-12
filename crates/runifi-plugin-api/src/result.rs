use thiserror::Error;

/// Errors that plugins can return.
#[derive(Debug, Error)]
pub enum PluginError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("processing failed: {0}")]
    ProcessingFailed(String),

    #[error("content not found: resource_id={0}")]
    ContentNotFound(u64),

    #[error("required property not set: {0}")]
    PropertyRequired(&'static str),

    #[error("session closed")]
    SessionClosed,
}

/// Result type for plugin operations.
pub type ProcessResult<T = ()> = std::result::Result<T, PluginError>;
