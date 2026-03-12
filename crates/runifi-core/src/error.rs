use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuniFiError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("processor '{name}' failed: {reason}")]
    ProcessorFailed { name: String, reason: String },

    #[error("processor '{name}' panicked: {message}")]
    ProcessorPanicked { name: String, message: String },

    #[error("processor '{name}' circuit breaker open after {failures} consecutive failures")]
    CircuitBreakerOpen { name: String, failures: u32 },

    #[error("connection queue full: {id}")]
    BackPressure { id: String },

    #[error("content not found: resource_id={0}")]
    ContentNotFound(u64),

    #[error("unknown processor type: {0}")]
    UnknownProcessorType(String),

    #[error("duplicate processor name: {0}")]
    DuplicateProcessorName(String),

    #[error(
        "dangling relationship: processor '{processor}' relationship '{relationship}' is not connected and not auto-terminated"
    )]
    DanglingRelationship {
        processor: String,
        relationship: String,
    },

    #[error("engine not started")]
    EngineNotStarted,

    #[error("engine already running")]
    EngineAlreadyRunning,

    #[error("transport error: {0}")]
    Transport(String),

    #[error("config error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, RuniFiError>;
