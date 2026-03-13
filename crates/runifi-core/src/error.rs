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

    #[error("content write failed: resource_id={resource_id}, {reason}")]
    ContentWriteFailed { resource_id: u64, reason: String },

    #[error(
        "content corrupted: resource_id={resource_id}, expected CRC={expected:#010x}, actual={actual:#010x}"
    )]
    ContentCorrupted {
        resource_id: u64,
        expected: u32,
        actual: u32,
    },

    #[error("segment error: {path}: {reason}")]
    SegmentError { path: String, reason: String },

    #[error("WAL error: {path}: {reason}")]
    WalError { path: String, reason: String },

    #[error("WAL corrupted at offset {offset}: {reason}")]
    WalCorrupted { offset: u64, reason: String },

    #[error("checkpoint error: {path}: {reason}")]
    CheckpointError { path: String, reason: String },
}

pub type Result<T> = std::result::Result<T, RuniFiError>;
