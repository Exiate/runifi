use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::connection::back_pressure::BackPressureConfig;

/// A runtime mutation command sent to the engine via the command channel.
///
/// API handlers send a command and await the `reply` oneshot for the result.
/// This serialises all topology mutations through the engine loop, avoiding
/// complex cross-thread locking of task handles and processor maps.
pub enum MutationCommand {
    /// Add a new processor at runtime.
    AddProcessor {
        name: String,
        type_name: String,
        properties: HashMap<String, String>,
        scheduling_strategy: String,
        interval_ms: u64,
        /// CRON expression (only used when scheduling_strategy = "cron").
        cron_expression: Option<String>,
        reply: oneshot::Sender<Result<(), MutationError>>,
    },

    /// Remove a processor at runtime.
    RemoveProcessor {
        name: String,
        reply: oneshot::Sender<Result<(), MutationError>>,
    },

    /// Add a new connection between two processors at runtime.
    AddConnection {
        source_name: String,
        relationship: String,
        dest_name: String,
        config: BackPressureConfig,
        reply: oneshot::Sender<Result<String, MutationError>>,
    },

    /// Remove a connection at runtime.
    RemoveConnection {
        id: String,
        force: bool,
        reply: oneshot::Sender<Result<(), MutationError>>,
    },
}

/// Error returned by engine mutation commands.
#[derive(Debug, thiserror::Error)]
pub enum MutationError {
    #[error("unknown processor type: {0}")]
    UnknownType(String),

    #[error("processor already exists: {0}")]
    DuplicateName(String),

    #[error("processor not found: {0}")]
    ProcessorNotFound(String),

    #[error("connection not found: {0}")]
    ConnectionNotFound(String),

    #[error("processor must be stopped before removal")]
    ProcessorNotStopped,

    #[error("processor has active connections: {0}")]
    ProcessorHasConnections(String),

    #[error("relationship '{0}' does not exist on processor '{1}'")]
    UnknownRelationship(String, String),

    #[error("duplicate connection: source={0} relationship={1} destination={2}")]
    DuplicateConnection(String, String, String),

    #[error("connection queue is not empty ({0} items); use force=true to discard")]
    QueueNotEmpty(usize),

    #[error("engine not running")]
    EngineNotRunning,

    #[error("invalid scheduling strategy '{0}'; must be 'timer', 'event', or 'cron'")]
    InvalidSchedulingStrategy(String),

    #[error("invalid CRON expression '{0}': {1}")]
    InvalidCronExpression(String, String),

    #[error("{0}")]
    Internal(String),
}
