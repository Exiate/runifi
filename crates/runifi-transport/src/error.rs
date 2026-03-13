//! Transport-layer error types.

use thiserror::Error;

/// Errors that can occur during QUIC transport operations.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("QUIC connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("QUIC connect error: {0}")]
    Connect(#[from] quinn::ConnectError),

    #[error("QUIC write error: {0}")]
    WriteError(#[from] quinn::WriteError),

    #[error("QUIC read error: {0}")]
    ReadError(#[from] quinn::ReadExactError),

    #[error("QUIC read-to-end error: {0}")]
    ReadToEndError(#[from] quinn::ReadToEndError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("integrity check failed: expected {expected}, got {actual}")]
    IntegrityMismatch { expected: String, actual: String },

    #[error("handshake failed: {0}")]
    Handshake(String),

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u16),

    #[error("server is shutting down")]
    Shutdown,

    #[error("connection pool exhausted")]
    PoolExhausted,

    #[error("timeout")]
    Timeout,
}

/// Result type for transport operations.
pub type TransportResult<T = ()> = std::result::Result<T, TransportError>;
