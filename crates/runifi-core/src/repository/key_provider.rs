use thiserror::Error;

/// Identifier for an encryption key.
pub type KeyId = String;

/// Error types for key provider operations.
#[derive(Debug, Error)]
pub enum KeyError {
    #[error("Key not found: {0}")]
    NotFound(String),
    #[error("Key provider error: {0}")]
    Provider(String),
}

/// Trait for pluggable encryption key providers.
///
/// Implementations supply encryption keys for content encryption at rest.
/// The provider must support key rotation: old keys remain accessible for
/// decrypting content encrypted under previous active keys.
pub trait KeyProvider: Send + Sync {
    /// Get the current active encryption key and its identifier.
    fn active_key(&self) -> Result<(KeyId, Vec<u8>), KeyError>;

    /// Get a specific key by identifier (for decryption of older content).
    fn get_key(&self, id: &KeyId) -> Result<Vec<u8>, KeyError>;
}
