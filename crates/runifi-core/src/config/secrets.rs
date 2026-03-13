use std::collections::HashMap;

use thiserror::Error;

use super::sensitive::SensitiveString;

/// Errors returned by secrets providers.
#[derive(Debug, Error)]
pub enum SecretsError {
    #[error("secret not found: {0}")]
    NotFound(String),

    #[error("secrets provider error: {0}")]
    Provider(String),
}

/// Trait for pluggable secrets backends.
///
/// Implementations supply sensitive property values at startup time.
/// The engine resolves secrets once during configuration loading and
/// stores the results as `SensitiveString` values that are zeroized
/// on drop.
///
/// Built-in implementations:
/// - [`StaticSecretsProvider`]: holds pre-decrypted values in memory
/// - [`EnvSecretsProvider`]: reads values from environment variables
///
/// Future implementations may integrate with HashiCorp Vault, AWS
/// Secrets Manager, Azure Key Vault, etc.
pub trait SecretsProvider: Send + Sync {
    /// Retrieve a secret by key.
    fn get_secret(&self, key: &str) -> Result<SensitiveString, SecretsError>;
}

/// A secrets provider that holds pre-decrypted values in memory.
///
/// Used by the `ENC()` config syntax: encrypted values are decrypted at
/// startup and stored here for later retrieval by the engine.
pub struct StaticSecretsProvider {
    secrets: HashMap<String, SensitiveString>,
}

impl StaticSecretsProvider {
    /// Create a new provider with the given key-value pairs.
    pub fn new(secrets: HashMap<String, SensitiveString>) -> Self {
        Self { secrets }
    }

    /// Create an empty provider.
    pub fn empty() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }

    /// Insert a secret into the provider.
    pub fn insert(&mut self, key: String, value: SensitiveString) {
        self.secrets.insert(key, value);
    }
}

impl SecretsProvider for StaticSecretsProvider {
    fn get_secret(&self, key: &str) -> Result<SensitiveString, SecretsError> {
        self.secrets
            .get(key)
            .cloned()
            .ok_or_else(|| SecretsError::NotFound(key.to_string()))
    }
}

/// A secrets provider that reads values from environment variables.
///
/// The key is used directly as the environment variable name. This is
/// useful for container deployments where secrets are injected as env
/// vars by the orchestrator (Kubernetes, Docker Compose, etc.).
pub struct EnvSecretsProvider;

impl EnvSecretsProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EnvSecretsProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretsProvider for EnvSecretsProvider {
    fn get_secret(&self, key: &str) -> Result<SensitiveString, SecretsError> {
        std::env::var(key)
            .map(SensitiveString::new)
            .map_err(|_| SecretsError::NotFound(key.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_provider_returns_stored_secret() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "db.password".to_string(),
            SensitiveString::new("s3cret".to_string()),
        );
        let provider = StaticSecretsProvider::new(secrets);

        let secret = provider.get_secret("db.password").unwrap();
        assert_eq!(&*secret, "s3cret");
    }

    #[test]
    fn static_provider_not_found() {
        let provider = StaticSecretsProvider::empty();
        let result = provider.get_secret("missing");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SecretsError::NotFound(_)));
    }

    #[test]
    fn static_provider_insert() {
        let mut provider = StaticSecretsProvider::empty();
        provider.insert(
            "api.key".to_string(),
            SensitiveString::new("key-123".to_string()),
        );
        let secret = provider.get_secret("api.key").unwrap();
        assert_eq!(&*secret, "key-123");
    }

    #[test]
    fn env_provider_returns_env_var() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::set_var("RUNIFI_TEST_SECRET_42", "env-secret-value") };
        let provider = EnvSecretsProvider::new();
        let secret = provider.get_secret("RUNIFI_TEST_SECRET_42").unwrap();
        assert_eq!(&*secret, "env-secret-value");
        unsafe { std::env::remove_var("RUNIFI_TEST_SECRET_42") };
    }

    #[test]
    fn env_provider_not_found() {
        let provider = EnvSecretsProvider::new();
        let result = provider.get_secret("RUNIFI_NONEXISTENT_VAR_XYZ_99");
        assert!(result.is_err());
    }

    #[test]
    fn static_provider_debug_masks_values() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "password".to_string(),
            SensitiveString::new("super-secret".to_string()),
        );
        let provider = StaticSecretsProvider::new(secrets);
        let secret = provider.get_secret("password").unwrap();
        let debug = format!("{:?}", secret);
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("********"));
    }
}
