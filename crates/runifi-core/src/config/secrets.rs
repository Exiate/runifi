use std::collections::HashMap;
use std::io::BufRead;
use std::path::Path;

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

    /// List all available secret keys.
    ///
    /// Returns an empty list if the provider does not support enumeration
    /// (e.g. environment-based providers where the key set is unbounded).
    fn list_keys(&self) -> Vec<String> {
        Vec::new()
    }
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

    fn list_keys(&self) -> Vec<String> {
        self.secrets.keys().cloned().collect()
    }
}

/// A secrets provider that reads values from environment variables.
///
/// Keys are prefixed with a configurable prefix (default: `RUNIFI_SECRET_`).
/// For example, `get_secret("DB_PASSWORD")` with prefix `RUNIFI_SECRET_`
/// reads the `RUNIFI_SECRET_DB_PASSWORD` environment variable.
pub struct EnvSecretsProvider {
    prefix: String,
}

impl EnvSecretsProvider {
    pub fn new() -> Self {
        Self {
            prefix: String::new(),
        }
    }

    /// Create with a custom prefix for environment variable names.
    pub fn with_prefix(prefix: String) -> Self {
        Self { prefix }
    }
}

impl Default for EnvSecretsProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretsProvider for EnvSecretsProvider {
    fn get_secret(&self, key: &str) -> Result<SensitiveString, SecretsError> {
        let var_name = format!("{}{}", self.prefix, key);
        std::env::var(&var_name)
            .map(SensitiveString::new)
            .map_err(|_| SecretsError::NotFound(var_name))
    }
}

/// A secrets provider that reads key-value pairs from a file.
///
/// The file format is one `KEY=VALUE` pair per line. Lines starting with
/// `#` are comments. Empty lines are ignored. Values may contain `=`.
pub struct FileSecretsProvider {
    secrets: HashMap<String, SensitiveString>,
}

impl FileSecretsProvider {
    /// Load secrets from the given file path.
    pub fn from_path(path: &Path) -> Result<Self, SecretsError> {
        let file = std::fs::File::open(path).map_err(|e| {
            SecretsError::Provider(format!(
                "failed to open secrets file '{}': {}",
                path.display(),
                e
            ))
        })?;
        let reader = std::io::BufReader::new(file);
        let mut secrets = HashMap::new();
        for line in reader.lines() {
            let line = line.map_err(|e| {
                SecretsError::Provider(format!("failed to read secrets file: {}", e))
            })?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('=') {
                secrets.insert(
                    key.trim().to_string(),
                    SensitiveString::new(value.trim().to_string()),
                );
            }
        }
        Ok(Self { secrets })
    }
}

impl SecretsProvider for FileSecretsProvider {
    fn get_secret(&self, key: &str) -> Result<SensitiveString, SecretsError> {
        self.secrets
            .get(key)
            .cloned()
            .ok_or_else(|| SecretsError::NotFound(key.to_string()))
    }

    fn list_keys(&self) -> Vec<String> {
        self.secrets.keys().cloned().collect()
    }
}

/// Resolve `${secret:KEY}` references in a property value string using the
/// given secrets provider.
///
/// This is a config-time resolution mechanism, distinct from the FlowFile
/// expression language (`${attribute}`). Secret references are resolved once
/// at configuration loading and the plaintext values are stored in memory
/// (never persisted).
///
/// Returns the original value unchanged if no `${secret:...}` patterns are
/// found, or an error if a referenced secret cannot be retrieved.
pub fn resolve_secret_refs(
    value: &str,
    provider: &dyn SecretsProvider,
) -> Result<String, SecretsError> {
    const PREFIX: &str = "${secret:";
    if !value.contains(PREFIX) {
        return Ok(value.to_string());
    }

    let mut result = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(start) = rest.find(PREFIX) {
        result.push_str(&rest[..start]);
        let after_prefix = &rest[start + PREFIX.len()..];
        if let Some(end) = after_prefix.find('}') {
            let key = &after_prefix[..end];
            let secret = provider.get_secret(key)?;
            result.push_str(&secret);
            rest = &after_prefix[end + 1..];
        } else {
            // Malformed — no closing brace, emit as-is.
            result.push_str(PREFIX);
            rest = after_prefix;
        }
    }
    result.push_str(rest);
    Ok(result)
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
    fn env_provider_with_prefix() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::set_var("RUNIFI_SECRET_DB_PASS", "prefixed-val") };
        let provider = EnvSecretsProvider::with_prefix("RUNIFI_SECRET_".to_string());
        let secret = provider.get_secret("DB_PASS").unwrap();
        assert_eq!(&*secret, "prefixed-val");
        unsafe { std::env::remove_var("RUNIFI_SECRET_DB_PASS") };
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

    #[test]
    fn file_provider_reads_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.env");
        std::fs::write(
            &path,
            "# comment line\nDB_PASSWORD=s3cret\nAPI_KEY = tok-123\n\nEMPTY=\n",
        )
        .unwrap();

        let provider = FileSecretsProvider::from_path(&path).unwrap();
        assert_eq!(&*provider.get_secret("DB_PASSWORD").unwrap(), "s3cret");
        assert_eq!(&*provider.get_secret("API_KEY").unwrap(), "tok-123");
        assert_eq!(&*provider.get_secret("EMPTY").unwrap(), "");
        assert!(provider.get_secret("MISSING").is_err());
    }

    #[test]
    fn file_provider_nonexistent_path() {
        let result = FileSecretsProvider::from_path(std::path::Path::new("/no/such/file"));
        assert!(result.is_err());
    }

    #[test]
    fn static_provider_list_keys() {
        let mut provider = StaticSecretsProvider::empty();
        assert!(provider.list_keys().is_empty());

        provider.insert("a".to_string(), SensitiveString::new("1".to_string()));
        provider.insert("b".to_string(), SensitiveString::new("2".to_string()));

        let mut keys = provider.list_keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn env_provider_list_keys_empty() {
        // EnvSecretsProvider cannot enumerate keys — returns empty.
        let provider = EnvSecretsProvider::new();
        assert!(provider.list_keys().is_empty());
    }

    #[test]
    fn file_provider_list_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.env");
        std::fs::write(&path, "KEY_A=val1\nKEY_B=val2\n").unwrap();

        let provider = FileSecretsProvider::from_path(&path).unwrap();
        let mut keys = provider.list_keys();
        keys.sort();
        assert_eq!(keys, vec!["KEY_A", "KEY_B"]);
    }

    #[test]
    fn resolve_secret_refs_basic() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "db_pass".to_string(),
            SensitiveString::new("s3cret".to_string()),
        );
        let provider = StaticSecretsProvider::new(secrets);

        let resolved =
            resolve_secret_refs("jdbc:pg://host?password=${secret:db_pass}", &provider).unwrap();
        assert_eq!(resolved, "jdbc:pg://host?password=s3cret");
    }

    #[test]
    fn resolve_secret_refs_multiple() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "user".to_string(),
            SensitiveString::new("admin".to_string()),
        );
        secrets.insert(
            "pass".to_string(),
            SensitiveString::new("pw123".to_string()),
        );
        let provider = StaticSecretsProvider::new(secrets);

        let resolved = resolve_secret_refs("${secret:user}:${secret:pass}", &provider).unwrap();
        assert_eq!(resolved, "admin:pw123");
    }

    #[test]
    fn resolve_secret_refs_no_pattern() {
        let provider = StaticSecretsProvider::empty();
        let resolved = resolve_secret_refs("plain-value", &provider).unwrap();
        assert_eq!(resolved, "plain-value");
    }

    #[test]
    fn resolve_secret_refs_missing_secret() {
        let provider = StaticSecretsProvider::empty();
        let result = resolve_secret_refs("${secret:nonexistent}", &provider);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_secret_refs_malformed_no_close_brace() {
        let provider = StaticSecretsProvider::empty();
        let resolved = resolve_secret_refs("${secret:unclosed", &provider).unwrap();
        assert_eq!(resolved, "${secret:unclosed");
    }
}
