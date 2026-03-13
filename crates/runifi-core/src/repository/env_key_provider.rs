//! Environment variable-based key provider for encryption keys.
//!
//! Reads encryption keys from environment variables. Useful for container
//! deployments where secrets are injected via env vars (e.g., Kubernetes
//! secrets, Docker secrets, cloud secret managers).
//!
//! ## Configuration
//!
//! ```toml
//! [engine.encryption.key_provider]
//! type = "env"
//! active_key_id = "key-2024-01"
//! key_env_prefix = "RUNIFI_ENC_KEY_"
//! ```
//!
//! With this configuration, the provider will look for environment variables
//! named `RUNIFI_ENC_KEY_key-2024-01` (with non-alphanumeric chars replaced
//! by underscores). Each variable should contain a hex-encoded 256-bit key.

use std::collections::HashMap;

use zeroize::Zeroize;

use super::key_provider::{KeyError, KeyId, KeyProvider};

/// A key provider that reads encryption keys from environment variables.
///
/// Each key is stored in an env var named `{prefix}{key_id}`, where
/// non-alphanumeric characters in the key ID are replaced with underscores
/// to form valid env var names.
#[derive(Debug)]
pub struct EnvKeyProvider {
    keys: HashMap<KeyId, Vec<u8>>,
    active_key_id: KeyId,
}

impl EnvKeyProvider {
    /// Create a new environment variable key provider.
    ///
    /// `key_ids` is the list of key IDs to look up. For each key ID,
    /// the provider reads the hex-encoded key from the env var
    /// `{prefix}{sanitized_key_id}`.
    ///
    /// `active_key_id` must be one of the provided key IDs.
    pub fn new(active_key_id: KeyId, key_ids: &[KeyId], prefix: &str) -> Result<Self, KeyError> {
        if key_ids.is_empty() {
            return Err(KeyError::Provider(
                "At least one key ID must be specified".to_string(),
            ));
        }

        let mut keys = HashMap::new();
        for key_id in key_ids {
            let env_name = format!("{}{}", prefix, sanitize_for_env(key_id));
            let hex_value = std::env::var(&env_name).map_err(|_| {
                KeyError::Provider(format!(
                    "Environment variable '{}' not set (for key '{}')",
                    env_name, key_id
                ))
            })?;

            let key_bytes = hex::decode(hex_value.trim()).map_err(|e| {
                KeyError::Provider(format!(
                    "Key '{}' from env var '{}' has invalid hex encoding: {}",
                    key_id, env_name, e
                ))
            })?;

            if key_bytes.len() != 32 {
                return Err(KeyError::Provider(format!(
                    "Key '{}' must be 32 bytes (256 bits), got {} bytes",
                    key_id,
                    key_bytes.len()
                )));
            }

            keys.insert(key_id.clone(), key_bytes);
        }

        if !keys.contains_key(&active_key_id) {
            return Err(KeyError::NotFound(format!(
                "Active key '{}' not found in provided key IDs",
                active_key_id
            )));
        }

        Ok(Self {
            keys,
            active_key_id,
        })
    }
}

/// Replace non-alphanumeric characters with underscores for env var names.
fn sanitize_for_env(key_id: &str) -> String {
    key_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl Drop for EnvKeyProvider {
    fn drop(&mut self) {
        for (_, key) in self.keys.iter_mut() {
            key.zeroize();
        }
    }
}

impl KeyProvider for EnvKeyProvider {
    fn active_key(&self) -> Result<(KeyId, Vec<u8>), KeyError> {
        let key = self
            .keys
            .get(&self.active_key_id)
            .ok_or_else(|| KeyError::NotFound(self.active_key_id.clone()))?;
        Ok((self.active_key_id.clone(), key.clone()))
    }

    fn get_key(&self, id: &KeyId) -> Result<Vec<u8>, KeyError> {
        self.keys
            .get(id)
            .cloned()
            .ok_or_else(|| KeyError::NotFound(id.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_key_aa() -> String {
        "aa".repeat(32)
    }

    fn hex_key_bb() -> String {
        "bb".repeat(32)
    }

    #[test]
    fn sanitize_replaces_dashes() {
        assert_eq!(sanitize_for_env("key-2024-01"), "key_2024_01");
    }

    #[test]
    fn sanitize_preserves_alphanumeric() {
        assert_eq!(sanitize_for_env("key123"), "key123");
    }

    #[test]
    fn sanitize_replaces_dots() {
        assert_eq!(sanitize_for_env("key.v1.2"), "key_v1_2");
    }

    /// SAFETY: These tests use `set_var`/`remove_var` which are unsafe in Rust 2024
    /// because they can cause UB if another thread reads the environment concurrently.
    /// Tests use unique prefixes to avoid conflicts.

    #[test]
    fn load_single_key_from_env() {
        let prefix = "TEST_RUNIFI_ENC_SINGLE_";
        let key_id = "key1".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}{}", prefix, "key1"), hex_key_aa());
        }

        let provider = EnvKeyProvider::new(key_id.clone(), &[key_id.clone()], prefix).unwrap();

        let (id, key) = provider.active_key().unwrap();
        assert_eq!(id, "key1");
        assert_eq!(key, vec![0xAA; 32]);

        unsafe {
            std::env::remove_var(format!("{}{}", prefix, "key1"));
        }
    }

    #[test]
    fn load_multiple_keys_from_env() {
        let prefix = "TEST_RUNIFI_ENC_MULTI_";
        let key1 = "key-a".to_string();
        let key2 = "key-b".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}key_a", prefix), hex_key_aa());
            std::env::set_var(format!("{}key_b", prefix), hex_key_bb());
        }

        let provider =
            EnvKeyProvider::new(key2.clone(), &[key1.clone(), key2.clone()], prefix).unwrap();

        let (id, _) = provider.active_key().unwrap();
        assert_eq!(id, "key-b");

        let old_key = provider.get_key(&key1).unwrap();
        assert_eq!(old_key, vec![0xAA; 32]);

        unsafe {
            std::env::remove_var(format!("{}key_a", prefix));
            std::env::remove_var(format!("{}key_b", prefix));
        }
    }

    #[test]
    fn missing_env_var_fails() {
        let prefix = "TEST_RUNIFI_ENC_MISSING_";
        let key_id = "missing-key".to_string();
        // Do not set the env var.
        let result = EnvKeyProvider::new(key_id.clone(), &[key_id], prefix);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("not set"));
    }

    #[test]
    fn invalid_hex_in_env_fails() {
        let prefix = "TEST_RUNIFI_ENC_BADHEX_";
        let key_id = "badkey".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}{}", prefix, "badkey"), "not-valid-hex");
        }

        let result = EnvKeyProvider::new(key_id.clone(), &[key_id], prefix);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("invalid hex"));

        unsafe {
            std::env::remove_var(format!("{}{}", prefix, "badkey"));
        }
    }

    #[test]
    fn wrong_key_size_from_env_fails() {
        let prefix = "TEST_RUNIFI_ENC_SHORT_";
        let key_id = "shortkey".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}{}", prefix, "shortkey"), "aa".repeat(16));
        }

        let result = EnvKeyProvider::new(key_id.clone(), &[key_id], prefix);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("32 bytes"));

        unsafe {
            std::env::remove_var(format!("{}{}", prefix, "shortkey"));
        }
    }

    #[test]
    fn empty_key_ids_fails() {
        let result = EnvKeyProvider::new("key-1".to_string(), &[], "PREFIX_");
        assert!(result.is_err());
    }

    #[test]
    fn active_key_not_in_list_fails() {
        let prefix = "TEST_RUNIFI_ENC_NOACTIVE_";
        let key_id = "key1".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}{}", prefix, "key1"), hex_key_aa());
        }

        let result = EnvKeyProvider::new("nonexistent".to_string(), &[key_id], prefix);
        assert!(result.is_err());

        unsafe {
            std::env::remove_var(format!("{}{}", prefix, "key1"));
        }
    }

    #[test]
    fn get_missing_key_fails() {
        let prefix = "TEST_RUNIFI_ENC_GETMISS_";
        let key_id = "key1".to_string();
        // SAFETY: Test uses unique env var prefix; no concurrent env access.
        unsafe {
            std::env::set_var(format!("{}{}", prefix, "key1"), hex_key_aa());
        }

        let provider = EnvKeyProvider::new(key_id.clone(), &[key_id], prefix).unwrap();
        let result = provider.get_key(&"nonexistent".to_string());
        assert!(result.is_err());

        unsafe {
            std::env::remove_var(format!("{}{}", prefix, "key1"));
        }
    }
}
