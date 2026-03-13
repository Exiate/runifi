//! File-based key provider that reads encryption keys from a JSON file.
//!
//! ## Key File Format
//!
//! ```json
//! {
//!   "active_key_id": "key-2024-01",
//!   "keys": {
//!     "key-2024-01": "hex-encoded-256-bit-key...",
//!     "key-2023-12": "hex-encoded-256-bit-key..."
//!   }
//! }
//! ```
//!
//! All keys must be hex-encoded 256-bit (32-byte) values for AES-256-GCM.
//! Multiple keys support key rotation: new writes use the active key,
//! old content can still be decrypted with its original key.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use zeroize::Zeroize;

use super::key_provider::{KeyError, KeyId, KeyProvider};

/// JSON schema for the key file.
#[derive(Debug, Deserialize)]
struct KeyFileContents {
    /// The key ID to use for new encryption operations.
    active_key_id: String,
    /// Map of key_id -> hex-encoded 256-bit key.
    keys: HashMap<String, String>,
}

/// A key provider that reads encryption keys from a JSON file on disk.
///
/// Suitable for single-node deployments where keys are managed via
/// configuration files. The key file should have restricted permissions
/// (e.g., 0600) and be stored outside the application directory.
#[derive(Debug)]
pub struct FileKeyProvider {
    keys: HashMap<KeyId, Vec<u8>>,
    active_key_id: KeyId,
}

impl FileKeyProvider {
    /// Load keys from a JSON file at the given path.
    ///
    /// The file must contain a JSON object with `active_key_id` and `keys`
    /// fields. All keys must be hex-encoded 32-byte values.
    pub fn from_file(path: &Path) -> Result<Self, KeyError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            KeyError::Provider(format!(
                "Failed to read key file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Self::from_json(&contents, path.display().to_string())
    }

    /// Parse keys from a JSON string (for testing or in-memory use).
    fn from_json(json: &str, source: String) -> Result<Self, KeyError> {
        let file_contents: KeyFileContents = serde_json::from_str(json).map_err(|e| {
            KeyError::Provider(format!("Failed to parse key file '{}': {}", source, e))
        })?;

        if file_contents.keys.is_empty() {
            return Err(KeyError::Provider(format!(
                "Key file '{}' contains no keys",
                source
            )));
        }

        if !file_contents
            .keys
            .contains_key(&file_contents.active_key_id)
        {
            return Err(KeyError::NotFound(format!(
                "Active key '{}' not found in key file '{}'",
                file_contents.active_key_id, source
            )));
        }

        let mut keys = HashMap::new();
        for (id, hex_key) in &file_contents.keys {
            let key_bytes = hex::decode(hex_key).map_err(|e| {
                KeyError::Provider(format!("Key '{}' has invalid hex encoding: {}", id, e))
            })?;
            if key_bytes.len() != 32 {
                return Err(KeyError::Provider(format!(
                    "Key '{}' must be 32 bytes (256 bits), got {} bytes",
                    id,
                    key_bytes.len()
                )));
            }
            keys.insert(id.clone(), key_bytes);
        }

        Ok(Self {
            keys,
            active_key_id: file_contents.active_key_id,
        })
    }
}

impl Drop for FileKeyProvider {
    fn drop(&mut self) {
        for (_, key) in self.keys.iter_mut() {
            key.zeroize();
        }
    }
}

impl KeyProvider for FileKeyProvider {
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

    fn valid_hex_key() -> String {
        "aa".repeat(32) // 64 hex chars = 32 bytes
    }

    fn another_hex_key() -> String {
        "bb".repeat(32)
    }

    fn make_key_json(active: &str, keys: &[(&str, &str)]) -> String {
        let key_entries: Vec<String> = keys
            .iter()
            .map(|(id, hex)| format!("    \"{}\": \"{}\"", id, hex))
            .collect();
        format!(
            r#"{{
  "active_key_id": "{}",
  "keys": {{
{}
  }}
}}"#,
            active,
            key_entries.join(",\n")
        )
    }

    #[test]
    fn load_single_key() {
        let json = make_key_json("key-1", &[("key-1", &valid_hex_key())]);
        let provider = FileKeyProvider::from_json(&json, "test".to_string()).unwrap();

        let (id, key) = provider.active_key().unwrap();
        assert_eq!(id, "key-1");
        assert_eq!(key, vec![0xAA; 32]);
    }

    #[test]
    fn load_multiple_keys() {
        let json = make_key_json(
            "key-2",
            &[("key-1", &valid_hex_key()), ("key-2", &another_hex_key())],
        );
        let provider = FileKeyProvider::from_json(&json, "test".to_string()).unwrap();

        let (id, _) = provider.active_key().unwrap();
        assert_eq!(id, "key-2");

        let key1 = provider.get_key(&"key-1".to_string()).unwrap();
        assert_eq!(key1, vec![0xAA; 32]);

        let key2 = provider.get_key(&"key-2".to_string()).unwrap();
        assert_eq!(key2, vec![0xBB; 32]);
    }

    #[test]
    fn missing_active_key_fails() {
        let json = make_key_json("nonexistent", &[("key-1", &valid_hex_key())]);
        let result = FileKeyProvider::from_json(&json, "test".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn empty_keys_fails() {
        let json = r#"{ "active_key_id": "key-1", "keys": {} }"#;
        let result = FileKeyProvider::from_json(json, "test".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn invalid_hex_fails() {
        let json = make_key_json("key-1", &[("key-1", "not-valid-hex")]);
        let result = FileKeyProvider::from_json(&json, "test".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn wrong_key_size_fails() {
        let short_key = "aa".repeat(16); // 16 bytes, too short
        let json = make_key_json("key-1", &[("key-1", &short_key)]);
        let result = FileKeyProvider::from_json(&json, "test".to_string());
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("32 bytes"));
    }

    #[test]
    fn get_missing_key_fails() {
        let json = make_key_json("key-1", &[("key-1", &valid_hex_key())]);
        let provider = FileKeyProvider::from_json(&json, "test".to_string()).unwrap();
        let result = provider.get_key(&"nonexistent".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("keys.json");
        let json = make_key_json("key-1", &[("key-1", &valid_hex_key())]);
        std::fs::write(&path, json).unwrap();

        let provider = FileKeyProvider::from_file(&path).unwrap();
        let (id, key) = provider.active_key().unwrap();
        assert_eq!(id, "key-1");
        assert_eq!(key, vec![0xAA; 32]);
    }

    #[test]
    fn load_from_nonexistent_file_fails() {
        let result = FileKeyProvider::from_file(Path::new("/nonexistent/keys.json"));
        assert!(result.is_err());
    }

    #[test]
    fn invalid_json_fails() {
        let result = FileKeyProvider::from_json("not json", "test".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn key_rotation_scenario() {
        // Start with key-1 as active.
        let json = make_key_json(
            "key-2",
            &[("key-1", &valid_hex_key()), ("key-2", &another_hex_key())],
        );
        let provider = FileKeyProvider::from_json(&json, "test".to_string()).unwrap();

        // Active key is key-2 for new writes.
        let (active_id, _) = provider.active_key().unwrap();
        assert_eq!(active_id, "key-2");

        // Old key-1 is still available for decrypting old content.
        let old_key = provider.get_key(&"key-1".to_string()).unwrap();
        assert_eq!(old_key, vec![0xAA; 32]);
    }
}
