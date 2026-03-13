use std::collections::HashMap;

use zeroize::Zeroize;

use super::key_provider::{KeyError, KeyId, KeyProvider};

/// A key provider that holds keys in memory, loaded from configuration.
///
/// Suitable for single-node deployments where keys are supplied via config
/// files or environment variables. For production clusters, consider
/// integrating with HashiCorp Vault or a cloud KMS.
pub struct StaticKeyProvider {
    keys: HashMap<KeyId, Vec<u8>>,
    active_key_id: KeyId,
}

impl StaticKeyProvider {
    /// Create a new static key provider.
    ///
    /// `active_key_id` must exist in the `keys` map. All keys must be
    /// exactly 32 bytes (256 bits) for AES-256-GCM.
    pub fn new(keys: HashMap<KeyId, Vec<u8>>, active_key_id: KeyId) -> Result<Self, KeyError> {
        if !keys.contains_key(&active_key_id) {
            return Err(KeyError::NotFound(active_key_id));
        }
        for (id, key) in &keys {
            if key.len() != 32 {
                return Err(KeyError::Provider(format!(
                    "Key '{}' must be 32 bytes, got {}",
                    id,
                    key.len()
                )));
            }
        }
        Ok(Self {
            keys,
            active_key_id,
        })
    }

    /// Create a provider from a single hex-encoded key.
    pub fn from_hex(key_id: KeyId, hex_key: &str) -> Result<Self, KeyError> {
        let key_bytes = hex::decode(hex_key)
            .map_err(|e| KeyError::Provider(format!("Invalid hex key: {}", e)))?;
        let mut keys = HashMap::new();
        keys.insert(key_id.clone(), key_bytes);
        Self::new(keys, key_id)
    }
}

impl Drop for StaticKeyProvider {
    fn drop(&mut self) {
        for (_, key) in self.keys.iter_mut() {
            key.zeroize();
        }
    }
}

impl KeyProvider for StaticKeyProvider {
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

    fn test_key() -> Vec<u8> {
        vec![0xAA; 32]
    }

    #[test]
    fn active_key_returns_correct_key() {
        let mut keys = HashMap::new();
        keys.insert("key-1".to_string(), test_key());
        let provider = StaticKeyProvider::new(keys, "key-1".to_string()).unwrap();

        let (id, key) = provider.active_key().unwrap();
        assert_eq!(id, "key-1");
        assert_eq!(key, test_key());
    }

    #[test]
    fn get_key_returns_correct_key() {
        let mut keys = HashMap::new();
        keys.insert("key-1".to_string(), vec![0xAA; 32]);
        keys.insert("key-2".to_string(), vec![0xBB; 32]);
        let provider = StaticKeyProvider::new(keys, "key-1".to_string()).unwrap();

        let key = provider.get_key(&"key-2".to_string()).unwrap();
        assert_eq!(key, vec![0xBB; 32]);
    }

    #[test]
    fn missing_active_key_fails() {
        let keys = HashMap::new();
        let result = StaticKeyProvider::new(keys, "missing".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn wrong_key_size_fails() {
        let mut keys = HashMap::new();
        keys.insert("key-1".to_string(), vec![0xAA; 16]); // 128-bit, too short
        let result = StaticKeyProvider::new(keys, "key-1".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn from_hex_works() {
        let hex_key = "aa".repeat(32); // 64 hex chars = 32 bytes
        let provider = StaticKeyProvider::from_hex("key-1".to_string(), &hex_key).unwrap();
        let (id, key) = provider.active_key().unwrap();
        assert_eq!(id, "key-1");
        assert_eq!(key, vec![0xAA; 32]);
    }

    #[test]
    fn from_hex_invalid_fails() {
        let result = StaticKeyProvider::from_hex("key-1".to_string(), "not-hex");
        assert!(result.is_err());
    }

    #[test]
    fn get_missing_key_fails() {
        let mut keys = HashMap::new();
        keys.insert("key-1".to_string(), test_key());
        let provider = StaticKeyProvider::new(keys, "key-1".to_string()).unwrap();

        let result = provider.get_key(&"nonexistent".to_string());
        assert!(result.is_err());
    }
}
