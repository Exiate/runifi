use std::sync::Arc;

use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use bytes::Bytes;
use dashmap::DashMap;
use runifi_plugin_api::ContentClaim;

use super::content_repo::ContentRepository;
use super::key_provider::KeyProvider;
use crate::error::{Result, RuniFiError};

/// Nonce size for AES-256-GCM.
const NONCE_SIZE: usize = 12;

/// A content repository wrapper that encrypts data at rest using AES-256-GCM.
///
/// Wraps any `ContentRepository` implementation, transparently encrypting on
/// `create()` and decrypting on `read()`. Each stored blob includes the key
/// identifier and a random nonce, enabling key rotation — older content
/// encrypted with a previous key can still be decrypted as long as the key
/// provider retains that key.
///
/// ## Wire format
///
/// ```text
/// [key_id_len: 1 byte][key_id: N bytes][nonce: 12 bytes][ciphertext + AES-GCM tag (16 bytes)]
/// ```
pub struct EncryptedContentRepository {
    inner: Arc<dyn ContentRepository>,
    key_provider: Arc<dyn KeyProvider>,
    /// Maps resource_id -> envelope byte length.
    ///
    /// The outer `ContentClaim` exposes the plaintext length to callers, but
    /// the inner repository stores the (larger) encrypted envelope. This map
    /// tracks the envelope size so we can reconstruct the inner claim on read.
    envelope_lengths: DashMap<u64, u64>,
}

impl EncryptedContentRepository {
    pub fn new(inner: Arc<dyn ContentRepository>, key_provider: Arc<dyn KeyProvider>) -> Self {
        Self {
            inner,
            key_provider,
            envelope_lengths: DashMap::new(),
        }
    }

    /// Encrypt plaintext and prepend the envelope header.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let (key_id, key_bytes) = self
            .key_provider
            .active_key()
            .map_err(|e| RuniFiError::Config(format!("Encryption key error: {}", e)))?;

        let cipher = Aes256Gcm::new_from_slice(&key_bytes)
            .map_err(|e| RuniFiError::Config(format!("Invalid encryption key: {}", e)))?;

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| RuniFiError::Config(format!("Encryption failed: {}", e)))?;

        let key_id_bytes = key_id.as_bytes();
        let key_id_len = key_id_bytes.len();
        if key_id_len > 255 {
            return Err(RuniFiError::Config(
                "Key ID too long (max 255 bytes)".to_string(),
            ));
        }

        // Build the envelope: [key_id_len][key_id][nonce][ciphertext+tag]
        let total_len = 1 + key_id_len + NONCE_SIZE + ciphertext.len();
        let mut envelope = Vec::with_capacity(total_len);
        envelope.push(key_id_len as u8);
        envelope.extend_from_slice(key_id_bytes);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);

        Ok(envelope)
    }

    /// Parse the envelope header and decrypt the content.
    fn decrypt(&self, envelope: &[u8]) -> Result<Bytes> {
        // Minimum: 1 (key_id_len) + 0 (key_id) + 12 (nonce) + 16 (GCM tag)
        if envelope.len() < 1 + NONCE_SIZE + 16 {
            return Err(RuniFiError::Config(
                "Encrypted content too short".to_string(),
            ));
        }

        let key_id_len = envelope[0] as usize;
        let header_len = 1 + key_id_len + NONCE_SIZE;
        if envelope.len() < header_len + 16 {
            return Err(RuniFiError::Config(
                "Encrypted content header is malformed".to_string(),
            ));
        }

        let key_id = std::str::from_utf8(&envelope[1..1 + key_id_len])
            .map_err(|_| RuniFiError::Config("Invalid key ID encoding".to_string()))?
            .to_string();

        let nonce_start = 1 + key_id_len;
        let nonce = aes_gcm::Nonce::from_slice(&envelope[nonce_start..nonce_start + NONCE_SIZE]);

        let ciphertext = &envelope[header_len..];

        let key_bytes = self
            .key_provider
            .get_key(&key_id)
            .map_err(|e| RuniFiError::Config(format!("Decryption key error: {}", e)))?;

        let cipher = Aes256Gcm::new_from_slice(&key_bytes)
            .map_err(|e| RuniFiError::Config(format!("Invalid decryption key: {}", e)))?;

        let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
            RuniFiError::Config("Decryption failed: authentication error".to_string())
        })?;

        Ok(Bytes::from(plaintext))
    }
}

impl ContentRepository for EncryptedContentRepository {
    fn create(&self, data: Bytes) -> Result<ContentClaim> {
        let plaintext_len = data.len() as u64;
        let encrypted = self.encrypt(&data)?;
        let envelope_len = encrypted.len() as u64;

        let inner_claim = self.inner.create(Bytes::from(encrypted))?;

        // Track envelope length so we can reconstruct the inner claim on read.
        self.envelope_lengths
            .insert(inner_claim.resource_id, envelope_len);

        // Return claim with plaintext length for callers.
        Ok(ContentClaim {
            resource_id: inner_claim.resource_id,
            offset: 0,
            length: plaintext_len,
        })
    }

    fn read(&self, claim: &ContentClaim) -> Result<Bytes> {
        let envelope_len = self
            .envelope_lengths
            .get(&claim.resource_id)
            .ok_or(RuniFiError::ContentNotFound(claim.resource_id))?;

        let inner_claim = ContentClaim {
            resource_id: claim.resource_id,
            offset: 0,
            length: *envelope_len,
        };

        let encrypted = self.inner.read(&inner_claim)?;
        let plaintext = self.decrypt(&encrypted)?;

        // Apply the caller's offset/length to the decrypted plaintext.
        let start = claim.offset as usize;
        let end = start + claim.length as usize;
        if end > plaintext.len() {
            return Err(RuniFiError::ContentNotFound(claim.resource_id));
        }
        Ok(plaintext.slice(start..end))
    }

    fn increment_ref(&self, resource_id: u64) -> Result<()> {
        self.inner.increment_ref(resource_id)
    }

    fn decrement_ref(&self, resource_id: u64) -> Result<()> {
        // Clean up envelope length tracking when ref count drops to zero.
        // We attempt decrement on inner first; if it succeeds and the resource
        // is gone, we clean up our map. Since we can't observe the inner ref
        // count directly, we optimistically try to read after decrement.
        self.inner.decrement_ref(resource_id)?;

        // If the inner resource was removed, clean up the envelope length entry.
        // We probe by checking if a subsequent read would fail. This is a
        // lightweight check — if the entry doesn't exist in inner, it's been freed.
        let inner_claim = ContentClaim {
            resource_id,
            offset: 0,
            length: 0,
        };
        if self.inner.read(&inner_claim).is_err() {
            self.envelope_lengths.remove(&resource_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::repository::content_memory::InMemoryContentRepository;
    use crate::repository::static_key_provider::StaticKeyProvider;

    fn make_provider(keys: Vec<(&str, Vec<u8>)>, active: &str) -> Arc<dyn KeyProvider> {
        let mut map = HashMap::new();
        for (id, key) in keys {
            map.insert(id.to_string(), key);
        }
        Arc::new(StaticKeyProvider::new(map, active.to_string()).unwrap())
    }

    fn key_a() -> Vec<u8> {
        vec![0xAA; 32]
    }

    fn key_b() -> Vec<u8> {
        vec![0xBB; 32]
    }

    #[test]
    fn round_trip_encrypt_decrypt() {
        let inner = Arc::new(InMemoryContentRepository::new());
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = EncryptedContentRepository::new(inner, provider);

        let data = Bytes::from("hello world, this is a test of encryption at rest");
        let claim = repo.create(data.clone()).unwrap();

        assert_eq!(claim.offset, 0);
        assert_eq!(claim.length, data.len() as u64);

        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn stored_bytes_differ_from_plaintext() {
        let inner = Arc::new(InMemoryContentRepository::new());
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = EncryptedContentRepository::new(inner.clone(), provider);

        let data = Bytes::from("sensitive data that should be encrypted");
        let claim = repo.create(data.clone()).unwrap();

        // Read raw bytes from inner repo (the encrypted envelope).
        let envelope_len = repo.envelope_lengths.get(&claim.resource_id).unwrap();
        let inner_claim = ContentClaim {
            resource_id: claim.resource_id,
            offset: 0,
            length: *envelope_len,
        };
        let raw = inner.read(&inner_claim).unwrap();

        // The raw stored bytes should NOT equal the plaintext.
        assert_ne!(raw, data);
        // The envelope should be larger than the plaintext (header + tag overhead).
        assert!(raw.len() > data.len());
    }

    #[test]
    fn key_rotation_old_content_still_readable() {
        let inner = Arc::new(InMemoryContentRepository::new());

        // Store with key A.
        let provider_a = make_provider(vec![("key-a", key_a())], "key-a");
        let repo_a = EncryptedContentRepository::new(inner.clone(), provider_a);
        let data = Bytes::from("content encrypted with key A");
        let claim = repo_a.create(data.clone()).unwrap();

        // Copy the envelope length to the new repo instance.
        let envelope_len = *repo_a.envelope_lengths.get(&claim.resource_id).unwrap();

        // Create a new repo with key B as active, but still holding key A.
        let provider_ab = make_provider(vec![("key-a", key_a()), ("key-b", key_b())], "key-b");
        let repo_b = EncryptedContentRepository::new(inner.clone(), provider_ab);
        // Transfer the envelope length metadata.
        repo_b
            .envelope_lengths
            .insert(claim.resource_id, envelope_len);

        // Old content (encrypted with key A) should still be readable.
        let read_back = repo_b.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn tampered_ciphertext_fails_authentication() {
        let inner = Arc::new(InMemoryContentRepository::new());
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = EncryptedContentRepository::new(inner.clone(), provider.clone());

        let data = Bytes::from("data to tamper with");
        let claim = repo.create(data).unwrap();

        // Tamper with the stored ciphertext.
        let envelope_len = *repo.envelope_lengths.get(&claim.resource_id).unwrap();
        let inner_claim = ContentClaim {
            resource_id: claim.resource_id,
            offset: 0,
            length: envelope_len,
        };
        let mut raw = inner.read(&inner_claim).unwrap().to_vec();

        // Flip a byte in the ciphertext portion (past the header).
        let key_id_len = raw[0] as usize;
        let tamper_idx = 1 + key_id_len + NONCE_SIZE + 1;
        raw[tamper_idx] ^= 0xFF;

        // Overwrite in inner store: decrement old, create new with same resource_id.
        // Simpler: just create a new repo pointing to a fresh inner with tampered data.
        let tampered_inner = Arc::new(InMemoryContentRepository::new());
        let tampered_claim = tampered_inner.create(Bytes::from(raw)).unwrap();
        let tampered_repo = EncryptedContentRepository::new(tampered_inner, provider);
        tampered_repo
            .envelope_lengths
            .insert(tampered_claim.resource_id, envelope_len);

        let read_claim = ContentClaim {
            resource_id: tampered_claim.resource_id,
            offset: 0,
            length: claim.length,
        };

        // Decryption should fail with an authentication error.
        let result = tampered_repo.read(&read_claim);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("authentication error"),
            "Expected authentication error, got: {}",
            err_msg
        );
    }

    #[test]
    fn ref_counting_delegates_to_inner() {
        let inner = Arc::new(InMemoryContentRepository::new());
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = EncryptedContentRepository::new(inner, provider);

        let data = Bytes::from("ref counted content");
        let claim = repo.create(data.clone()).unwrap();

        // Increment ref count.
        repo.increment_ref(claim.resource_id).unwrap();

        // Decrement once — should still be readable.
        repo.decrement_ref(claim.resource_id).unwrap();
        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);

        // Decrement again — should be freed.
        repo.decrement_ref(claim.resource_id).unwrap();
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn empty_content_round_trip() {
        let inner = Arc::new(InMemoryContentRepository::new());
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = EncryptedContentRepository::new(inner, provider);

        let data = Bytes::new();
        let claim = repo.create(data.clone()).unwrap();
        assert_eq!(claim.length, 0);

        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }
}
