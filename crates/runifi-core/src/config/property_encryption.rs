use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit, Nonce};
use thiserror::Error;

/// Errors from property value encryption/decryption.
#[derive(Debug, Error)]
pub enum PropertyEncryptionError {
    #[error("invalid ENC() format: {0}")]
    InvalidFormat(String),

    #[error("base64 decode error: {0}")]
    Base64(String),

    #[error("invalid encryption key: {0}")]
    InvalidKey(String),

    #[error("decryption failed: {0}")]
    DecryptionFailed(String),

    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
}

/// Nonce size for AES-256-GCM.
const NONCE_SIZE: usize = 12;

/// Check whether a property value uses the `ENC(...)` envelope format.
pub fn is_encrypted_value(value: &str) -> bool {
    value.starts_with("ENC(") && value.ends_with(')')
}

/// Decrypt a property value that uses the `ENC(base64-ciphertext)` format.
///
/// The base64 payload is `nonce (12 bytes) || ciphertext + GCM tag`.
/// The `key` must be exactly 32 bytes (AES-256).
///
/// Returns the plaintext string on success.
pub fn decrypt_property_value(value: &str, key: &[u8]) -> Result<String, PropertyEncryptionError> {
    // Strip the ENC(...) wrapper.
    let inner = value
        .strip_prefix("ENC(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            PropertyEncryptionError::InvalidFormat(
                "value must be in ENC(base64) format".to_string(),
            )
        })?;

    // Decode base64.
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    let decoded = STANDARD
        .decode(inner)
        .map_err(|e| PropertyEncryptionError::Base64(e.to_string()))?;

    if decoded.len() < NONCE_SIZE + 16 {
        return Err(PropertyEncryptionError::InvalidFormat(
            "encrypted payload too short (need at least nonce + GCM tag)".to_string(),
        ));
    }

    let (nonce_bytes, ciphertext) = decoded.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| PropertyEncryptionError::InvalidKey(e.to_string()))?;

    let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
        PropertyEncryptionError::DecryptionFailed("authentication failed".to_string())
    })?;

    String::from_utf8(plaintext).map_err(|e| {
        PropertyEncryptionError::DecryptionFailed(format!(
            "decrypted value is not valid UTF-8: {}",
            e
        ))
    })
}

/// Encrypt a plaintext property value into the `ENC(base64)` format.
///
/// The base64 payload is `nonce (12 bytes) || ciphertext + GCM tag`.
/// The `key` must be exactly 32 bytes (AES-256).
///
/// This function is primarily for testing and for tooling that generates
/// encrypted config values.
pub fn encrypt_property_value(
    plaintext: &str,
    key: &[u8],
) -> Result<String, PropertyEncryptionError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| PropertyEncryptionError::InvalidKey(e.to_string()))?;

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| PropertyEncryptionError::EncryptionFailed(e.to_string()))?;

    // Combine nonce + ciphertext into one blob, then base64 encode.
    let mut payload = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&ciphertext);

    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    let encoded = STANDARD.encode(&payload);
    Ok(format!("ENC({})", encoded))
}

/// Expand environment variable references in a property value string.
///
/// Supported patterns:
/// - `${VAR_NAME}` — replaced with the value of `VAR_NAME`, or left as-is
///   if the variable is not set.
/// - `${VAR_NAME:-default}` — replaced with the value of `VAR_NAME`, or
///   `default` if the variable is not set.
///
/// Literal `${` sequences that should not be expanded are not currently
/// supported (no escaping mechanism).
pub fn expand_env_vars(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            // Consume the '{'
            chars.next();

            // Read until '}' or end of string.
            let mut var_expr = String::new();
            let mut found_close = false;
            for c in chars.by_ref() {
                if c == '}' {
                    found_close = true;
                    break;
                }
                var_expr.push(c);
            }

            if !found_close {
                // Malformed — no closing brace, emit as-is.
                result.push_str("${");
                result.push_str(&var_expr);
            } else if let Some(colon_pos) = var_expr.find(":-") {
                let var_name = &var_expr[..colon_pos];
                let default_val = &var_expr[colon_pos + 2..];
                match std::env::var(var_name) {
                    Ok(val) if !val.is_empty() => result.push_str(&val),
                    _ => result.push_str(default_val),
                }
            } else {
                // Simple ${VAR_NAME} — leave original text if not set.
                match std::env::var(&var_expr) {
                    Ok(val) => result.push_str(&val),
                    Err(_) => {
                        result.push_str("${");
                        result.push_str(&var_expr);
                        result.push('}');
                    }
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- expand_env_vars tests --

    #[test]
    fn expand_env_vars_no_vars() {
        assert_eq!(expand_env_vars("hello world"), "hello world");
    }

    #[test]
    fn expand_env_vars_simple() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::set_var("RUNIFI_TEST_EXPAND_A", "alpha") };
        let result = expand_env_vars("prefix-${RUNIFI_TEST_EXPAND_A}-suffix");
        assert_eq!(result, "prefix-alpha-suffix");
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_A") };
    }

    #[test]
    fn expand_env_vars_with_default_used() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_MISSING") };
        let result = expand_env_vars("${RUNIFI_TEST_EXPAND_MISSING:-fallback}");
        assert_eq!(result, "fallback");
    }

    #[test]
    fn expand_env_vars_with_default_not_needed() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::set_var("RUNIFI_TEST_EXPAND_B", "beta") };
        let result = expand_env_vars("${RUNIFI_TEST_EXPAND_B:-fallback}");
        assert_eq!(result, "beta");
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_B") };
    }

    #[test]
    fn expand_env_vars_unset_left_as_is() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_GONE") };
        let result = expand_env_vars("${RUNIFI_TEST_EXPAND_GONE}");
        assert_eq!(result, "${RUNIFI_TEST_EXPAND_GONE}");
    }

    #[test]
    fn expand_env_vars_multiple() {
        // SAFETY: test runs single-threaded; env var names are unique to this test.
        unsafe {
            std::env::set_var("RUNIFI_TEST_EX_X", "xx");
            std::env::set_var("RUNIFI_TEST_EX_Y", "yy");
        }
        let result = expand_env_vars("${RUNIFI_TEST_EX_X}:${RUNIFI_TEST_EX_Y}");
        assert_eq!(result, "xx:yy");
        unsafe {
            std::env::remove_var("RUNIFI_TEST_EX_X");
            std::env::remove_var("RUNIFI_TEST_EX_Y");
        }
    }

    #[test]
    fn expand_env_vars_empty_default() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_EMPTY_DEFAULT") };
        let result = expand_env_vars("${RUNIFI_TEST_EXPAND_EMPTY_DEFAULT:-}");
        assert_eq!(result, "");
    }

    #[test]
    fn expand_env_vars_unclosed_brace() {
        let result = expand_env_vars("${UNCLOSED");
        assert_eq!(result, "${UNCLOSED");
    }

    #[test]
    fn expand_env_vars_plain_dollar() {
        let result = expand_env_vars("$100 price");
        assert_eq!(result, "$100 price");
    }

    #[test]
    fn expand_env_vars_empty_var_uses_default() {
        // SAFETY: test runs single-threaded; env var name is unique to this test.
        unsafe { std::env::set_var("RUNIFI_TEST_EXPAND_EMPTY_VAR", "") };
        let result = expand_env_vars("${RUNIFI_TEST_EXPAND_EMPTY_VAR:-default_val}");
        assert_eq!(result, "default_val");
        unsafe { std::env::remove_var("RUNIFI_TEST_EXPAND_EMPTY_VAR") };
    }

    // -- encrypt / decrypt round-trip tests --

    fn test_key() -> Vec<u8> {
        vec![0xAA; 32]
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        let key = test_key();
        let plaintext = "my-database-password";
        let encrypted = encrypt_property_value(plaintext, &key).unwrap();

        assert!(is_encrypted_value(&encrypted));
        assert!(encrypted.starts_with("ENC("));
        assert!(encrypted.ends_with(')'));

        let decrypted = decrypt_property_value(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_empty_string() {
        let key = test_key();
        let encrypted = encrypt_property_value("", &key).unwrap();
        let decrypted = decrypt_property_value(&encrypted, &key).unwrap();
        assert_eq!(decrypted, "");
    }

    #[test]
    fn decrypt_wrong_key_fails() {
        let key_a = vec![0xAA; 32];
        let key_b = vec![0xBB; 32];
        let encrypted = encrypt_property_value("secret", &key_a).unwrap();
        let result = decrypt_property_value(&encrypted, &key_b);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_invalid_format() {
        let key = test_key();
        // Not wrapped in ENC().
        let result = decrypt_property_value("not-encrypted", &key);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_invalid_base64() {
        let key = test_key();
        let result = decrypt_property_value("ENC(!!!invalid-base64!!!)", &key);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_too_short_payload() {
        let key = test_key();
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        // Only 10 bytes — too short for nonce + tag.
        let encoded = STANDARD.encode(&[0u8; 10]);
        let enc_val = format!("ENC({})", encoded);
        let result = decrypt_property_value(&enc_val, &key);
        assert!(result.is_err());
    }

    #[test]
    fn is_encrypted_value_positive() {
        assert!(is_encrypted_value("ENC(abc123==)"));
    }

    #[test]
    fn is_encrypted_value_negative() {
        assert!(!is_encrypted_value("plaintext"));
        assert!(!is_encrypted_value("ENC("));
        assert!(!is_encrypted_value("ENC abc)"));
    }

    #[test]
    fn encrypt_invalid_key_fails() {
        let bad_key = vec![0xAA; 16]; // 128-bit, not 256-bit
        let result = encrypt_property_value("test", &bad_key);
        assert!(result.is_err());
    }
}
