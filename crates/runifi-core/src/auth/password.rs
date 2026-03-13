//! Password hashing and verification using Argon2id.

use argon2::Argon2;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};

/// Hash a plaintext password with Argon2id.
///
/// Returns the PHC-formatted hash string containing the algorithm parameters,
/// salt, and hash — suitable for storage.
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

/// Verify a plaintext password against a stored Argon2 hash.
///
/// Returns `true` if the password matches, `false` otherwise.
pub fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_correct_password() {
        let hash = hash_password("my-secure-password").unwrap();
        assert!(verify_password("my-secure-password", &hash));
    }

    #[test]
    fn verify_wrong_password_fails() {
        let hash = hash_password("correct-password").unwrap();
        assert!(!verify_password("wrong-password", &hash));
    }

    #[test]
    fn verify_invalid_hash_fails() {
        assert!(!verify_password("anything", "not-a-valid-hash"));
    }

    #[test]
    fn hash_is_unique_per_call() {
        let h1 = hash_password("same-password").unwrap();
        let h2 = hash_password("same-password").unwrap();
        // Different salts produce different hashes.
        assert_ne!(h1, h2);
        // But both verify correctly.
        assert!(verify_password("same-password", &h1));
        assert!(verify_password("same-password", &h2));
    }

    #[test]
    fn hash_contains_argon2_marker() {
        let hash = hash_password("test").unwrap();
        assert!(hash.starts_with("$argon2"));
    }
}
