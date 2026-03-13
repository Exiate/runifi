//! JWT token generation and validation.

use chrono::Utc;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JWT claims embedded in every access token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject — the user ID.
    pub sub: String,
    /// Username (convenience claim for display).
    pub username: String,
    /// Issued-at timestamp (seconds since epoch).
    pub iat: i64,
    /// Expiration timestamp (seconds since epoch).
    pub exp: i64,
    /// Unique token identifier (for revocation tracking).
    pub jti: String,
}

/// Configuration for JWT token handling.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC-SHA256 secret key.
    secret: Vec<u8>,
    /// Token lifetime in seconds.
    expiry_secs: i64,
}

impl JwtConfig {
    /// Create a new JWT configuration.
    pub fn new(secret: &str, expiry_secs: i64) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
            expiry_secs,
        }
    }

    /// Generate a signed JWT for the given user.
    pub fn generate_token(
        &self,
        user_id: Uuid,
        username: &str,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        let now = Utc::now().timestamp();
        let claims = Claims {
            sub: user_id.to_string(),
            username: username.to_string(),
            iat: now,
            exp: now + self.expiry_secs,
            jti: Uuid::now_v7().to_string(),
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.secret),
        )
    }

    /// Validate a JWT and return the claims if valid.
    pub fn validate_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let mut validation = Validation::default();
        // No clock leeway — enforce strict expiration.
        validation.leeway = 0;
        let token_data =
            decode::<Claims>(token, &DecodingKey::from_secret(&self.secret), &validation)?;
        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> JwtConfig {
        JwtConfig::new("test-secret-key-at-least-32-chars-long!", 3600)
    }

    #[test]
    fn generate_and_validate_token() {
        let config = test_config();
        let user_id = Uuid::now_v7();
        let token = config.generate_token(user_id, "admin").unwrap();
        let claims = config.validate_token(&token).unwrap();
        assert_eq!(claims.sub, user_id.to_string());
        assert_eq!(claims.username, "admin");
    }

    #[test]
    fn expired_token_is_rejected() {
        let config = JwtConfig::new("test-secret", -1); // Already expired
        let user_id = Uuid::now_v7();
        let token = config.generate_token(user_id, "admin").unwrap();
        assert!(config.validate_token(&token).is_err());
    }

    #[test]
    fn invalid_token_is_rejected() {
        let config = test_config();
        assert!(config.validate_token("not.a.valid.token").is_err());
    }

    #[test]
    fn wrong_secret_rejects_token() {
        let config1 = JwtConfig::new("secret-one", 3600);
        let config2 = JwtConfig::new("secret-two", 3600);
        let user_id = Uuid::now_v7();
        let token = config1.generate_token(user_id, "admin").unwrap();
        assert!(config2.validate_token(&token).is_err());
    }

    #[test]
    fn each_token_has_unique_jti() {
        let config = test_config();
        let user_id = Uuid::now_v7();
        let t1 = config.generate_token(user_id, "admin").unwrap();
        let t2 = config.generate_token(user_id, "admin").unwrap();
        let c1 = config.validate_token(&t1).unwrap();
        let c2 = config.validate_token(&t2).unwrap();
        assert_ne!(c1.jti, c2.jti);
    }
}
