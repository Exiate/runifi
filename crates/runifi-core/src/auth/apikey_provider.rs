//! API key authentication provider — wraps existing [`SecurityConfig`] API key validation.

use subtle::ConstantTimeEq;

use crate::config::flow_config::SecurityConfig;

use super::provider::{
    AuthCredentials, AuthError, AuthProvider, AuthResult, CredentialType, UserIdentity,
};

/// Authenticates requests using pre-configured API keys.
#[derive(Debug)]
pub struct ApiKeyAuthProvider {
    security: SecurityConfig,
}

impl ApiKeyAuthProvider {
    pub fn new(security: SecurityConfig) -> Self {
        Self { security }
    }

    fn validate_key(&self, provided: &str) -> Option<&str> {
        let mut found_role: Option<&str> = None;
        // Compare against all keys in constant time to prevent length-based
        // timing side-channels. Pad both values to equal length before comparison.
        for key in self.security.key_strings() {
            let max_len = provided.len().max(key.len());
            let mut a = vec![0u8; max_len];
            let mut b = vec![0u8; max_len];
            a[..provided.len()].copy_from_slice(provided.as_bytes());
            b[..key.len()].copy_from_slice(key.as_bytes());
            let lengths_match = provided.len() == key.len();
            if lengths_match && bool::from(a.ct_eq(&b)) && found_role.is_none() {
                found_role = self.security.role_for_key(key);
            }
        }
        found_role
    }
}

#[async_trait::async_trait]
impl AuthProvider for ApiKeyAuthProvider {
    fn name(&self) -> &str {
        "api-key"
    }

    fn supported_credentials(&self) -> &[CredentialType] {
        &[CredentialType::ApiKey]
    }

    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        let key = match credentials {
            AuthCredentials::ApiKey(k) => k,
            _ => return AuthResult::Unsupported,
        };

        if key.is_empty() {
            return AuthResult::Failed(AuthError::InvalidCredentials);
        }

        match self.validate_key(key) {
            Some(role) => AuthResult::Authenticated(UserIdentity {
                username: format!("api-key-***{}", &key[key.len().saturating_sub(4)..]),
                display_name: None,
                groups: vec![role.to_string()],
                provider: "api-key".into(),
                email: None,
                expires_at: None,
                provider_data: None,
            }),
            None => AuthResult::Failed(AuthError::InvalidCredentials),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::flow_config::{ApiKeyEntry, ApiKeyWithRole};

    fn test_security() -> SecurityConfig {
        SecurityConfig {
            api_keys: vec![
                ApiKeyEntry::Simple("test-key-12345".to_string()),
                ApiKeyEntry::WithRole(ApiKeyWithRole {
                    key: "op-key-67890".to_string(),
                    role: "operator".to_string(),
                }),
            ],
            ..SecurityConfig::default()
        }
    }

    #[tokio::test]
    async fn valid_simple_key() {
        let provider = ApiKeyAuthProvider::new(test_security());
        let creds = AuthCredentials::ApiKey("test-key-12345".into());

        match provider.authenticate(&creds).await {
            AuthResult::Authenticated(id) => {
                assert_eq!(id.provider, "api-key");
                assert!(id.groups.contains(&"admin".to_string()));
                // Username shows only last 4 chars of key.
                assert_eq!(id.username, "api-key-***2345");
            }
            _ => panic!("Expected Authenticated"),
        }
    }

    #[tokio::test]
    async fn valid_role_key() {
        let provider = ApiKeyAuthProvider::new(test_security());
        let creds = AuthCredentials::ApiKey("op-key-67890".into());

        match provider.authenticate(&creds).await {
            AuthResult::Authenticated(id) => {
                assert!(id.groups.contains(&"operator".to_string()));
            }
            _ => panic!("Expected Authenticated"),
        }
    }

    #[tokio::test]
    async fn invalid_key() {
        let provider = ApiKeyAuthProvider::new(test_security());
        let creds = AuthCredentials::ApiKey("wrong-key".into());

        match provider.authenticate(&creds).await {
            AuthResult::Failed(AuthError::InvalidCredentials) => {}
            _ => panic!("Expected Failed(InvalidCredentials)"),
        }
    }

    #[tokio::test]
    async fn unsupported_credential_type() {
        let provider = ApiKeyAuthProvider::new(test_security());
        let creds = AuthCredentials::Password {
            username: "test".into(),
            password: "pass".into(),
        };

        match provider.authenticate(&creds).await {
            AuthResult::Unsupported => {}
            _ => panic!("Expected Unsupported"),
        }
    }
}
