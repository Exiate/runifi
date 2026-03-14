//! Pluggable authentication provider abstraction.
//!
//! Defines the [`AuthProvider`] trait that all authentication backends implement,
//! along with the credential and identity types that flow through the system.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Credential types that auth providers can accept.
#[derive(Debug, Clone)]
pub enum AuthCredentials {
    /// Username + password (local, LDAP).
    Password { username: String, password: String },
    /// Bearer token (JWT, OIDC access token).
    BearerToken(String),
    /// Client certificate — DER-encoded bytes.
    Certificate(Vec<u8>),
    /// API key (legacy).
    ApiKey(String),
    /// OIDC authorization code callback.
    OidcCallback { code: String, state: String },
}

/// Identifies which credential type a provider supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CredentialType {
    Password,
    BearerToken,
    Certificate,
    ApiKey,
    OidcCallback,
}

/// A unified identity produced by any auth provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentity {
    /// Unique username (e.g., "jane.doe", CN from cert, OIDC sub).
    pub username: String,
    /// Display name (OIDC name claim, LDAP cn, etc.).
    pub display_name: Option<String>,
    /// Groups/roles from the identity source (LDAP groups, OIDC claims).
    pub groups: Vec<String>,
    /// Which provider authenticated this identity.
    pub provider: String,
    /// Email if available.
    pub email: Option<String>,
    /// When this identity information expires.
    pub expires_at: Option<DateTime<Utc>>,
    /// Opaque provider-specific data (e.g., OIDC refresh token).
    #[serde(skip_serializing)]
    pub provider_data: Option<String>,
}

/// Result of an authentication attempt.
pub enum AuthResult {
    /// Authenticated successfully.
    Authenticated(UserIdentity),
    /// This provider doesn't handle this credential type — skip to next.
    Unsupported,
    /// Authentication failed (bad credentials, expired, revoked).
    Failed(AuthError),
}

/// Errors specific to authentication.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Account disabled: {0}")]
    AccountDisabled(String),
    #[error("Token expired")]
    TokenExpired,
    #[error("Token revoked")]
    TokenRevoked,
    #[error("Certificate rejected: {0}")]
    CertificateRejected(String),
    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Trait that all authentication providers implement.
///
/// Each provider handles one or more credential types and returns a unified
/// [`UserIdentity`] on success. The engine manages async wrapping — providers
/// use async for I/O-bound operations (LDAP, OIDC token exchange).
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync + std::fmt::Debug {
    /// Human-readable name for this provider (e.g., "local", "oidc", "ldap").
    fn name(&self) -> &str;

    /// Which credential types this provider can authenticate.
    fn supported_credentials(&self) -> &[CredentialType];

    /// Attempt authentication with the given credentials.
    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult;

    /// Attempt to refresh an authentication (e.g., OIDC refresh token).
    /// Returns [`AuthResult::Unsupported`] if the provider doesn't support refresh.
    async fn refresh(&self, _token: &str) -> AuthResult {
        AuthResult::Unsupported
    }

    /// Called on logout to allow the provider to do cleanup.
    /// Returns an optional redirect URL (e.g., OIDC RP-initiated logout).
    async fn logout(&self, _identity: &UserIdentity) -> Result<Option<String>, AuthError> {
        Ok(None)
    }
}

impl AuthCredentials {
    /// Return the credential type for dispatch.
    pub fn credential_type(&self) -> CredentialType {
        match self {
            AuthCredentials::Password { .. } => CredentialType::Password,
            AuthCredentials::BearerToken(_) => CredentialType::BearerToken,
            AuthCredentials::Certificate(_) => CredentialType::Certificate,
            AuthCredentials::ApiKey(_) => CredentialType::ApiKey,
            AuthCredentials::OidcCallback { .. } => CredentialType::OidcCallback,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_type_dispatch() {
        let cred = AuthCredentials::Password {
            username: "test".into(),
            password: "pass".into(),
        };
        assert_eq!(cred.credential_type(), CredentialType::Password);

        let cred = AuthCredentials::BearerToken("tok".into());
        assert_eq!(cred.credential_type(), CredentialType::BearerToken);

        let cred = AuthCredentials::Certificate(vec![1, 2, 3]);
        assert_eq!(cred.credential_type(), CredentialType::Certificate);

        let cred = AuthCredentials::ApiKey("key".into());
        assert_eq!(cred.credential_type(), CredentialType::ApiKey);

        let cred = AuthCredentials::OidcCallback {
            code: "code".into(),
            state: "state".into(),
        };
        assert_eq!(cred.credential_type(), CredentialType::OidcCallback);
    }

    #[test]
    fn user_identity_serialization_skips_provider_data() {
        let identity = UserIdentity {
            username: "alice".into(),
            display_name: Some("Alice".into()),
            groups: vec!["admins".into()],
            provider: "local".into(),
            email: Some("alice@example.com".into()),
            expires_at: None,
            provider_data: Some("secret-refresh-token".into()),
        };
        let json = serde_json::to_string(&identity).unwrap();
        assert!(!json.contains("secret-refresh-token"));
        assert!(json.contains("alice"));
    }

    #[test]
    fn auth_error_display() {
        let err = AuthError::InvalidCredentials;
        assert_eq!(err.to_string(), "Invalid credentials");

        let err = AuthError::AccountDisabled("bob".into());
        assert_eq!(err.to_string(), "Account disabled: bob");
    }
}
