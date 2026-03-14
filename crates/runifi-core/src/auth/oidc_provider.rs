//! OIDC/OAuth 2.0 authentication provider.
//!
//! Implements the Authorization Code + PKCE flow for enterprise SSO integration.
//! Supports any OpenID Connect-compliant identity provider (Keycloak, Okta,
//! Azure AD, Auth0, Google Workspace).
//!
//! The provider is configured via TOML under `[auth.oidc]` and handles:
//! - Discovery via `.well-known/openid-configuration`
//! - JWKS-based ID token validation
//! - Claims-to-identity mapping
//! - Token refresh
//! - RP-initiated logout

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use serde::Deserialize;
use tracing::{debug, warn};

use super::provider::{
    AuthCredentials, AuthError, AuthProvider, AuthResult, CredentialType, UserIdentity,
};

/// OIDC provider configuration from TOML.
#[derive(Debug, Deserialize, Clone)]
pub struct OidcConfig {
    /// OIDC discovery URL (e.g., `https://keycloak.example.com/realms/runifi/.well-known/openid-configuration`).
    pub discovery_url: String,
    /// OAuth 2.0 client ID.
    pub client_id: String,
    /// OAuth 2.0 client secret.
    pub client_secret: String,
    /// OAuth 2.0 scopes. Default: `["openid", "profile", "email"]`.
    #[serde(default = "default_oidc_scopes")]
    pub scopes: Vec<String>,
    /// Redirect URI for the callback. Auto-detected if not set.
    pub redirect_uri: Option<String>,
    /// Claim mapping configuration.
    #[serde(default)]
    pub claim_mapping: OidcClaimMappingConfig,
}

/// Configures how OIDC claims map to [`UserIdentity`] fields.
#[derive(Debug, Deserialize, Clone)]
pub struct OidcClaimMappingConfig {
    #[serde(default = "default_username_claim")]
    pub username_claim: String,
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
    #[serde(default = "default_email_claim")]
    pub email_claim: String,
    #[serde(default = "default_name_claim")]
    pub display_name_claim: String,
}

impl Default for OidcClaimMappingConfig {
    fn default() -> Self {
        Self {
            username_claim: default_username_claim(),
            groups_claim: default_groups_claim(),
            email_claim: default_email_claim(),
            display_name_claim: default_name_claim(),
        }
    }
}

fn default_oidc_scopes() -> Vec<String> {
    vec!["openid".into(), "profile".into(), "email".into()]
}

fn default_username_claim() -> String {
    "preferred_username".into()
}
fn default_groups_claim() -> String {
    "groups".into()
}
fn default_email_claim() -> String {
    "email".into()
}
fn default_name_claim() -> String {
    "name".into()
}

/// In-flight OIDC authorization state for CSRF protection.
#[derive(Debug, Clone)]
pub struct PendingOidcAuth {
    /// PKCE code verifier.
    pub code_verifier: String,
    /// When this state was created (for TTL expiry).
    pub created_at: std::time::Instant,
}

/// OIDC authentication provider.
///
/// Handles Authorization Code + PKCE flow. The actual HTTP calls to the IdP
/// are delegated to endpoint handlers in `runifi-api`.
#[derive(Debug)]
pub struct OidcAuthProvider {
    config: OidcConfig,
    /// In-flight authorization states keyed by the random `state` parameter.
    pending_states: Arc<DashMap<String, PendingOidcAuth>>,
    /// Cached OIDC discovery document (parsed endpoints).
    discovery: Arc<tokio::sync::RwLock<Option<OidcDiscovery>>>,
}

/// Parsed fields from the OIDC discovery document.
#[derive(Debug, Clone)]
pub struct OidcDiscovery {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub jwks_uri: String,
    pub end_session_endpoint: Option<String>,
    pub issuer: String,
}

impl OidcAuthProvider {
    pub fn new(config: OidcConfig) -> Self {
        Self {
            config,
            pending_states: Arc::new(DashMap::new()),
            discovery: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Get the OIDC configuration.
    pub fn config(&self) -> &OidcConfig {
        &self.config
    }

    /// Store a pending auth state for the authorization code flow.
    pub fn store_pending_state(&self, state: String, auth: PendingOidcAuth) {
        self.pending_states.insert(state, auth);
    }

    /// Retrieve and remove a pending auth state.
    pub fn take_pending_state(&self, state: &str) -> Option<PendingOidcAuth> {
        self.pending_states.remove(state).map(|(_, v)| v)
    }

    /// Clean up expired pending states (older than 5 minutes).
    pub fn cleanup_expired_states(&self) {
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(300);
        self.pending_states.retain(|_, v| v.created_at > cutoff);
    }

    /// Set the discovery document (called after fetching from the IdP).
    pub async fn set_discovery(&self, discovery: OidcDiscovery) {
        let mut guard = self.discovery.write().await;
        *guard = Some(discovery);
    }

    /// Get the cached discovery document.
    pub async fn get_discovery(&self) -> Option<OidcDiscovery> {
        self.discovery.read().await.clone()
    }

    /// Extract identity from OIDC ID token claims.
    pub fn extract_identity(&self, claims: &HashMap<String, serde_json::Value>) -> UserIdentity {
        let mapping = &self.config.claim_mapping;

        let username = claims
            .get(&mapping.username_claim)
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let display_name = claims
            .get(&mapping.display_name_claim)
            .and_then(|v| v.as_str())
            .map(String::from);

        let email = claims
            .get(&mapping.email_claim)
            .and_then(|v| v.as_str())
            .map(String::from);

        let groups = claims
            .get(&mapping.groups_claim)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        UserIdentity {
            username,
            display_name,
            groups,
            provider: "oidc".into(),
            email,
            expires_at: None,
            provider_data: None,
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for OidcAuthProvider {
    fn name(&self) -> &str {
        "oidc"
    }

    fn supported_credentials(&self) -> &[CredentialType] {
        &[CredentialType::OidcCallback]
    }

    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        match credentials {
            AuthCredentials::OidcCallback { code, state } => {
                // Validate the state parameter against pending states.
                let _pending = match self.take_pending_state(state) {
                    Some(p) => {
                        // Check TTL (5 minutes).
                        if p.created_at.elapsed() > std::time::Duration::from_secs(300) {
                            warn!("OIDC state expired");
                            return AuthResult::Failed(AuthError::TokenExpired);
                        }
                        p
                    }
                    None => {
                        warn!(state = %state, "Unknown OIDC state parameter");
                        return AuthResult::Failed(AuthError::InvalidCredentials);
                    }
                };

                debug!(
                    code_len = code.len(),
                    "OIDC callback received, token exchange required"
                );

                // The actual token exchange (HTTP call to IdP) is handled by
                // the OIDC route handler in runifi-api, which calls
                // extract_identity() with the decoded claims.
                // This trait method validates state only.
                AuthResult::Failed(AuthError::ProviderUnavailable(
                    "Token exchange must be performed by the API layer".into(),
                ))
            }
            _ => AuthResult::Unsupported,
        }
    }

    async fn logout(&self, _identity: &UserIdentity) -> Result<Option<String>, AuthError> {
        // Return the IdP end-session endpoint if available.
        if let Some(discovery) = self.get_discovery().await
            && let Some(endpoint) = discovery.end_session_endpoint
        {
            return Ok(Some(endpoint));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OidcConfig {
        OidcConfig {
            discovery_url: "https://idp.example.com/.well-known/openid-configuration".into(),
            client_id: "runifi-test".into(),
            client_secret: "secret".into(),
            scopes: default_oidc_scopes(),
            redirect_uri: Some("https://runifi.example.com/api/v1/auth/oidc/callback".into()),
            claim_mapping: OidcClaimMappingConfig::default(),
        }
    }

    #[test]
    fn extract_identity_from_claims() {
        let provider = OidcAuthProvider::new(test_config());

        let claims: HashMap<String, serde_json::Value> = serde_json::from_str(
            r#"{
                "preferred_username": "jane.doe",
                "name": "Jane Doe",
                "email": "jane@example.com",
                "groups": ["admins", "developers"]
            }"#,
        )
        .unwrap();

        let identity = provider.extract_identity(&claims);
        assert_eq!(identity.username, "jane.doe");
        assert_eq!(identity.display_name.as_deref(), Some("Jane Doe"));
        assert_eq!(identity.email.as_deref(), Some("jane@example.com"));
        assert_eq!(identity.groups, vec!["admins", "developers"]);
        assert_eq!(identity.provider, "oidc");
    }

    #[test]
    fn extract_identity_missing_claims() {
        let provider = OidcAuthProvider::new(test_config());
        let claims: HashMap<String, serde_json::Value> = HashMap::new();

        let identity = provider.extract_identity(&claims);
        assert_eq!(identity.username, "unknown");
        assert!(identity.display_name.is_none());
        assert!(identity.groups.is_empty());
    }

    #[test]
    fn pending_state_lifecycle() {
        let provider = OidcAuthProvider::new(test_config());

        provider.store_pending_state(
            "state-123".into(),
            PendingOidcAuth {
                code_verifier: "verifier".into(),
                created_at: std::time::Instant::now(),
            },
        );

        assert!(provider.take_pending_state("state-123").is_some());
        assert!(provider.take_pending_state("state-123").is_none()); // Already consumed
    }

    #[test]
    fn cleanup_expired_states() {
        let provider = OidcAuthProvider::new(test_config());

        // Insert an expired state
        provider.store_pending_state(
            "old-state".into(),
            PendingOidcAuth {
                code_verifier: "v".into(),
                created_at: std::time::Instant::now() - std::time::Duration::from_secs(600),
            },
        );

        // Insert a fresh state
        provider.store_pending_state(
            "new-state".into(),
            PendingOidcAuth {
                code_verifier: "v".into(),
                created_at: std::time::Instant::now(),
            },
        );

        provider.cleanup_expired_states();

        assert!(provider.take_pending_state("old-state").is_none());
        assert!(provider.take_pending_state("new-state").is_some());
    }

    #[test]
    fn default_claim_mapping() {
        let mapping = OidcClaimMappingConfig::default();
        assert_eq!(mapping.username_claim, "preferred_username");
        assert_eq!(mapping.groups_claim, "groups");
        assert_eq!(mapping.email_claim, "email");
        assert_eq!(mapping.display_name_claim, "name");
    }
}
