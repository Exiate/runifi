//! Unified session management for all authentication providers.
//!
//! Regardless of which provider authenticated the user, a RuniFi session JWT
//! is issued for subsequent requests. This module manages session creation,
//! validation, and revocation.

use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use super::jwt::JwtConfig;
use super::provider::{AuthError, UserIdentity};
use super::store::UserStore;

/// A session token issued after successful authentication.
pub struct SessionToken {
    /// The JWT token string.
    pub token: String,
    /// Seconds until expiration.
    pub expires_in: i64,
}

/// Manages RuniFi sessions across all authentication providers.
#[derive(Debug)]
pub struct SessionManager {
    jwt_config: Arc<JwtConfig>,
    user_store: Arc<UserStore>,
    /// Maximum concurrent sessions per user. `None` = unlimited.
    max_sessions_per_user: Option<usize>,
    /// Active sessions: username → list of JTIs.
    active_sessions: DashMap<String, Vec<String>>,
    /// JWT expiry in seconds (for the response).
    expiry_secs: i64,
}

impl SessionManager {
    pub fn new(
        jwt_config: Arc<JwtConfig>,
        user_store: Arc<UserStore>,
        max_sessions_per_user: Option<usize>,
        expiry_secs: i64,
    ) -> Self {
        Self {
            jwt_config,
            user_store,
            max_sessions_per_user,
            active_sessions: DashMap::new(),
            expiry_secs,
        }
    }

    /// Create a session for an authenticated identity.
    ///
    /// Issues a RuniFi JWT regardless of which provider authenticated the user.
    /// For local users, the existing user ID is used as the JWT subject.
    /// For external users, a synthetic UUID is generated.
    pub fn create_session(&self, identity: &UserIdentity) -> Result<SessionToken, AuthError> {
        // Enforce concurrent session limits.
        if let Some(max) = self.max_sessions_per_user {
            let mut sessions = self
                .active_sessions
                .entry(identity.username.clone())
                .or_default();

            // Clean up revoked sessions from the list.
            sessions.retain(|jti| !self.user_store.is_token_revoked(jti));

            if sessions.len() >= max {
                return Err(AuthError::AccountDisabled(format!(
                    "Maximum {} concurrent sessions reached",
                    max
                )));
            }
        }

        // Resolve user ID: use local user ID if available, otherwise generate one.
        let user_id = identity
            .provider_data
            .as_ref()
            .and_then(|id| id.parse::<Uuid>().ok())
            .unwrap_or_else(Uuid::now_v7);

        let token = self
            .jwt_config
            .generate_token(user_id, &identity.username)
            .map_err(|e| AuthError::ProviderUnavailable(format!("JWT generation failed: {}", e)))?;

        // Track the session JTI.
        let claims = self
            .jwt_config
            .validate_token(&token)
            .map_err(|e| AuthError::ProviderUnavailable(format!("JWT validation failed: {}", e)))?;

        self.active_sessions
            .entry(identity.username.clone())
            .or_default()
            .push(claims.jti);

        Ok(SessionToken {
            token,
            expires_in: self.expiry_secs,
        })
    }

    /// Revoke a session by its JTI.
    pub fn revoke_session(&self, jti: &str) {
        self.user_store.revoke_token(jti);
    }

    /// Revoke all sessions for a user.
    pub fn revoke_all_sessions(&self, username: &str) {
        if let Some((_, jtis)) = self.active_sessions.remove(username) {
            for jti in jtis {
                self.user_store.revoke_token(&jti);
            }
        }
    }

    /// Get the number of active sessions for a user.
    pub fn session_count(&self, username: &str) -> usize {
        self.active_sessions
            .get(username)
            .map(|v| {
                v.iter()
                    .filter(|jti| !self.user_store.is_token_revoked(jti))
                    .count()
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<UserStore>, Arc<JwtConfig>) {
        let store = Arc::new(UserStore::new());
        let jwt = Arc::new(JwtConfig::new("test-secret-at-least-32-chars!!", 3600));
        (store, jwt)
    }

    fn test_identity() -> UserIdentity {
        UserIdentity {
            username: "alice".into(),
            display_name: None,
            groups: vec![],
            provider: "test".into(),
            email: None,
            expires_at: None,
            provider_data: None,
        }
    }

    #[test]
    fn create_session_returns_token() {
        let (store, jwt) = setup();
        let manager = SessionManager::new(jwt.clone(), store, None, 3600);

        let token = manager.create_session(&test_identity()).unwrap();
        assert!(!token.token.is_empty());
        assert_eq!(token.expires_in, 3600);

        // Token should be valid.
        jwt.validate_token(&token.token).unwrap();
    }

    #[test]
    fn concurrent_session_limit() {
        let (store, jwt) = setup();
        let manager = SessionManager::new(jwt, store, Some(2), 3600);

        let identity = test_identity();
        manager.create_session(&identity).unwrap();
        manager.create_session(&identity).unwrap();

        // Third session should fail.
        let result = manager.create_session(&identity);
        assert!(result.is_err());
    }

    #[test]
    fn revoke_session_frees_slot() {
        let (store, jwt) = setup();
        let manager = SessionManager::new(jwt.clone(), store, Some(1), 3600);

        let identity = test_identity();
        let token = manager.create_session(&identity).unwrap();

        // Get JTI from the token.
        let claims = jwt.validate_token(&token.token).unwrap();
        manager.revoke_session(&claims.jti);

        // Should be able to create another session now.
        manager.create_session(&identity).unwrap();
    }

    #[test]
    fn session_count() {
        let (store, jwt) = setup();
        let manager = SessionManager::new(jwt, store, None, 3600);

        let identity = test_identity();
        assert_eq!(manager.session_count("alice"), 0);

        manager.create_session(&identity).unwrap();
        assert_eq!(manager.session_count("alice"), 1);

        manager.create_session(&identity).unwrap();
        assert_eq!(manager.session_count("alice"), 2);
    }

    #[test]
    fn revoke_all_sessions() {
        let (store, jwt) = setup();
        let manager = SessionManager::new(jwt, store, None, 3600);

        let identity = test_identity();
        manager.create_session(&identity).unwrap();
        manager.create_session(&identity).unwrap();
        assert_eq!(manager.session_count("alice"), 2);

        manager.revoke_all_sessions("alice");
        assert_eq!(manager.session_count("alice"), 0);
    }
}
