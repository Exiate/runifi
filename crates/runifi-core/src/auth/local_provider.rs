//! Local authentication provider — wraps the existing [`UserStore`] and [`JwtConfig`].

use std::sync::Arc;

use super::jwt::JwtConfig;
use super::provider::{
    AuthCredentials, AuthError, AuthProvider, AuthResult, CredentialType, UserIdentity,
};
use super::store::UserStore;

/// Authenticates users against the local [`UserStore`] (password) and validates
/// JWTs via [`JwtConfig`] (bearer token).
#[derive(Debug)]
pub struct LocalAuthProvider {
    user_store: Arc<UserStore>,
    jwt_config: Arc<JwtConfig>,
}

impl LocalAuthProvider {
    pub fn new(user_store: Arc<UserStore>, jwt_config: Arc<JwtConfig>) -> Self {
        Self {
            user_store,
            jwt_config,
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for LocalAuthProvider {
    fn name(&self) -> &str {
        "local"
    }

    fn supported_credentials(&self) -> &[CredentialType] {
        &[CredentialType::Password, CredentialType::BearerToken]
    }

    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        match credentials {
            AuthCredentials::Password { username, password } => {
                match self.user_store.authenticate(username, password) {
                    Ok(user) => AuthResult::Authenticated(UserIdentity {
                        username: user.username,
                        display_name: None,
                        groups: vec![],
                        provider: "local".into(),
                        email: None,
                        expires_at: None,
                        provider_data: Some(user.id.to_string()),
                    }),
                    Err(super::store::UserStoreError::AccountDisabled(u)) => {
                        AuthResult::Failed(AuthError::AccountDisabled(u))
                    }
                    Err(_) => AuthResult::Failed(AuthError::InvalidCredentials),
                }
            }
            AuthCredentials::BearerToken(token) => match self.jwt_config.validate_token(token) {
                Ok(claims) => {
                    if self.user_store.is_token_revoked(&claims.jti) {
                        return AuthResult::Failed(AuthError::TokenRevoked);
                    }
                    AuthResult::Authenticated(UserIdentity {
                        username: claims.username,
                        display_name: None,
                        groups: vec![],
                        provider: "local".into(),
                        email: None,
                        expires_at: None,
                        provider_data: Some(claims.sub),
                    })
                }
                Err(_) => AuthResult::Failed(AuthError::InvalidCredentials),
            },
            _ => AuthResult::Unsupported,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<UserStore>, Arc<JwtConfig>) {
        let store = Arc::new(UserStore::new());
        store
            .create_user("admin".to_string(), "password123")
            .unwrap();
        let jwt = Arc::new(JwtConfig::new("test-secret-at-least-32-chars!!", 3600));
        (store, jwt)
    }

    #[tokio::test]
    async fn password_auth_success() {
        let (store, jwt) = setup();
        let provider = LocalAuthProvider::new(store, jwt);

        let creds = AuthCredentials::Password {
            username: "admin".into(),
            password: "password123".into(),
        };

        match provider.authenticate(&creds).await {
            AuthResult::Authenticated(id) => {
                assert_eq!(id.username, "admin");
                assert_eq!(id.provider, "local");
            }
            _ => panic!("Expected Authenticated"),
        }
    }

    #[tokio::test]
    async fn password_auth_wrong_password() {
        let (store, jwt) = setup();
        let provider = LocalAuthProvider::new(store, jwt);

        let creds = AuthCredentials::Password {
            username: "admin".into(),
            password: "wrong".into(),
        };

        match provider.authenticate(&creds).await {
            AuthResult::Failed(AuthError::InvalidCredentials) => {}
            _ => panic!("Expected Failed(InvalidCredentials)"),
        }
    }

    #[tokio::test]
    async fn bearer_auth_success() {
        let (store, jwt) = setup();
        let user = store.get_user_by_username("admin").unwrap();
        let token = jwt.generate_token(user.id, "admin").unwrap();

        let provider = LocalAuthProvider::new(store, jwt);
        let creds = AuthCredentials::BearerToken(token);

        match provider.authenticate(&creds).await {
            AuthResult::Authenticated(id) => assert_eq!(id.username, "admin"),
            _ => panic!("Expected Authenticated"),
        }
    }

    #[tokio::test]
    async fn bearer_auth_revoked_token() {
        let (store, jwt) = setup();
        let user = store.get_user_by_username("admin").unwrap();
        let token = jwt.generate_token(user.id, "admin").unwrap();
        let claims = jwt.validate_token(&token).unwrap();
        store.revoke_token(&claims.jti);

        let provider = LocalAuthProvider::new(store, jwt);
        let creds = AuthCredentials::BearerToken(token);

        match provider.authenticate(&creds).await {
            AuthResult::Failed(AuthError::TokenRevoked) => {}
            _ => panic!("Expected Failed(TokenRevoked)"),
        }
    }

    #[tokio::test]
    async fn unsupported_credential_type() {
        let (store, jwt) = setup();
        let provider = LocalAuthProvider::new(store, jwt);

        let creds = AuthCredentials::ApiKey("some-key".into());
        match provider.authenticate(&creds).await {
            AuthResult::Unsupported => {}
            _ => panic!("Expected Unsupported"),
        }
    }
}
