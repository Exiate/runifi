//! Ordered chain of authentication providers.
//!
//! Tries each provider in sequence until one returns [`AuthResult::Authenticated`]
//! or [`AuthResult::Failed`]. Providers that return [`AuthResult::Unsupported`]
//! are skipped.

use tracing::debug;

use super::provider::{AuthCredentials, AuthError, AuthProvider, AuthResult, UserIdentity};

/// An ordered chain of auth providers that tries each until one succeeds.
#[derive(Debug)]
pub struct AuthProviderChain {
    providers: Vec<Box<dyn AuthProvider>>,
}

impl AuthProviderChain {
    pub fn new(providers: Vec<Box<dyn AuthProvider>>) -> Self {
        Self { providers }
    }

    /// Authenticate by trying each provider in order.
    ///
    /// - [`AuthResult::Authenticated`]: stop and return success.
    /// - [`AuthResult::Failed`]: stop and return failure (provider recognized the
    ///   credential but rejected it).
    /// - [`AuthResult::Unsupported`]: skip to next provider.
    ///
    /// If no provider handles the credential, returns [`AuthResult::Unsupported`].
    pub async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        let cred_type = credentials.credential_type();

        for provider in &self.providers {
            // Fast filter: skip providers that don't handle this credential type.
            if !provider.supported_credentials().contains(&cred_type) {
                continue;
            }

            debug!(provider = provider.name(), "Trying auth provider");

            match provider.authenticate(credentials).await {
                AuthResult::Authenticated(identity) => {
                    debug!(
                        provider = provider.name(),
                        username = %identity.username,
                        "Authentication succeeded"
                    );
                    return AuthResult::Authenticated(identity);
                }
                AuthResult::Failed(err) => {
                    debug!(
                        provider = provider.name(),
                        error = %err,
                        "Authentication failed"
                    );
                    return AuthResult::Failed(err);
                }
                AuthResult::Unsupported => {
                    continue;
                }
            }
        }

        AuthResult::Unsupported
    }

    /// Refresh authentication via the named provider.
    pub async fn refresh(&self, provider_name: &str, token: &str) -> AuthResult {
        for provider in &self.providers {
            if provider.name() == provider_name {
                return provider.refresh(token).await;
            }
        }
        AuthResult::Unsupported
    }

    /// Logout via the named provider.
    pub async fn logout(&self, identity: &UserIdentity) -> Result<Option<String>, AuthError> {
        for provider in &self.providers {
            if provider.name() == identity.provider {
                return provider.logout(identity).await;
            }
        }
        Ok(None)
    }

    /// Returns the names of all providers in the chain.
    pub fn provider_names(&self) -> Vec<&str> {
        self.providers.iter().map(|p| p.name()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::provider::CredentialType;

    /// A test provider that always succeeds for password auth.
    #[derive(Debug)]
    struct AcceptAllProvider;

    #[async_trait::async_trait]
    impl AuthProvider for AcceptAllProvider {
        fn name(&self) -> &str {
            "accept-all"
        }
        fn supported_credentials(&self) -> &[CredentialType] {
            &[CredentialType::Password]
        }
        async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
            if let AuthCredentials::Password { username, .. } = credentials {
                AuthResult::Authenticated(UserIdentity {
                    username: username.clone(),
                    display_name: None,
                    groups: vec![],
                    provider: "accept-all".into(),
                    email: None,
                    expires_at: None,
                    provider_data: None,
                })
            } else {
                AuthResult::Unsupported
            }
        }
    }

    /// A test provider that always rejects.
    #[derive(Debug)]
    struct RejectAllProvider;

    #[async_trait::async_trait]
    impl AuthProvider for RejectAllProvider {
        fn name(&self) -> &str {
            "reject-all"
        }
        fn supported_credentials(&self) -> &[CredentialType] {
            &[CredentialType::Password]
        }
        async fn authenticate(&self, _credentials: &AuthCredentials) -> AuthResult {
            AuthResult::Failed(AuthError::InvalidCredentials)
        }
    }

    /// A test provider that returns Unsupported.
    #[derive(Debug)]
    struct SkipProvider;

    #[async_trait::async_trait]
    impl AuthProvider for SkipProvider {
        fn name(&self) -> &str {
            "skip"
        }
        fn supported_credentials(&self) -> &[CredentialType] {
            &[CredentialType::Password]
        }
        async fn authenticate(&self, _credentials: &AuthCredentials) -> AuthResult {
            AuthResult::Unsupported
        }
    }

    #[tokio::test]
    async fn chain_returns_first_success() {
        let chain =
            AuthProviderChain::new(vec![Box::new(SkipProvider), Box::new(AcceptAllProvider)]);
        let creds = AuthCredentials::Password {
            username: "alice".into(),
            password: "pass".into(),
        };
        match chain.authenticate(&creds).await {
            AuthResult::Authenticated(id) => assert_eq!(id.username, "alice"),
            _ => panic!("Expected Authenticated"),
        }
    }

    #[tokio::test]
    async fn chain_stops_on_failure() {
        let chain = AuthProviderChain::new(vec![
            Box::new(RejectAllProvider),
            Box::new(AcceptAllProvider),
        ]);
        let creds = AuthCredentials::Password {
            username: "alice".into(),
            password: "pass".into(),
        };
        match chain.authenticate(&creds).await {
            AuthResult::Failed(_) => {} // Expected
            _ => panic!("Expected Failed"),
        }
    }

    #[tokio::test]
    async fn chain_unsupported_when_no_providers_match() {
        let chain = AuthProviderChain::new(vec![Box::new(SkipProvider)]);
        let creds = AuthCredentials::Password {
            username: "alice".into(),
            password: "pass".into(),
        };
        match chain.authenticate(&creds).await {
            AuthResult::Unsupported => {} // Expected
            _ => panic!("Expected Unsupported"),
        }
    }

    #[tokio::test]
    async fn chain_skips_providers_with_wrong_credential_type() {
        let chain = AuthProviderChain::new(vec![Box::new(AcceptAllProvider)]);
        // AcceptAllProvider only supports Password, so ApiKey should be unsupported.
        let creds = AuthCredentials::ApiKey("some-key".into());
        match chain.authenticate(&creds).await {
            AuthResult::Unsupported => {} // Expected
            _ => panic!("Expected Unsupported"),
        }
    }

    #[test]
    fn provider_names() {
        let chain = AuthProviderChain::new(vec![
            Box::new(AcceptAllProvider),
            Box::new(RejectAllProvider),
        ]);
        assert_eq!(chain.provider_names(), vec!["accept-all", "reject-all"]);
    }
}
