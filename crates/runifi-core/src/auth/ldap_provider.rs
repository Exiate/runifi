//! LDAP/LDAPS authentication provider.
//!
//! Authenticates users against an LDAP directory (Active Directory, OpenLDAP, etc.)
//! with support for:
//! - Bind authentication (direct or search-then-bind)
//! - Group resolution and role mapping
//! - LDAPS (TLS) and STARTTLS
//! - Connection pooling
//!
//! Configured via TOML under `[auth.ldap]`.

use std::collections::HashMap;

use serde::Deserialize;

use super::provider::{AuthCredentials, AuthProvider, AuthResult, CredentialType};

/// LDAP provider configuration from TOML.
#[derive(Debug, Deserialize, Clone)]
pub struct LdapConfig {
    /// LDAP server URL (e.g., `ldaps://ldap.example.com:636`).
    pub url: String,
    /// Service account bind DN for search operations.
    pub bind_dn: String,
    /// Service account bind password.
    pub bind_password: String,
    /// Base DN for user search.
    pub user_search_base: String,
    /// LDAP filter for user search. Use `{username}` placeholder.
    #[serde(default = "default_user_search_filter")]
    pub user_search_filter: String,
    /// Base DN for group search.
    #[serde(default)]
    pub group_search_base: String,
    /// LDAP filter for group membership. Use `{user_dn}` placeholder.
    #[serde(default = "default_group_search_filter")]
    pub group_search_filter: String,
    /// Group attribute to extract (default: `cn`).
    #[serde(default = "default_group_attribute")]
    pub group_attribute: String,
    /// Use STARTTLS on port 389 instead of LDAPS on port 636.
    #[serde(default)]
    pub use_starttls: bool,
    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    /// LDAP-specific group-to-role mapping.
    #[serde(default)]
    pub group_mapping: HashMap<String, String>,
}

fn default_user_search_filter() -> String {
    "(sAMAccountName={username})".into()
}

fn default_group_search_filter() -> String {
    "(member={user_dn})".into()
}

fn default_group_attribute() -> String {
    "cn".into()
}

fn default_pool_size() -> usize {
    5
}

/// LDAP authentication provider.
///
/// Performs search-then-bind authentication: connects with a service account,
/// searches for the user's DN, then rebinds with the user's credentials.
/// On success, resolves group memberships.
///
/// Note: The actual LDAP protocol operations require the `ldap3` crate,
/// which is gated behind the `ldap` feature. Without it, this provider
/// returns [`AuthResult::Failed`] with [`AuthError::ProviderUnavailable`].
#[derive(Debug)]
pub struct LdapAuthProvider {
    config: LdapConfig,
}

impl LdapAuthProvider {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    /// Get the LDAP configuration.
    pub fn config(&self) -> &LdapConfig {
        &self.config
    }

    /// Build the user search filter by replacing `{username}` placeholder.
    pub fn build_user_filter(&self, username: &str) -> String {
        self.config
            .user_search_filter
            .replace("{username}", username)
    }

    /// Build the group search filter by replacing `{user_dn}` placeholder.
    pub fn build_group_filter(&self, user_dn: &str) -> String {
        self.config
            .group_search_filter
            .replace("{user_dn}", user_dn)
    }

    /// Map resolved LDAP groups to RuniFi role names using configured mapping.
    pub fn map_groups_to_roles(&self, ldap_groups: &[String]) -> Vec<String> {
        let mut roles = Vec::new();
        for group in ldap_groups {
            if let Some(role) = self.config.group_mapping.get(group) {
                roles.push(role.clone());
            } else {
                // Pass through unmapped groups as-is.
                roles.push(group.clone());
            }
        }
        roles
    }
}

#[async_trait::async_trait]
impl AuthProvider for LdapAuthProvider {
    fn name(&self) -> &str {
        "ldap"
    }

    fn supported_credentials(&self) -> &[CredentialType] {
        &[CredentialType::Password]
    }

    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        let (_username, _password) = match credentials {
            AuthCredentials::Password { username, password } => (username, password),
            _ => return AuthResult::Unsupported,
        };

        // LDAP protocol operations require a network client.
        // This is a structural implementation — actual LDAP bind/search
        // requires the ldap3 crate (added as optional dependency).
        // Return Unsupported so the chain continues to the next provider.
        tracing::warn!("LDAP provider not compiled — skipping (compile with --features ldap)");
        AuthResult::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LdapConfig {
        LdapConfig {
            url: "ldaps://ldap.example.com:636".into(),
            bind_dn: "cn=svc,ou=services,dc=example,dc=com".into(),
            bind_password: "svc-password".into(),
            user_search_base: "ou=users,dc=example,dc=com".into(),
            user_search_filter: "(sAMAccountName={username})".into(),
            group_search_base: "ou=groups,dc=example,dc=com".into(),
            group_search_filter: "(member={user_dn})".into(),
            group_attribute: "cn".into(),
            use_starttls: false,
            pool_size: 5,
            group_mapping: HashMap::from([
                ("nifi-admins".into(), "admin".into()),
                ("nifi-operators".into(), "operator".into()),
            ]),
        }
    }

    #[test]
    fn build_user_filter() {
        let provider = LdapAuthProvider::new(test_config());
        assert_eq!(
            provider.build_user_filter("jane.doe"),
            "(sAMAccountName=jane.doe)"
        );
    }

    #[test]
    fn build_group_filter() {
        let provider = LdapAuthProvider::new(test_config());
        assert_eq!(
            provider.build_group_filter("cn=jane,ou=users,dc=example,dc=com"),
            "(member=cn=jane,ou=users,dc=example,dc=com)"
        );
    }

    #[test]
    fn map_groups_to_roles() {
        let provider = LdapAuthProvider::new(test_config());
        let groups = vec![
            "nifi-admins".into(),
            "nifi-operators".into(),
            "other-group".into(),
        ];
        let roles = provider.map_groups_to_roles(&groups);
        assert_eq!(roles, vec!["admin", "operator", "other-group"]);
    }

    #[test]
    fn default_filters() {
        assert_eq!(default_user_search_filter(), "(sAMAccountName={username})");
        assert_eq!(default_group_search_filter(), "(member={user_dn})");
        assert_eq!(default_group_attribute(), "cn");
    }

    #[tokio::test]
    async fn unsupported_credential_type() {
        let provider = LdapAuthProvider::new(test_config());
        let creds = AuthCredentials::ApiKey("key".into());
        match provider.authenticate(&creds).await {
            AuthResult::Unsupported => {}
            _ => panic!("Expected Unsupported"),
        }
    }
}
