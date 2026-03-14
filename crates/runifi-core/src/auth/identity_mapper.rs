//! Maps external identity provider groups to RuniFi RBAC roles.

use super::provider::UserIdentity;

/// Configurable mapping from identity provider groups to RuniFi roles.
#[derive(Debug, Clone)]
pub struct IdentityMapper {
    /// Group name -> role string mapping. First match wins (highest privilege).
    group_to_role: Vec<(String, String)>,
    /// Default role if no group mapping matches.
    default_role: String,
}

impl IdentityMapper {
    /// Create a new identity mapper with explicit group-to-role mappings.
    ///
    /// The `mappings` are checked in order. The first matching group determines
    /// the role. If no group matches, `default_role` is used.
    pub fn new(mappings: Vec<(String, String)>, default_role: String) -> Self {
        Self {
            group_to_role: mappings,
            default_role,
        }
    }

    /// Resolve the role string for a given identity.
    ///
    /// Checks the identity's groups against the configured mappings in order.
    /// Returns the first matching role, or the default role if no match.
    pub fn resolve_role(&self, identity: &UserIdentity) -> &str {
        for (group, role) in &self.group_to_role {
            if identity.groups.iter().any(|g| g == group) {
                return role;
            }
        }
        &self.default_role
    }
}

impl Default for IdentityMapper {
    fn default() -> Self {
        Self {
            group_to_role: Vec::new(),
            default_role: "viewer".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(groups: Vec<&str>) -> UserIdentity {
        UserIdentity {
            username: "test".into(),
            display_name: None,
            groups: groups.into_iter().map(String::from).collect(),
            provider: "test".into(),
            email: None,
            expires_at: None,
            provider_data: None,
        }
    }

    #[test]
    fn resolves_first_matching_group() {
        let mapper = IdentityMapper::new(
            vec![
                ("admins".into(), "admin".into()),
                ("operators".into(), "operator".into()),
            ],
            "viewer".into(),
        );

        let identity = test_identity(vec!["operators", "admins"]);
        // "admins" mapping comes first in the mapping list, so it wins
        assert_eq!(mapper.resolve_role(&identity), "admin");
    }

    #[test]
    fn falls_back_to_default() {
        let mapper = IdentityMapper::new(vec![("admins".into(), "admin".into())], "viewer".into());

        let identity = test_identity(vec!["users"]);
        assert_eq!(mapper.resolve_role(&identity), "viewer");
    }

    #[test]
    fn empty_groups_get_default() {
        let mapper = IdentityMapper::new(vec![("admins".into(), "admin".into())], "viewer".into());

        let identity = test_identity(vec![]);
        assert_eq!(mapper.resolve_role(&identity), "viewer");
    }

    #[test]
    fn default_mapper_gives_viewer() {
        let mapper = IdentityMapper::default();
        let identity = test_identity(vec!["anything"]);
        assert_eq!(mapper.resolve_role(&identity), "viewer");
    }
}
