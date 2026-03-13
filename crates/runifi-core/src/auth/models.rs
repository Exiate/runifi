//! User and UserGroup domain models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A local user identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier.
    pub id: Uuid,
    /// Login username (unique, case-sensitive).
    pub username: String,
    /// Argon2 password hash (never serialized to API responses).
    #[serde(skip_serializing)]
    pub password_hash: String,
    /// Whether the user account is enabled.
    pub enabled: bool,
    /// Timestamp when the user was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the last update.
    pub updated_at: DateTime<Utc>,
}

impl User {
    /// Create a new user with a pre-hashed password.
    pub fn new(username: String, password_hash: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            username,
            password_hash,
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }
}

/// A group of users.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserGroup {
    /// Unique group identifier.
    pub id: Uuid,
    /// Display name for the group.
    pub name: String,
    /// User IDs that belong to this group.
    pub members: Vec<Uuid>,
    /// Timestamp when the group was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the last update.
    pub updated_at: DateTime<Utc>,
}

impl UserGroup {
    /// Create a new empty group.
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            name,
            members: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_new_sets_defaults() {
        let user = User::new("admin".to_string(), "hash".to_string());
        assert_eq!(user.username, "admin");
        assert_eq!(user.password_hash, "hash");
        assert!(user.enabled);
        assert!(user.created_at <= Utc::now());
    }

    #[test]
    fn user_group_new_is_empty() {
        let group = UserGroup::new("operators".to_string());
        assert_eq!(group.name, "operators");
        assert!(group.members.is_empty());
    }

    #[test]
    fn user_serialization_skips_password() {
        let user = User::new("alice".to_string(), "secret_hash".to_string());
        let json = serde_json::to_string(&user).unwrap();
        assert!(!json.contains("secret_hash"));
        assert!(json.contains("alice"));
    }
}
