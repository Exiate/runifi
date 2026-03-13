//! In-memory user and group store.
//!
//! Provides thread-safe storage for users and groups. Uses `DashMap` for
//! concurrent access without coarse-grained locking.

use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use super::models::{User, UserGroup};
use super::password;

/// Errors returned by the user store.
#[derive(Debug, thiserror::Error)]
pub enum UserStoreError {
    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Username already exists: {0}")]
    DuplicateUsername(String),

    #[error("Group not found: {0}")]
    GroupNotFound(String),

    #[error("Group name already exists: {0}")]
    DuplicateGroupName(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Password hashing failed: {0}")]
    HashError(String),

    #[error("Account disabled: {0}")]
    AccountDisabled(String),
}

/// Thread-safe in-memory store for users and user groups.
#[derive(Debug)]
pub struct UserStore {
    /// Users indexed by ID.
    users: DashMap<Uuid, User>,
    /// Username -> User ID index for fast lookup.
    username_index: DashMap<String, Uuid>,
    /// Groups indexed by ID.
    groups: DashMap<Uuid, UserGroup>,
    /// Group name -> Group ID index.
    group_name_index: DashMap<String, Uuid>,
    /// Revoked JWT token IDs (jti) — tokens that have been logged out.
    revoked_tokens: DashMap<String, ()>,
}

impl UserStore {
    /// Create a new empty user store.
    pub fn new() -> Self {
        Self {
            users: DashMap::new(),
            username_index: DashMap::new(),
            groups: DashMap::new(),
            group_name_index: DashMap::new(),
            revoked_tokens: DashMap::new(),
        }
    }

    // -----------------------------------------------------------------------
    // User operations
    // -----------------------------------------------------------------------

    /// Create a new user with a plaintext password. Returns the created user.
    pub fn create_user(
        &self,
        username: String,
        plaintext_password: &str,
    ) -> Result<User, UserStoreError> {
        if self.username_index.contains_key(&username) {
            return Err(UserStoreError::DuplicateUsername(username));
        }

        let hash = password::hash_password(plaintext_password)
            .map_err(|e| UserStoreError::HashError(e.to_string()))?;

        let user = User::new(username.clone(), hash);
        let id = user.id;

        self.username_index.insert(username, id);
        self.users.insert(id, user.clone());

        Ok(user)
    }

    /// Get a user by ID.
    pub fn get_user(&self, id: Uuid) -> Option<User> {
        self.users.get(&id).map(|r| r.value().clone())
    }

    /// Get a user by username.
    pub fn get_user_by_username(&self, username: &str) -> Option<User> {
        let id = self.username_index.get(username)?;
        self.users.get(id.value()).map(|r| r.value().clone())
    }

    /// List all users.
    pub fn list_users(&self) -> Vec<User> {
        self.users.iter().map(|r| r.value().clone()).collect()
    }

    /// Update a user's properties (username, enabled).
    /// Does NOT change password — use `change_password` for that.
    pub fn update_user(
        &self,
        id: Uuid,
        new_username: Option<String>,
        enabled: Option<bool>,
    ) -> Result<User, UserStoreError> {
        let mut user = self
            .users
            .get_mut(&id)
            .ok_or_else(|| UserStoreError::UserNotFound(id.to_string()))?;

        if let Some(ref new_name) = new_username
            && new_name != &user.username
        {
            // Check for duplicate.
            if self.username_index.contains_key(new_name) {
                return Err(UserStoreError::DuplicateUsername(new_name.clone()));
            }
            // Remove old index entry, add new one.
            self.username_index.remove(&user.username);
            self.username_index.insert(new_name.clone(), id);
            user.username = new_name.clone();
        }

        if let Some(enabled) = enabled {
            user.enabled = enabled;
        }

        user.updated_at = chrono::Utc::now();
        Ok(user.clone())
    }

    /// Change a user's password.
    pub fn change_password(
        &self,
        id: Uuid,
        new_plaintext_password: &str,
    ) -> Result<(), UserStoreError> {
        let mut user = self
            .users
            .get_mut(&id)
            .ok_or_else(|| UserStoreError::UserNotFound(id.to_string()))?;

        let hash = password::hash_password(new_plaintext_password)
            .map_err(|e| UserStoreError::HashError(e.to_string()))?;

        user.password_hash = hash;
        user.updated_at = chrono::Utc::now();
        Ok(())
    }

    /// Delete a user by ID. Also removes user from any groups.
    pub fn delete_user(&self, id: Uuid) -> Result<(), UserStoreError> {
        let (_, user) = self
            .users
            .remove(&id)
            .ok_or_else(|| UserStoreError::UserNotFound(id.to_string()))?;

        self.username_index.remove(&user.username);

        // Remove from all groups.
        for mut group in self.groups.iter_mut() {
            group.members.retain(|mid| *mid != id);
        }

        Ok(())
    }

    /// Authenticate a user with username and password.
    /// Returns the user if credentials are valid.
    pub fn authenticate(
        &self,
        username: &str,
        plaintext_password: &str,
    ) -> Result<User, UserStoreError> {
        let user = self
            .get_user_by_username(username)
            .ok_or(UserStoreError::InvalidCredentials)?;

        if !user.enabled {
            return Err(UserStoreError::AccountDisabled(username.to_string()));
        }

        if !password::verify_password(plaintext_password, &user.password_hash) {
            return Err(UserStoreError::InvalidCredentials);
        }

        Ok(user)
    }

    /// Return the total number of users.
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    // -----------------------------------------------------------------------
    // Group operations
    // -----------------------------------------------------------------------

    /// Create a new user group.
    pub fn create_group(&self, name: String) -> Result<UserGroup, UserStoreError> {
        if self.group_name_index.contains_key(&name) {
            return Err(UserStoreError::DuplicateGroupName(name));
        }

        let group = UserGroup::new(name.clone());
        let id = group.id;
        self.group_name_index.insert(name, id);
        self.groups.insert(id, group.clone());
        Ok(group)
    }

    /// Get a group by ID.
    pub fn get_group(&self, id: Uuid) -> Option<UserGroup> {
        self.groups.get(&id).map(|r| r.value().clone())
    }

    /// List all groups.
    pub fn list_groups(&self) -> Vec<UserGroup> {
        self.groups.iter().map(|r| r.value().clone()).collect()
    }

    /// Update a group's name and/or members.
    pub fn update_group(
        &self,
        id: Uuid,
        new_name: Option<String>,
        members: Option<Vec<Uuid>>,
    ) -> Result<UserGroup, UserStoreError> {
        let mut group = self
            .groups
            .get_mut(&id)
            .ok_or_else(|| UserStoreError::GroupNotFound(id.to_string()))?;

        if let Some(ref name) = new_name
            && name != &group.name
        {
            if self.group_name_index.contains_key(name) {
                return Err(UserStoreError::DuplicateGroupName(name.clone()));
            }
            self.group_name_index.remove(&group.name);
            self.group_name_index.insert(name.clone(), id);
            group.name = name.clone();
        }

        if let Some(members) = members {
            // Validate all member IDs exist.
            for uid in &members {
                if !self.users.contains_key(uid) {
                    return Err(UserStoreError::UserNotFound(uid.to_string()));
                }
            }
            group.members = members;
        }

        group.updated_at = chrono::Utc::now();
        Ok(group.clone())
    }

    /// Delete a group by ID.
    pub fn delete_group(&self, id: Uuid) -> Result<(), UserStoreError> {
        let (_, group) = self
            .groups
            .remove(&id)
            .ok_or_else(|| UserStoreError::GroupNotFound(id.to_string()))?;

        self.group_name_index.remove(&group.name);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Token revocation
    // -----------------------------------------------------------------------

    /// Revoke a JWT token by its JTI (used for logout).
    pub fn revoke_token(&self, jti: &str) {
        self.revoked_tokens.insert(jti.to_string(), ());
    }

    /// Check whether a token has been revoked.
    pub fn is_token_revoked(&self, jti: &str) -> bool {
        self.revoked_tokens.contains_key(jti)
    }
}

impl Default for UserStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a shared `UserStore` wrapped in an `Arc`.
pub fn new_shared_store() -> Arc<UserStore> {
    Arc::new(UserStore::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get_user() {
        let store = UserStore::new();
        let user = store.create_user("alice".to_string(), "pass123").unwrap();
        assert_eq!(user.username, "alice");
        assert!(user.enabled);

        let fetched = store.get_user(user.id).unwrap();
        assert_eq!(fetched.username, "alice");

        let by_name = store.get_user_by_username("alice").unwrap();
        assert_eq!(by_name.id, user.id);
    }

    #[test]
    fn duplicate_username_rejected() {
        let store = UserStore::new();
        store.create_user("bob".to_string(), "pass").unwrap();
        let result = store.create_user("bob".to_string(), "pass2");
        assert!(result.is_err());
    }

    #[test]
    fn authenticate_valid_credentials() {
        let store = UserStore::new();
        store.create_user("admin".to_string(), "s3cret").unwrap();
        let user = store.authenticate("admin", "s3cret").unwrap();
        assert_eq!(user.username, "admin");
    }

    #[test]
    fn authenticate_wrong_password() {
        let store = UserStore::new();
        store.create_user("admin".to_string(), "correct").unwrap();
        assert!(store.authenticate("admin", "wrong").is_err());
    }

    #[test]
    fn authenticate_disabled_user() {
        let store = UserStore::new();
        let user = store.create_user("disabled".to_string(), "pass").unwrap();
        store.update_user(user.id, None, Some(false)).unwrap();
        let result = store.authenticate("disabled", "pass");
        assert!(matches!(result, Err(UserStoreError::AccountDisabled(_))));
    }

    #[test]
    fn update_user_username() {
        let store = UserStore::new();
        let user = store.create_user("old".to_string(), "pass").unwrap();
        store
            .update_user(user.id, Some("new".to_string()), None)
            .unwrap();
        assert!(store.get_user_by_username("old").is_none());
        assert!(store.get_user_by_username("new").is_some());
    }

    #[test]
    fn delete_user_removes_from_groups() {
        let store = UserStore::new();
        let user = store.create_user("member".to_string(), "pass").unwrap();
        let group = store.create_group("team".to_string()).unwrap();
        store
            .update_group(group.id, None, Some(vec![user.id]))
            .unwrap();
        assert_eq!(store.get_group(group.id).unwrap().members.len(), 1);

        store.delete_user(user.id).unwrap();
        assert_eq!(store.get_group(group.id).unwrap().members.len(), 0);
    }

    #[test]
    fn create_and_list_groups() {
        let store = UserStore::new();
        store.create_group("admins".to_string()).unwrap();
        store.create_group("operators".to_string()).unwrap();
        assert_eq!(store.list_groups().len(), 2);
    }

    #[test]
    fn duplicate_group_name_rejected() {
        let store = UserStore::new();
        store.create_group("team".to_string()).unwrap();
        let result = store.create_group("team".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn update_group_members_validates_user_ids() {
        let store = UserStore::new();
        let group = store.create_group("team".to_string()).unwrap();
        let fake_id = Uuid::now_v7();
        let result = store.update_group(group.id, None, Some(vec![fake_id]));
        assert!(result.is_err());
    }

    #[test]
    fn delete_group() {
        let store = UserStore::new();
        let group = store.create_group("temp".to_string()).unwrap();
        store.delete_group(group.id).unwrap();
        assert!(store.get_group(group.id).is_none());
    }

    #[test]
    fn token_revocation() {
        let store = UserStore::new();
        assert!(!store.is_token_revoked("token-123"));
        store.revoke_token("token-123");
        assert!(store.is_token_revoked("token-123"));
    }

    #[test]
    fn change_password() {
        let store = UserStore::new();
        let user = store.create_user("user".to_string(), "old-pass").unwrap();
        store.change_password(user.id, "new-pass").unwrap();
        assert!(store.authenticate("user", "old-pass").is_err());
        assert!(store.authenticate("user", "new-pass").is_ok());
    }

    #[test]
    fn list_users() {
        let store = UserStore::new();
        store.create_user("a".to_string(), "p").unwrap();
        store.create_user("b".to_string(), "p").unwrap();
        assert_eq!(store.list_users().len(), 2);
    }
}
