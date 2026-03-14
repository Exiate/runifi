//! Core identity and pluggable authentication.
//!
//! Provides user and group models, password hashing with Argon2, JWT session
//! management, an in-memory user/group store, and a pluggable authentication
//! provider framework for enterprise identity integration (OIDC, LDAP, mTLS).

pub mod jwt;
pub mod models;
pub mod password;
pub mod store;

// Pluggable auth provider framework.
pub mod apikey_provider;
pub mod chain;
pub mod identity_mapper;
pub mod ldap_provider;
pub mod local_provider;
pub mod mtls_provider;
pub mod oidc_provider;
pub mod provider;
pub mod session;
