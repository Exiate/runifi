//! Core identity and local authentication.
//!
//! Provides user and group models, password hashing with Argon2, JWT session
//! management, and an in-memory user/group store.

pub mod jwt;
pub mod models;
pub mod password;
pub mod store;
