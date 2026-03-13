//! Flow versioning with git-based storage.
//!
//! Provides version control for flow configurations using a local git repository.
//! Each saved version creates a git commit, enabling full history, diffing, and
//! rollback capabilities.

pub mod diff;
pub mod version_store;

pub use diff::{FlowDiff, FlowDiffEntry};
pub use version_store::{FlowVersion, FlowVersionStore, VersionError};
