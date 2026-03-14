//! Split-brain protection via seed-node quorum.
//!
//! A node must be able to reach a majority of seed nodes to continue processing.
//! If quorum is lost (minority partition), the node stops scheduling processors
//! and returns 503 on API requests.

use std::sync::atomic::{AtomicBool, Ordering};

/// Quorum state tracker for split-brain protection.
pub struct QuorumState {
    /// Seed node IDs that participate in quorum calculation.
    seed_node_ids: Vec<String>,
    /// Whether this node currently has quorum.
    has_quorum: AtomicBool,
}

impl QuorumState {
    /// Create a new quorum state tracker.
    ///
    /// `seed_node_ids` should be the IDs of all seed nodes (extracted from addresses).
    pub fn new(seed_node_ids: Vec<String>) -> Self {
        Self {
            seed_node_ids,
            has_quorum: AtomicBool::new(true), // Optimistic start
        }
    }

    /// Check quorum against the currently alive nodes.
    ///
    /// Returns `true` if quorum is held (majority of seed nodes are reachable).
    pub fn check_quorum(&self, alive_nodes: &[String], self_id: &str) -> bool {
        if self.seed_node_ids.is_empty() {
            return true; // No seeds configured = always has quorum
        }

        // Count how many seed nodes are alive (including self if self is a seed).
        let reachable = self
            .seed_node_ids
            .iter()
            .filter(|seed| *seed == self_id || alive_nodes.iter().any(|alive| alive == *seed))
            .count();

        let majority = (self.seed_node_ids.len() / 2) + 1;
        let quorum = reachable >= majority;

        let old_quorum = self.has_quorum.swap(quorum, Ordering::Relaxed);

        if old_quorum && !quorum {
            tracing::error!(
                reachable = reachable,
                total_seeds = self.seed_node_ids.len(),
                majority = majority,
                "Cluster quorum LOST — this node is in a minority partition"
            );
        } else if !old_quorum && quorum {
            tracing::warn!(
                reachable = reachable,
                total_seeds = self.seed_node_ids.len(),
                "Cluster quorum RESTORED"
            );
        }

        quorum
    }

    /// Returns whether this node currently has quorum.
    pub fn has_quorum(&self) -> bool {
        self.has_quorum.load(Ordering::Relaxed)
    }

    /// Get the number of seed nodes.
    pub fn seed_count(&self) -> usize {
        self.seed_node_ids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_with_no_seeds_always_holds() {
        let quorum = QuorumState::new(vec![]);
        assert!(quorum.check_quorum(&[], "node-1"));
        assert!(quorum.has_quorum());
    }

    #[test]
    fn quorum_with_three_seeds_needs_two() {
        let quorum = QuorumState::new(vec!["node-1".into(), "node-2".into(), "node-3".into()]);

        // Self + 1 alive = 2 reachable = majority of 3
        assert!(quorum.check_quorum(&["node-2".into()], "node-1"));

        // Self alone = 1 reachable < majority of 2
        assert!(!quorum.check_quorum(&[], "node-1"));
    }

    #[test]
    fn quorum_with_five_seeds_needs_three() {
        let quorum = QuorumState::new(vec![
            "node-1".into(),
            "node-2".into(),
            "node-3".into(),
            "node-4".into(),
            "node-5".into(),
        ]);

        // Self + 2 = 3 = majority
        assert!(quorum.check_quorum(&["node-2".into(), "node-3".into()], "node-1"));

        // Self + 1 = 2 < majority of 3
        assert!(!quorum.check_quorum(&["node-2".into()], "node-1"));
    }

    #[test]
    fn quorum_loss_and_recovery() {
        let quorum = QuorumState::new(vec!["node-1".into(), "node-2".into(), "node-3".into()]);

        assert!(quorum.check_quorum(&["node-2".into()], "node-1"));
        assert!(quorum.has_quorum());

        // Lose quorum
        assert!(!quorum.check_quorum(&[], "node-1"));
        assert!(!quorum.has_quorum());

        // Regain quorum
        assert!(quorum.check_quorum(&["node-2".into()], "node-1"));
        assert!(quorum.has_quorum());
    }

    #[test]
    fn non_seed_node_doesnt_count_self() {
        let quorum = QuorumState::new(vec!["node-2".into(), "node-3".into(), "node-4".into()]);

        // Self is not a seed — need 2 seed nodes alive
        assert!(quorum.check_quorum(&["node-2".into(), "node-3".into()], "node-1"));

        // Only 1 seed alive
        assert!(!quorum.check_quorum(&["node-2".into()], "node-1"));
    }
}
