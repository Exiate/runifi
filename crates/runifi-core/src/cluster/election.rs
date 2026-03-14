//! Raft-inspired leader election for cluster coordinator and primary node
//! roles.
//!
//! This is a simplified Raft election (no log replication — only role election).
//! The full Raft log is unnecessary because RuniFi only needs consensus on:
//! 1. Who is the cluster coordinator.
//! 2. Who is the primary node.
//!
//! The election protocol:
//! 1. Each node starts as a **Follower**.
//! 2. If a follower does not receive a heartbeat from the coordinator within
//!    the election timeout (with random jitter), it becomes a **Candidate**.
//! 3. The candidate increments its term and sends `VoteRequest` to all peers.
//! 4. Each node votes for at most one candidate per term (first-come).
//! 5. A candidate that receives a majority of votes becomes the leader for
//!    that role and broadcasts `ElectionWon`.
//! 6. If the election times out (no majority), a new term begins.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::node::ClusterNodeId;

/// The role being elected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElectionRole {
    /// Cluster coordinator (heartbeat monitor, flow replicator).
    Coordinator,
    /// Primary node (runs primary-only processors).
    PrimaryNode,
}

/// Raft-like election state for a single role.
#[derive(Debug)]
pub struct ElectionState {
    /// The role this election is for.
    role: ElectionRole,

    /// Current election term (monotonically increasing).
    term: AtomicU64,

    /// Mutable election state protected by a mutex.
    inner: Mutex<ElectionInner>,
}

#[derive(Debug)]
struct ElectionInner {
    /// Current state of this node in the election.
    state: RaftState,

    /// Node ID this node voted for in the current term (at most one per term).
    voted_for: Option<ClusterNodeId>,

    /// Set of nodes that have voted for us (when we are a candidate).
    votes_received: HashSet<ClusterNodeId>,

    /// The current leader for this role.
    leader_id: Option<ClusterNodeId>,

    /// Total number of voting nodes in the cluster.
    cluster_size: usize,
}

/// Raft node states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    /// Passive — waits for heartbeats from the leader.
    Follower,
    /// Actively requesting votes.
    Candidate,
    /// Won the election — holds the role.
    Leader,
}

impl ElectionState {
    /// Create a new election state for the given role.
    pub fn new(role: ElectionRole, cluster_size: usize) -> Self {
        Self {
            role,
            term: AtomicU64::new(0),
            inner: Mutex::new(ElectionInner {
                state: RaftState::Follower,
                voted_for: None,
                votes_received: HashSet::new(),
                leader_id: None,
                cluster_size,
            }),
        }
    }

    /// Get the role this election is for.
    pub fn role(&self) -> ElectionRole {
        self.role
    }

    /// Get the current election term.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Relaxed)
    }

    /// Get the current Raft state.
    pub fn raft_state(&self) -> RaftState {
        self.inner.lock().state
    }

    /// Get the current leader ID for this role.
    pub fn leader_id(&self) -> Option<ClusterNodeId> {
        self.inner.lock().leader_id.clone()
    }

    /// Update the cluster size (used when nodes join/leave).
    pub fn set_cluster_size(&self, size: usize) {
        self.inner.lock().cluster_size = size;
    }

    /// Start a new election: increment term, become candidate, vote for self.
    ///
    /// Returns the new term number.
    pub fn start_election(&self, self_id: &ClusterNodeId) -> u64 {
        let new_term = self.term.fetch_add(1, Ordering::Relaxed) + 1;
        let mut inner = self.inner.lock();
        inner.state = RaftState::Candidate;
        inner.voted_for = Some(self_id.clone());
        inner.votes_received.clear();
        inner.votes_received.insert(self_id.clone());
        inner.leader_id = None;
        tracing::info!(
            role = %self.role,
            term = new_term,
            node = %self_id,
            "Starting election"
        );
        new_term
    }

    /// Handle a vote request from another candidate.
    ///
    /// Returns `(vote_granted, current_term)`.
    ///
    /// A vote is granted if:
    /// - The request term >= our current term
    /// - We have not voted for anyone else in this term
    pub fn handle_vote_request(
        &self,
        candidate_id: &ClusterNodeId,
        request_term: u64,
    ) -> (bool, u64) {
        let current_term = self.term.load(Ordering::Relaxed);

        if request_term < current_term {
            // Stale term — reject.
            return (false, current_term);
        }

        let mut inner = self.inner.lock();

        if request_term > current_term {
            // Higher term — step down to follower and update term.
            self.term.store(request_term, Ordering::Relaxed);
            inner.state = RaftState::Follower;
            inner.voted_for = None;
            inner.leader_id = None;
        }

        // Vote if we haven't voted yet or already voted for this candidate.
        let grant = match &inner.voted_for {
            None => true,
            Some(id) => id == candidate_id,
        };

        if grant {
            inner.voted_for = Some(candidate_id.clone());
            tracing::debug!(
                role = %self.role,
                term = request_term,
                candidate = %candidate_id,
                "Granted vote"
            );
        }

        (grant, request_term)
    }

    /// Record a vote received from a peer.
    ///
    /// Returns `true` if a majority has been reached (election won).
    pub fn record_vote(&self, voter_id: &ClusterNodeId, vote_term: u64) -> bool {
        let current_term = self.term.load(Ordering::Relaxed);
        if vote_term != current_term {
            return false;
        }

        let mut inner = self.inner.lock();
        if inner.state != RaftState::Candidate {
            return false;
        }

        inner.votes_received.insert(voter_id.clone());
        let votes = inner.votes_received.len();
        let majority = (inner.cluster_size / 2) + 1;

        tracing::debug!(
            role = %self.role,
            term = current_term,
            votes = votes,
            majority = majority,
            voter = %voter_id,
            "Vote received"
        );

        votes >= majority
    }

    /// Transition to leader state after winning an election.
    pub fn become_leader(&self, self_id: &ClusterNodeId) {
        let mut inner = self.inner.lock();
        inner.state = RaftState::Leader;
        inner.leader_id = Some(self_id.clone());
        tracing::info!(
            role = %self.role,
            term = self.term.load(Ordering::Relaxed),
            node = %self_id,
            "Won election — became leader"
        );
    }

    /// Step down from leader to follower (e.g. when a higher term is seen).
    pub fn step_down(&self) {
        let mut inner = self.inner.lock();
        if inner.state == RaftState::Leader {
            tracing::info!(
                role = %self.role,
                term = self.term.load(Ordering::Relaxed),
                "Stepping down from leader"
            );
        }
        inner.state = RaftState::Follower;
        inner.voted_for = None;
    }

    /// Accept a leader announcement from another node.
    ///
    /// If the announced term >= our term, we step down and record the leader.
    pub fn accept_leader(&self, leader_id: &ClusterNodeId, leader_term: u64) {
        let current_term = self.term.load(Ordering::Relaxed);
        if leader_term >= current_term {
            self.term.store(leader_term, Ordering::Relaxed);
            let mut inner = self.inner.lock();
            inner.state = RaftState::Follower;
            inner.leader_id = Some(leader_id.clone());
            inner.voted_for = None;
            tracing::info!(
                role = %self.role,
                term = leader_term,
                leader = %leader_id,
                "Accepted leader"
            );
        }
    }

    /// Check if this node is currently the leader.
    pub fn is_leader(&self) -> bool {
        self.inner.lock().state == RaftState::Leader
    }

    /// Reset election state (used when the node leaves the cluster).
    pub fn reset(&self) {
        self.term.store(0, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        inner.state = RaftState::Follower;
        inner.voted_for = None;
        inner.votes_received.clear();
        inner.leader_id = None;
    }
}

impl std::fmt::Display for ElectionRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Coordinator => write!(f, "coordinator"),
            Self::PrimaryNode => write!(f, "primary_node"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn election_basic_flow() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);

        // Initially a follower.
        assert_eq!(election.raft_state(), RaftState::Follower);
        assert_eq!(election.term(), 0);
        assert!(!election.is_leader());

        // Start election — becomes candidate at term 1.
        let term = election.start_election(&"node-1".into());
        assert_eq!(term, 1);
        assert_eq!(election.raft_state(), RaftState::Candidate);

        // Record own vote (already counted in start_election).
        // Need 2 votes for majority in a 3-node cluster.
        assert!(!election.record_vote(&"node-1".into(), 1)); // already counted

        // Second vote — majority reached.
        assert!(election.record_vote(&"node-2".into(), 1));

        // Become leader.
        election.become_leader(&"node-1".into());
        assert!(election.is_leader());
        assert_eq!(election.leader_id(), Some("node-1".into()));
    }

    #[test]
    fn election_vote_request_handling() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);

        // Grant vote to first requester.
        let (granted, term) = election.handle_vote_request(&"node-2".into(), 1);
        assert!(granted);
        assert_eq!(term, 1);

        // Reject vote to second requester in same term.
        let (granted, _) = election.handle_vote_request(&"node-3".into(), 1);
        assert!(!granted);

        // Accept vote from higher term.
        let (granted, term) = election.handle_vote_request(&"node-3".into(), 2);
        assert!(granted);
        assert_eq!(term, 2);
    }

    #[test]
    fn election_step_down_on_higher_term() {
        let election = ElectionState::new(ElectionRole::PrimaryNode, 3);

        election.start_election(&"node-1".into());
        assert_eq!(election.raft_state(), RaftState::Candidate);

        // Higher term vote request forces step down.
        let (granted, _) = election.handle_vote_request(&"node-2".into(), 10);
        assert!(granted);
        assert_eq!(election.raft_state(), RaftState::Follower);
        assert_eq!(election.term(), 10);
    }

    #[test]
    fn election_accept_leader() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);

        election.accept_leader(&"node-3".into(), 5);
        assert_eq!(election.raft_state(), RaftState::Follower);
        assert_eq!(election.leader_id(), Some("node-3".into()));
        assert_eq!(election.term(), 5);
    }

    #[test]
    fn election_stale_vote_rejected() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);

        // Advance term to 5.
        election.handle_vote_request(&"node-2".into(), 5);

        // Stale term 3 is rejected.
        let (granted, term) = election.handle_vote_request(&"node-3".into(), 3);
        assert!(!granted);
        assert_eq!(term, 5);
    }

    #[test]
    fn election_reset() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);
        election.start_election(&"node-1".into());
        election.become_leader(&"node-1".into());

        election.reset();
        assert_eq!(election.term(), 0);
        assert_eq!(election.raft_state(), RaftState::Follower);
        assert!(election.leader_id().is_none());
    }

    #[test]
    fn election_stale_vote_not_counted() {
        let election = ElectionState::new(ElectionRole::Coordinator, 3);
        election.start_election(&"node-1".into());

        // Vote from a different term is ignored.
        assert!(!election.record_vote(&"node-2".into(), 99));
    }
}
