//! SWIM-based gossip protocol for dynamic cluster membership.
//!
//! Implements failure detection via direct and indirect probing, with
//! piggybacked membership updates on every message exchange.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::extract_node_id;
use super::node::ClusterNodeId;

/// Gossip-level membership state (separate from NodeState to avoid coupling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberState {
    /// Node is healthy and responsive.
    Alive,
    /// Node is suspected of failure (awaiting confirmation).
    Suspect,
    /// Node has been confirmed dead.
    Dead,
    /// Node has gracefully left the cluster.
    Left,
}

/// A membership update that is piggybacked on gossip messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberUpdate {
    /// The node this update is about.
    pub node_id: ClusterNodeId,
    /// Network address.
    pub address: String,
    /// New membership state.
    pub state: MemberState,
    /// Incarnation number — higher wins for the same node.
    pub incarnation: u64,
}

/// Internal representation of a gossip member.
#[derive(Debug, Clone)]
struct GossipMember {
    id: ClusterNodeId,
    address: String,
    state: MemberState,
    incarnation: u64,
    /// When the node was first suspected (for timeout calculation).
    suspect_since: Option<Instant>,
}

/// Events emitted by the gossip layer to inform the coordinator of
/// membership changes.
#[derive(Debug)]
pub enum GossipEvent {
    /// A new node has joined the cluster.
    NodeJoined { id: ClusterNodeId, address: String },
    /// A node is suspected of failure.
    NodeSuspected { id: ClusterNodeId },
    /// A node has been confirmed dead (failed probing).
    NodeDead { id: ClusterNodeId },
    /// A node has gracefully left the cluster.
    NodeLeft { id: ClusterNodeId },
    /// A previously suspect/dead node is now alive.
    NodeAlive { id: ClusterNodeId },
}

/// SWIM gossip state machine for dynamic membership management.
pub struct GossipState {
    /// This node's ID.
    self_id: ClusterNodeId,
    /// This node's address.
    self_address: String,
    /// Current incarnation number for this node.
    incarnation: AtomicU64,
    /// Known cluster members.
    members: Arc<RwLock<HashMap<ClusterNodeId, GossipMember>>>,
    /// Pending membership updates to piggyback on messages.
    pending_updates: RwLock<Vec<MemberUpdate>>,
    /// Gossip interval.
    gossip_interval: Duration,
    /// Number of indirect probes to send when direct probe fails.
    indirect_probe_count: usize,
    /// Time before a suspect node is declared dead.
    suspicion_timeout: Duration,
    /// Maximum updates to piggyback per message.
    gossip_fanout: usize,
}

impl GossipState {
    /// Create a new gossip state for this node.
    pub fn new(
        self_id: ClusterNodeId,
        self_address: String,
        gossip_interval_ms: u64,
        gossip_fanout: usize,
        indirect_probe_count: usize,
        suspicion_timeout_ms: u64,
    ) -> Self {
        Self {
            self_id,
            self_address,
            incarnation: AtomicU64::new(0),
            members: Arc::new(RwLock::new(HashMap::new())),
            pending_updates: RwLock::new(Vec::new()),
            gossip_interval: Duration::from_millis(gossip_interval_ms),
            indirect_probe_count,
            suspicion_timeout: Duration::from_millis(suspicion_timeout_ms),
            gossip_fanout,
        }
    }

    /// Bootstrap the membership list from seed nodes.
    pub fn add_seed_nodes(&self, seeds: &[String]) {
        let mut members = self.members.write();
        for addr in seeds {
            let node_id = extract_node_id(addr);
            if node_id == self.self_id {
                continue;
            }
            members
                .entry(node_id.clone())
                .or_insert_with(|| GossipMember {
                    id: node_id,
                    address: addr.clone(),
                    state: MemberState::Alive,
                    incarnation: 0,
                    suspect_since: None,
                });
        }
    }

    /// Process a membership update from a gossip message.
    ///
    /// Returns `Some(GossipEvent)` if the update changed membership state.
    pub fn process_member_update(&self, update: &MemberUpdate) -> Option<GossipEvent> {
        // If the update is about ourselves and says we're suspect/dead,
        // refute it by incrementing our incarnation.
        if update.node_id == self.self_id {
            if matches!(update.state, MemberState::Suspect | MemberState::Dead) {
                let current = self.incarnation.load(Ordering::Relaxed);
                if update.incarnation >= current {
                    let new_inc = update.incarnation + 1;
                    self.incarnation.store(new_inc, Ordering::Relaxed);
                    // Queue an Alive update for ourselves to broadcast.
                    self.queue_update(MemberUpdate {
                        node_id: self.self_id.clone(),
                        address: self.self_address.clone(),
                        state: MemberState::Alive,
                        incarnation: new_inc,
                    });
                }
            }
            return None;
        }

        let mut members = self.members.write();
        let member = members
            .entry(update.node_id.clone())
            .or_insert_with(|| GossipMember {
                id: update.node_id.clone(),
                address: update.address.clone(),
                state: MemberState::Dead, // Will be overwritten below
                incarnation: 0,
                suspect_since: None,
            });

        // SWIM update rules:
        // 1. Higher incarnation always wins.
        // 2. At same incarnation: Dead > Suspect > Alive.
        // 3. Left always wins at any incarnation.
        // SWIM update precedence: Left always wins, then higher incarnation,
        // then higher severity at same incarnation.
        let should_update = matches!(update.state, MemberState::Left)
            || update.incarnation > member.incarnation
            || (update.incarnation == member.incarnation
                && state_severity(update.state) > state_severity(member.state));

        if !should_update {
            return None;
        }

        let old_state = member.state;
        member.incarnation = update.incarnation;
        member.state = update.state;
        member.address = update.address.clone();

        match update.state {
            MemberState::Suspect => {
                if member.suspect_since.is_none() {
                    member.suspect_since = Some(Instant::now());
                }
                if old_state != MemberState::Suspect {
                    return Some(GossipEvent::NodeSuspected {
                        id: update.node_id.clone(),
                    });
                }
            }
            MemberState::Dead => {
                member.suspect_since = None;
                if old_state != MemberState::Dead {
                    return Some(GossipEvent::NodeDead {
                        id: update.node_id.clone(),
                    });
                }
            }
            MemberState::Left => {
                member.suspect_since = None;
                return Some(GossipEvent::NodeLeft {
                    id: update.node_id.clone(),
                });
            }
            MemberState::Alive => {
                member.suspect_since = None;
                if matches!(old_state, MemberState::Suspect | MemberState::Dead) {
                    return Some(GossipEvent::NodeAlive {
                        id: update.node_id.clone(),
                    });
                }
            }
        }

        None
    }

    /// Queue a membership update for piggybacking on outgoing messages.
    pub fn queue_update(&self, update: MemberUpdate) {
        let mut pending = self.pending_updates.write();
        // Limit pending queue to prevent unbounded growth.
        if pending.len() < 256 {
            pending.push(update);
        }
    }

    /// Drain up to `fanout` pending updates for piggybacking.
    pub fn drain_updates(&self) -> Vec<MemberUpdate> {
        let mut pending = self.pending_updates.write();
        let count = self.gossip_fanout.min(pending.len());
        pending.drain(..count).collect()
    }

    /// Check for suspect nodes that have exceeded the suspicion timeout
    /// and should be declared dead. Returns events for each newly dead node.
    pub fn check_suspicion_timeouts(&self) -> Vec<GossipEvent> {
        let mut events = Vec::new();
        let mut members = self.members.write();

        for member in members.values_mut() {
            if member.state == MemberState::Suspect
                && let Some(since) = member.suspect_since
                && since.elapsed() >= self.suspicion_timeout
            {
                member.state = MemberState::Dead;
                member.suspect_since = None;
                events.push(GossipEvent::NodeDead {
                    id: member.id.clone(),
                });
                // Queue death announcement
                self.pending_updates.write().push(MemberUpdate {
                    node_id: member.id.clone(),
                    address: member.address.clone(),
                    state: MemberState::Dead,
                    incarnation: member.incarnation,
                });
            }
        }

        events
    }

    /// Mark a node as suspect (failed direct probe).
    pub fn suspect_node(&self, node_id: &ClusterNodeId) -> Option<GossipEvent> {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(node_id)
            && member.state == MemberState::Alive
        {
            member.state = MemberState::Suspect;
            member.suspect_since = Some(Instant::now());
            self.pending_updates.write().push(MemberUpdate {
                node_id: member.id.clone(),
                address: member.address.clone(),
                state: MemberState::Suspect,
                incarnation: member.incarnation,
            });
            return Some(GossipEvent::NodeSuspected {
                id: node_id.clone(),
            });
        }
        None
    }

    /// Announce this node is gracefully leaving the cluster.
    pub fn announce_leave(&self) {
        let inc = self.incarnation.load(Ordering::Relaxed);
        self.queue_update(MemberUpdate {
            node_id: self.self_id.clone(),
            address: self.self_address.clone(),
            state: MemberState::Left,
            incarnation: inc,
        });
    }

    /// Get the list of alive member IDs (excluding self).
    pub fn alive_members(&self) -> Vec<ClusterNodeId> {
        self.members
            .read()
            .values()
            .filter(|m| m.state == MemberState::Alive && m.id != self.self_id)
            .map(|m| m.id.clone())
            .collect()
    }

    /// Get the address of a node by ID.
    pub fn member_address(&self, node_id: &ClusterNodeId) -> Option<String> {
        self.members.read().get(node_id).map(|m| m.address.clone())
    }

    /// Get the total number of known members (all states, excluding self).
    pub fn member_count(&self) -> usize {
        self.members
            .read()
            .values()
            .filter(|m| m.id != self.self_id)
            .count()
    }

    /// Get the number of alive members (excluding self).
    pub fn alive_count(&self) -> usize {
        self.alive_members().len()
    }

    /// Get the current incarnation number.
    pub fn incarnation(&self) -> u64 {
        self.incarnation.load(Ordering::Relaxed)
    }

    /// Get the gossip interval.
    pub fn interval(&self) -> Duration {
        self.gossip_interval
    }

    /// Get the indirect probe count.
    pub fn indirect_probe_count(&self) -> usize {
        self.indirect_probe_count
    }

    /// Add a new member that joined via gossip or join request.
    pub fn add_member(&self, node_id: ClusterNodeId, address: String) -> Option<GossipEvent> {
        let mut members = self.members.write();
        if node_id == self.self_id {
            return None;
        }
        let is_new = !members.contains_key(&node_id);
        let member = members
            .entry(node_id.clone())
            .or_insert_with(|| GossipMember {
                id: node_id.clone(),
                address: address.clone(),
                state: MemberState::Alive,
                incarnation: 0,
                suspect_since: None,
            });
        member.state = MemberState::Alive;
        member.suspect_since = None;

        if is_new {
            Some(GossipEvent::NodeJoined {
                id: node_id,
                address,
            })
        } else {
            Some(GossipEvent::NodeAlive { id: node_id })
        }
    }

    /// Remove a member entirely from the gossip membership.
    pub fn remove_member(&self, node_id: &ClusterNodeId) {
        self.members.write().remove(node_id);
    }

    /// Check if a node is alive in the gossip membership.
    pub fn is_alive(&self, node_id: &ClusterNodeId) -> bool {
        self.members
            .read()
            .get(node_id)
            .is_some_and(|m| m.state == MemberState::Alive)
    }

    /// Get all member summaries for API/dashboard consumption.
    pub fn all_members(&self) -> Vec<MemberUpdate> {
        self.members
            .read()
            .values()
            .map(|m| MemberUpdate {
                node_id: m.id.clone(),
                address: m.address.clone(),
                state: m.state,
                incarnation: m.incarnation,
            })
            .collect()
    }

    /// Run the main gossip loop.
    ///
    /// Currently handles suspicion timeouts (declaring timed-out suspects dead).
    /// Active SWIM probing (direct Ping, indirect PingReq, piggybacked
    /// membership dissemination) will be added when the gossip layer is
    /// integrated with the TCP transport for inter-node messaging.
    /// Until then, failure detection relies on the existing heartbeat mechanism.
    pub async fn run_gossip_loop(
        &self,
        event_tx: mpsc::Sender<GossipEvent>,
        cancel: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(self.gossip_interval);
        interval.tick().await; // Skip first tick

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("Gossip loop cancelled");
                    break;
                }
                _ = interval.tick() => {
                    // Check suspicion timeouts
                    let timeout_events = self.check_suspicion_timeouts();
                    for event in timeout_events {
                        let _ = event_tx.send(event).await;
                    }
                }
            }
        }
    }
}

/// Severity ordering for SWIM state precedence at the same incarnation.
fn state_severity(state: MemberState) -> u8 {
    match state {
        MemberState::Alive => 0,
        MemberState::Suspect => 1,
        MemberState::Dead => 2,
        MemberState::Left => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_gossip() -> GossipState {
        GossipState::new("node-1".into(), "node-1:9443".into(), 1000, 3, 3, 5000)
    }

    #[test]
    fn add_seed_nodes_excludes_self() {
        let gossip = make_gossip();
        gossip.add_seed_nodes(&[
            "node-1:9443".into(),
            "node-2:9443".into(),
            "node-3:9443".into(),
        ]);
        assert_eq!(gossip.alive_count(), 2);
    }

    #[test]
    fn process_update_higher_incarnation_wins() {
        let gossip = make_gossip();
        gossip.add_seed_nodes(&["node-2:9443".into(), "node-3:9443".into()]);

        // Suspect at incarnation 0
        let event = gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Suspect,
            incarnation: 0,
        });
        assert!(matches!(event, Some(GossipEvent::NodeSuspected { .. })));

        // Alive at higher incarnation overrides suspect
        let event = gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Alive,
            incarnation: 1,
        });
        assert!(matches!(event, Some(GossipEvent::NodeAlive { .. })));
    }

    #[test]
    fn process_update_same_incarnation_severity_wins() {
        let gossip = make_gossip();
        gossip.add_seed_nodes(&["node-2:9443".into(), "node-3:9443".into()]);

        // First suspect
        gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        // Dead at same incarnation overrides suspect
        let event = gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Dead,
            incarnation: 0,
        });
        assert!(matches!(event, Some(GossipEvent::NodeDead { .. })));

        // Alive at same incarnation does NOT override dead
        let event = gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Alive,
            incarnation: 0,
        });
        assert!(event.is_none());
    }

    #[test]
    fn self_suspect_refutation() {
        let gossip = make_gossip();

        // Someone suspects us
        gossip.process_member_update(&MemberUpdate {
            node_id: "node-1".into(),
            address: "node-1:9443".into(),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        // Our incarnation should have been incremented
        assert_eq!(gossip.incarnation(), 1);
    }

    #[test]
    fn left_always_wins() {
        let gossip = make_gossip();
        gossip.add_seed_nodes(&["node-2:9443".into(), "node-3:9443".into()]);

        // Even at lower incarnation, Left wins
        let event = gossip.process_member_update(&MemberUpdate {
            node_id: "node-2".into(),
            address: "node-2:9443".into(),
            state: MemberState::Left,
            incarnation: 0,
        });
        assert!(matches!(event, Some(GossipEvent::NodeLeft { .. })));
    }

    #[test]
    fn suspect_node_transitions_alive_to_suspect() {
        let gossip = make_gossip();
        gossip.add_seed_nodes(&["node-2:9443".into(), "node-3:9443".into()]);

        let event = gossip.suspect_node(&"node-2".into());
        assert!(matches!(event, Some(GossipEvent::NodeSuspected { .. })));
        assert!(!gossip.is_alive(&"node-2".into()));
    }

    #[test]
    fn drain_updates_respects_fanout() {
        let gossip = GossipState::new("node-1".into(), "node-1:9443".into(), 1000, 2, 3, 5000);
        for i in 0..5 {
            gossip.queue_update(MemberUpdate {
                node_id: format!("node-{}", i),
                address: format!("node-{}:9443", i),
                state: MemberState::Alive,
                incarnation: 0,
            });
        }
        let drained = gossip.drain_updates();
        assert_eq!(drained.len(), 2); // fanout = 2
    }

    #[test]
    fn add_member_returns_join_event() {
        let gossip = make_gossip();
        let event = gossip.add_member("node-5".into(), "node-5:9443".into());
        assert!(matches!(event, Some(GossipEvent::NodeJoined { .. })));
        assert!(gossip.is_alive(&"node-5".into()));
    }

    #[test]
    fn announce_leave_queues_update() {
        let gossip = make_gossip();
        gossip.announce_leave();
        let updates = gossip.drain_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].node_id, "node-1");
        assert!(matches!(updates[0].state, MemberState::Left));
    }

    #[test]
    fn suspicion_timeout_declares_dead() {
        let gossip = GossipState::new(
            "node-1".into(),
            "node-1:9443".into(),
            1000,
            3,
            3,
            0, // 0ms timeout for immediate timeout in tests
        );
        gossip.add_seed_nodes(&["node-2:9443".into(), "node-3:9443".into()]);

        // Suspect a node
        gossip.suspect_node(&"node-2".into());

        // Check timeouts — should immediately declare dead since timeout is 0
        let events = gossip.check_suspicion_timeouts();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], GossipEvent::NodeDead { .. }));
    }
}
