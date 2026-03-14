//! Cluster inter-node communication protocol.
//!
//! All messages are length-prefixed JSON over TCP.  The wire format is:
//!
//! ```text
//! ┌──────────┬────────────────────────┐
//! │ 4 bytes  │  N bytes               │
//! │ (u32 LE) │  (JSON payload)        │
//! └──────────┴────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use super::election::ElectionRole;
use super::gossip::MemberUpdate;
use super::node::{ClusterNodeId, ClusterRole, NodeState};

/// Envelope for all cluster protocol messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMessage {
    /// The node that sent this message.
    pub sender_id: ClusterNodeId,
    /// Monotonic message sequence number (per sender).
    pub seq: u64,
    /// The payload.
    pub payload: MessagePayload,
}

/// Protocol message types exchanged between cluster nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum MessagePayload {
    // ── Heartbeat ────────────────────────────────────────────────────────
    /// Periodic heartbeat from a node.
    Heartbeat(HeartbeatData),

    /// Acknowledgement of a heartbeat.
    HeartbeatAck(HeartbeatAckData),

    // ── Election ─────────────────────────────────────────────────────────
    /// Request a vote in an election.
    VoteRequest(VoteRequestData),

    /// Response to a vote request.
    VoteResponse(VoteResponseData),

    /// Announcement that a node has won an election.
    ElectionWon(ElectionWonData),

    // ── Flow replication ─────────────────────────────────────────────────
    /// Flow configuration update broadcast by the coordinator.
    FlowUpdate(FlowUpdateData),

    /// Acknowledgement that a flow update was applied.
    FlowUpdateAck(FlowUpdateAckData),

    /// Request the current flow configuration (sent by joining nodes).
    FlowSyncRequest,

    /// Response with the current flow configuration.
    FlowSyncResponse(FlowSyncResponseData),

    // ── Membership ───────────────────────────────────────────────────────
    /// Node join request.
    JoinRequest(JoinRequestData),

    /// Node join response from coordinator.
    JoinResponse(JoinResponseData),

    /// Graceful disconnect announcement.
    DisconnectNotice,

    // ── Gossip (Phase 2) ──────────────────────────────────────────────
    /// SWIM ping probe.
    Ping(PingData),

    /// SWIM ping acknowledgement with piggybacked membership updates.
    PingAck(PingAckData),

    /// SWIM indirect ping request — ask a peer to probe a suspect node.
    PingReq(PingReqData),

    /// Gossip membership update broadcast.
    GossipMembership(GossipMembershipData),

    // ── Decommission (Phase 2) ────────────────────────────────────────
    /// Coordinator instructs a node to begin decommissioning.
    DecommissionNotice,

    /// Node reports that decommission is complete.
    DecommissionComplete,

    // ── Bulletins (Phase 2) ───────────────────────────────────────────
    /// Forward a bulletin from a remote node to the coordinator.
    BulletinForward(BulletinForwardData),
}

// ── Heartbeat types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatData {
    /// Current state of the sending node.
    pub state: NodeState,
    /// Roles held by the sending node.
    pub roles: Vec<ClusterRole>,
    /// Flow version the sender has applied.
    pub flow_version: u64,
    /// Current election term.
    pub election_term: u64,
    /// Summary metrics for monitoring.
    pub metrics: Option<NodeMetricsSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatAckData {
    /// Whether the coordinator accepted the heartbeat.
    pub accepted: bool,
    /// Current coordinator node ID.
    pub coordinator_id: Option<ClusterNodeId>,
    /// Current primary node ID.
    pub primary_id: Option<ClusterNodeId>,
    /// Current election term.
    pub election_term: u64,
}

/// Lightweight metrics included in heartbeats for cluster-wide monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetricsSummary {
    /// Total FlowFiles processed since startup.
    pub total_flowfiles: u64,
    /// Active processor count.
    pub active_processors: u32,
    /// System load (0.0..1.0 if available).
    pub system_load: Option<f64>,
}

// ── Election types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequestData {
    /// The election term for this vote.
    pub term: u64,
    /// The role being elected.
    pub role: ElectionRole,
    /// Candidate's node ID.
    pub candidate_id: ClusterNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponseData {
    /// The election term this vote is for.
    pub term: u64,
    /// Whether the voter grants the vote.
    pub vote_granted: bool,
    /// The role being elected.
    pub role: ElectionRole,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionWonData {
    /// The election term.
    pub term: u64,
    /// The role that was won.
    pub role: ElectionRole,
    /// The winning node ID.
    pub winner_id: ClusterNodeId,
}

// ── Flow replication types ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowUpdateData {
    /// Monotonically increasing flow version.
    pub version: u64,
    /// The complete flow configuration as TOML string.
    pub flow_config: String,
    /// Election term of the coordinator that issued this update.
    pub coordinator_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowUpdateAckData {
    /// The flow version that was applied.
    pub version: u64,
    /// Whether the update was successfully applied.
    pub success: bool,
    /// Error message if the update failed.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSyncResponseData {
    /// Current flow version.
    pub version: u64,
    /// Complete flow configuration as TOML string.
    pub flow_config: String,
    /// Current coordinator ID.
    pub coordinator_id: Option<ClusterNodeId>,
    /// Current primary node ID.
    pub primary_id: Option<ClusterNodeId>,
    /// Current election term.
    pub election_term: u64,
}

// ── Membership types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequestData {
    /// Address the joining node listens on.
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponseData {
    /// Whether the join was accepted.
    pub accepted: bool,
    /// Reason for rejection (if not accepted).
    pub reason: Option<String>,
    /// Current flow version to sync.
    pub flow_version: u64,
    /// Current flow config.
    pub flow_config: Option<String>,
}

// ── Gossip types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingData {
    /// This node's incarnation number.
    pub incarnation: u64,
    /// Piggybacked membership updates.
    pub updates: Vec<MemberUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingAckData {
    /// Responding node's incarnation number.
    pub incarnation: u64,
    /// Piggybacked membership updates.
    pub updates: Vec<MemberUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingReqData {
    /// The suspect node to probe.
    pub target_id: ClusterNodeId,
    /// Address of the suspect node.
    pub target_address: String,
    /// Requester's incarnation.
    pub incarnation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMembershipData {
    /// Batch of membership updates.
    pub updates: Vec<MemberUpdate>,
}

// ── Bulletin types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulletinForwardData {
    /// The processor that generated the bulletin.
    pub processor_name: String,
    /// Severity level ("warn" or "error").
    pub severity: String,
    /// The bulletin message.
    pub message: String,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
}

// ── Wire format helpers ──────────────────────────────────────────────────────

impl ClusterMessage {
    /// Serialize to length-prefixed JSON bytes for wire transmission.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        let len = json.len() as u32;
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&json);
        Ok(buf)
    }

    /// Deserialize from a JSON byte slice (without the length prefix).
    pub fn decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_heartbeat() {
        let msg = ClusterMessage {
            sender_id: "node-1".into(),
            seq: 42,
            payload: MessagePayload::Heartbeat(HeartbeatData {
                state: NodeState::Connected,
                roles: vec![ClusterRole::Coordinator],
                flow_version: 5,
                election_term: 3,
                metrics: Some(NodeMetricsSummary {
                    total_flowfiles: 1000,
                    active_processors: 4,
                    system_load: Some(0.75),
                }),
            }),
        };

        let encoded = msg.encode().unwrap();
        assert!(encoded.len() > 4);

        let len = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
        assert_eq!(len, encoded.len() - 4);

        let decoded = ClusterMessage::decode(&encoded[4..]).unwrap();
        assert_eq!(decoded.sender_id, "node-1");
        assert_eq!(decoded.seq, 42);

        match decoded.payload {
            MessagePayload::Heartbeat(data) => {
                assert_eq!(data.state, NodeState::Connected);
                assert_eq!(data.flow_version, 5);
                assert_eq!(data.election_term, 3);
            }
            _ => panic!("expected Heartbeat payload"),
        }
    }

    #[test]
    fn roundtrip_vote_request() {
        let msg = ClusterMessage {
            sender_id: "node-2".into(),
            seq: 1,
            payload: MessagePayload::VoteRequest(VoteRequestData {
                term: 5,
                role: ElectionRole::Coordinator,
                candidate_id: "node-2".into(),
            }),
        };

        let encoded = msg.encode().unwrap();
        let decoded = ClusterMessage::decode(&encoded[4..]).unwrap();
        match decoded.payload {
            MessagePayload::VoteRequest(data) => {
                assert_eq!(data.term, 5);
                assert_eq!(data.role, ElectionRole::Coordinator);
            }
            _ => panic!("expected VoteRequest payload"),
        }
    }

    #[test]
    fn roundtrip_flow_update() {
        let msg = ClusterMessage {
            sender_id: "node-1".into(),
            seq: 10,
            payload: MessagePayload::FlowUpdate(FlowUpdateData {
                version: 42,
                flow_config: "[flow]\nname = \"test\"".into(),
                coordinator_term: 3,
            }),
        };

        let encoded = msg.encode().unwrap();
        let decoded = ClusterMessage::decode(&encoded[4..]).unwrap();
        match decoded.payload {
            MessagePayload::FlowUpdate(data) => {
                assert_eq!(data.version, 42);
                assert!(data.flow_config.contains("test"));
            }
            _ => panic!("expected FlowUpdate payload"),
        }
    }
}
