//! Site-to-Site (S2S) protocol types and frame encode/decode.
//!
//! S2S extends the base QUIC protocol with peer communication for
//! remote process group support: port enumeration, peer health status,
//! and transactional FlowFile transfers between RuniFi instances.

use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use crate::error::{TransportError, TransportResult};
use crate::protocol::FrameType;

// ── Configuration ────────────────────────────────────────────────────────────

/// Configuration for a Site-to-Site connection to a remote instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S2sConfig {
    /// Remote instance URL (e.g., "https://remote-host:8443").
    pub remote_url: String,
    /// Transport protocol to use.
    pub transport_protocol: TransportProtocol,
    /// Timeout for communications with the remote instance.
    #[serde(with = "duration_ms")]
    pub communications_timeout: Duration,
    /// Yield duration when no data is available.
    #[serde(with = "duration_ms")]
    pub yield_duration: Duration,
    /// Maximum number of FlowFiles per transaction batch.
    pub batch_count: usize,
    /// Maximum bytes per transaction batch.
    pub batch_size_bytes: u64,
    /// Maximum duration for a transaction batch.
    #[serde(with = "duration_ms")]
    pub batch_duration: Duration,
}

impl Default for S2sConfig {
    fn default() -> Self {
        Self {
            remote_url: String::new(),
            transport_protocol: TransportProtocol::Quic,
            communications_timeout: Duration::from_secs(30),
            yield_duration: Duration::from_secs(1),
            batch_count: 100,
            batch_size_bytes: 5_000_000,
            batch_duration: Duration::from_secs(5),
        }
    }
}

/// Transport protocol for S2S communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportProtocol {
    /// QUIC transport (default and only supported protocol).
    Quic,
}

impl std::fmt::Display for TransportProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportProtocol::Quic => write!(f, "QUIC"),
        }
    }
}

// ── Remote port types ────────────────────────────────────────────────────────

/// Information about a port on a remote RuniFi instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemotePort {
    /// Unique identifier for this port on the remote instance.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Whether this is an input or output port.
    pub port_type: RemotePortType,
    /// Whether the port is connected to a local connection.
    pub connected: bool,
    /// Whether the port is actively transmitting.
    pub transmitting: bool,
}

/// The type of a remote port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemotePortType {
    /// Receives FlowFiles from the remote instance (remote output port).
    Input,
    /// Sends FlowFiles to the remote instance (remote input port).
    Output,
}

impl std::fmt::Display for RemotePortType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemotePortType::Input => write!(f, "INPUT"),
            RemotePortType::Output => write!(f, "OUTPUT"),
        }
    }
}

// ── S2S decoded frames ───────────────────────────────────────────────────────

/// A decoded S2S protocol frame.
#[derive(Debug, Clone)]
pub enum S2sFrame {
    /// Request to enumerate available ports on the remote instance.
    PortEnumRequest,
    /// Response with available ports.
    PortEnumResponse { ports: Vec<RemotePort> },
    /// Request for peer health/load status.
    PeerStatusRequest,
    /// Response with peer health/load status.
    PeerStatusResponse {
        /// Load factor (0.0 = idle, 1.0 = fully loaded).
        load: f64,
        /// Number of FlowFiles currently queued.
        flow_files_queued: u64,
    },
    /// Begin a new transaction for the specified port.
    TxnBegin { port_id: String },
    /// Commit an active transaction.
    TxnCommit { transaction_id: String },
    /// Roll back an active transaction.
    TxnRollback { transaction_id: String },
}

// ── Encoding ─────────────────────────────────────────────────────────────────

/// Encode a port enumeration request frame.
pub fn encode_port_enum_request() -> BytesMut {
    let mut buf = BytesMut::with_capacity(1);
    buf.put_u8(FrameType::S2sPortEnumRequest as u8);
    buf
}

/// Encode a port enumeration response frame.
pub fn encode_port_enum_response(ports: &[RemotePort]) -> BytesMut {
    let mut size = 1 + 4; // frame_type + port_count
    for p in ports {
        // id_len(2) + id + name_len(2) + name + port_type(1) + connected(1) + transmitting(1)
        size += 2 + p.id.len() + 2 + p.name.len() + 1 + 1 + 1;
    }

    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(FrameType::S2sPortEnumResponse as u8);
    buf.put_u32(ports.len() as u32);

    for p in ports {
        buf.put_u16(p.id.len() as u16);
        buf.put_slice(p.id.as_bytes());
        buf.put_u16(p.name.len() as u16);
        buf.put_slice(p.name.as_bytes());
        buf.put_u8(match p.port_type {
            RemotePortType::Input => 0,
            RemotePortType::Output => 1,
        });
        buf.put_u8(u8::from(p.connected));
        buf.put_u8(u8::from(p.transmitting));
    }

    buf
}

/// Encode a peer status request frame.
pub fn encode_peer_status_request() -> BytesMut {
    let mut buf = BytesMut::with_capacity(1);
    buf.put_u8(FrameType::S2sPeerStatusRequest as u8);
    buf
}

/// Encode a peer status response frame.
pub fn encode_peer_status_response(load: f64, flow_files_queued: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + 8 + 8);
    buf.put_u8(FrameType::S2sPeerStatusResponse as u8);
    buf.put_f64(load);
    buf.put_u64(flow_files_queued);
    buf
}

/// Encode a transaction begin frame.
pub fn encode_txn_begin(port_id: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + 2 + port_id.len());
    buf.put_u8(FrameType::S2sTxnBegin as u8);
    buf.put_u16(port_id.len() as u16);
    buf.put_slice(port_id.as_bytes());
    buf
}

/// Encode a transaction commit frame.
pub fn encode_txn_commit(transaction_id: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + 2 + transaction_id.len());
    buf.put_u8(FrameType::S2sTxnCommit as u8);
    buf.put_u16(transaction_id.len() as u16);
    buf.put_slice(transaction_id.as_bytes());
    buf
}

/// Encode a transaction rollback frame.
pub fn encode_txn_rollback(transaction_id: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1 + 2 + transaction_id.len());
    buf.put_u8(FrameType::S2sTxnRollback as u8);
    buf.put_u16(transaction_id.len() as u16);
    buf.put_slice(transaction_id.as_bytes());
    buf
}

// ── Decoding ─────────────────────────────────────────────────────────────────

/// Decode a length-prefixed UTF-8 string (u16 length prefix) from a buffer.
fn decode_string(buf: &mut &[u8]) -> TransportResult<String> {
    if buf.remaining() < 2 {
        return Err(TransportError::Protocol("missing string length".into()));
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(TransportError::Protocol("truncated string".into()));
    }
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|e| TransportError::Protocol(format!("invalid UTF-8: {e}")))?
        .to_string();
    buf.advance(len);
    Ok(s)
}

/// Decode an S2S frame from raw bytes. The first byte must be the frame type.
pub fn decode_s2s_frame(data: &[u8]) -> TransportResult<S2sFrame> {
    if data.is_empty() {
        return Err(TransportError::Protocol("empty S2S frame".into()));
    }

    let frame_type = FrameType::from_u8(data[0])?;
    let mut buf = &data[1..];

    match frame_type {
        FrameType::S2sPortEnumRequest => Ok(S2sFrame::PortEnumRequest),

        FrameType::S2sPortEnumResponse => {
            if buf.remaining() < 4 {
                return Err(TransportError::Protocol("missing port count".into()));
            }
            let count = buf.get_u32() as usize;
            let mut ports = Vec::with_capacity(count);

            for _ in 0..count {
                let id = decode_string(&mut buf)?;
                let name = decode_string(&mut buf)?;

                if buf.remaining() < 3 {
                    return Err(TransportError::Protocol("truncated port entry".into()));
                }
                let port_type = match buf.get_u8() {
                    0 => RemotePortType::Input,
                    1 => RemotePortType::Output,
                    other => {
                        return Err(TransportError::Protocol(format!(
                            "unknown port type: {other}"
                        )));
                    }
                };
                let connected = buf.get_u8() != 0;
                let transmitting = buf.get_u8() != 0;

                ports.push(RemotePort {
                    id,
                    name,
                    port_type,
                    connected,
                    transmitting,
                });
            }

            Ok(S2sFrame::PortEnumResponse { ports })
        }

        FrameType::S2sPeerStatusRequest => Ok(S2sFrame::PeerStatusRequest),

        FrameType::S2sPeerStatusResponse => {
            if buf.remaining() < 16 {
                return Err(TransportError::Protocol(
                    "peer status response too short".into(),
                ));
            }
            let load = buf.get_f64();
            let flow_files_queued = buf.get_u64();
            Ok(S2sFrame::PeerStatusResponse {
                load,
                flow_files_queued,
            })
        }

        FrameType::S2sTxnBegin => {
            let port_id = decode_string(&mut buf)?;
            Ok(S2sFrame::TxnBegin { port_id })
        }

        FrameType::S2sTxnCommit => {
            let transaction_id = decode_string(&mut buf)?;
            Ok(S2sFrame::TxnCommit { transaction_id })
        }

        FrameType::S2sTxnRollback => {
            let transaction_id = decode_string(&mut buf)?;
            Ok(S2sFrame::TxnRollback { transaction_id })
        }

        _ => Err(TransportError::Protocol(format!(
            "not an S2S frame type: 0x{:02x}",
            frame_type as u8,
        ))),
    }
}

// ── Serde helper for Duration as milliseconds ────────────────────────────────

mod duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_enum_request_round_trip() {
        let encoded = encode_port_enum_request();
        let decoded = decode_s2s_frame(&encoded).unwrap();
        assert!(matches!(decoded, S2sFrame::PortEnumRequest));
    }

    #[test]
    fn port_enum_response_round_trip() {
        let ports = vec![
            RemotePort {
                id: "port-1".into(),
                name: "raw-input".into(),
                port_type: RemotePortType::Input,
                connected: true,
                transmitting: true,
            },
            RemotePort {
                id: "port-2".into(),
                name: "processed-output".into(),
                port_type: RemotePortType::Output,
                connected: false,
                transmitting: false,
            },
        ];

        let encoded = encode_port_enum_response(&ports);
        let decoded = decode_s2s_frame(&encoded).unwrap();

        match decoded {
            S2sFrame::PortEnumResponse {
                ports: decoded_ports,
            } => {
                assert_eq!(decoded_ports.len(), 2);
                assert_eq!(decoded_ports[0].id, "port-1");
                assert_eq!(decoded_ports[0].name, "raw-input");
                assert_eq!(decoded_ports[0].port_type, RemotePortType::Input);
                assert!(decoded_ports[0].connected);
                assert!(decoded_ports[0].transmitting);
                assert_eq!(decoded_ports[1].id, "port-2");
                assert_eq!(decoded_ports[1].port_type, RemotePortType::Output);
                assert!(!decoded_ports[1].connected);
            }
            _ => panic!("expected PortEnumResponse"),
        }
    }

    #[test]
    fn port_enum_response_empty_round_trip() {
        let encoded = encode_port_enum_response(&[]);
        let decoded = decode_s2s_frame(&encoded).unwrap();
        match decoded {
            S2sFrame::PortEnumResponse { ports } => assert!(ports.is_empty()),
            _ => panic!("expected PortEnumResponse"),
        }
    }

    #[test]
    fn peer_status_request_round_trip() {
        let encoded = encode_peer_status_request();
        let decoded = decode_s2s_frame(&encoded).unwrap();
        assert!(matches!(decoded, S2sFrame::PeerStatusRequest));
    }

    #[test]
    fn peer_status_response_round_trip() {
        let encoded = encode_peer_status_response(0.75, 12345);
        let decoded = decode_s2s_frame(&encoded).unwrap();
        match decoded {
            S2sFrame::PeerStatusResponse {
                load,
                flow_files_queued,
            } => {
                assert!((load - 0.75).abs() < f64::EPSILON);
                assert_eq!(flow_files_queued, 12345);
            }
            _ => panic!("expected PeerStatusResponse"),
        }
    }

    #[test]
    fn txn_begin_round_trip() {
        let encoded = encode_txn_begin("port-abc-123");
        let decoded = decode_s2s_frame(&encoded).unwrap();
        match decoded {
            S2sFrame::TxnBegin { port_id } => assert_eq!(port_id, "port-abc-123"),
            _ => panic!("expected TxnBegin"),
        }
    }

    #[test]
    fn txn_commit_round_trip() {
        let encoded = encode_txn_commit("txn-001");
        let decoded = decode_s2s_frame(&encoded).unwrap();
        match decoded {
            S2sFrame::TxnCommit { transaction_id } => assert_eq!(transaction_id, "txn-001"),
            _ => panic!("expected TxnCommit"),
        }
    }

    #[test]
    fn txn_rollback_round_trip() {
        let encoded = encode_txn_rollback("txn-002");
        let decoded = decode_s2s_frame(&encoded).unwrap();
        match decoded {
            S2sFrame::TxnRollback { transaction_id } => assert_eq!(transaction_id, "txn-002"),
            _ => panic!("expected TxnRollback"),
        }
    }

    #[test]
    fn s2s_config_default() {
        let config = S2sConfig::default();
        assert_eq!(config.transport_protocol, TransportProtocol::Quic);
        assert_eq!(config.communications_timeout, Duration::from_secs(30));
        assert_eq!(config.batch_count, 100);
    }

    #[test]
    fn empty_frame_returns_error() {
        let result = decode_s2s_frame(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn non_s2s_frame_returns_error() {
        // FrameType::Handshake = 0x01 is not an S2S frame
        let result = decode_s2s_frame(&[0x01]);
        assert!(result.is_err());
    }
}
