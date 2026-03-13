//! Wire protocol for QUIC-based FlowFile transfers.
//!
//! Protocol framing:
//! - Each FlowFile transfer uses one bidirectional QUIC stream.
//! - Sender writes a handshake, then FlowFile frames.
//! - Receiver reads frames and sends acknowledgments.
//!
//! Wire format for a FlowFile frame:
//! ```text
//! [frame_type: u8]
//! [attribute_count: u16 BE]
//! For each attribute:
//!   [key_len: u16 BE] [key_bytes]
//!   [value_len: u16 BE] [value_bytes]
//! [content_length: u64 BE]
//! [content_bytes]
//! [blake3_hash: 32 bytes]
//! ```

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{TransportError, TransportResult};

/// Protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Magic bytes identifying RuniFi protocol.
pub const MAGIC: &[u8; 4] = b"RNFI";

/// Frame types.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// Protocol handshake.
    Handshake = 0x01,
    /// FlowFile data frame.
    FlowFile = 0x02,
    /// Acknowledgment.
    Ack = 0x03,
    /// Back-pressure signal.
    BackPressure = 0x04,
    /// Batch of small FlowFiles (inline stream).
    FlowFileBatch = 0x05,
    /// End of stream marker.
    EndOfStream = 0xFF,
}

impl FrameType {
    pub fn from_u8(v: u8) -> TransportResult<Self> {
        match v {
            0x01 => Ok(Self::Handshake),
            0x02 => Ok(Self::FlowFile),
            0x03 => Ok(Self::Ack),
            0x04 => Ok(Self::BackPressure),
            0x05 => Ok(Self::FlowFileBatch),
            0xFF => Ok(Self::EndOfStream),
            other => Err(TransportError::Protocol(format!(
                "unknown frame type: 0x{other:02x}"
            ))),
        }
    }
}

/// Capability flags exchanged during handshake.
#[derive(Debug, Clone, Copy, Default)]
pub struct Capabilities {
    /// Supports inline stream (small file batching).
    pub inline_stream: bool,
    /// Supports chunked streaming.
    pub chunked_stream: bool,
    /// Supports zero-copy transfer.
    pub zero_copy: bool,
    /// Supports resumable transfers.
    pub resumable: bool,
}

impl Capabilities {
    pub fn all() -> Self {
        Self {
            inline_stream: true,
            chunked_stream: true,
            zero_copy: true,
            resumable: true,
        }
    }

    pub fn to_bits(self) -> u32 {
        let mut bits = 0u32;
        if self.inline_stream {
            bits |= 1 << 0;
        }
        if self.chunked_stream {
            bits |= 1 << 1;
        }
        if self.zero_copy {
            bits |= 1 << 2;
        }
        if self.resumable {
            bits |= 1 << 3;
        }
        bits
    }

    pub fn from_bits(bits: u32) -> Self {
        Self {
            inline_stream: bits & (1 << 0) != 0,
            chunked_stream: bits & (1 << 1) != 0,
            zero_copy: bits & (1 << 2) != 0,
            resumable: bits & (1 << 3) != 0,
        }
    }
}

/// Acknowledgment status.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Success = 0,
    Failure = 1,
    BackPressure = 2,
}

impl AckStatus {
    pub fn from_u8(v: u8) -> TransportResult<Self> {
        match v {
            0 => Ok(Self::Success),
            1 => Ok(Self::Failure),
            2 => Ok(Self::BackPressure),
            other => Err(TransportError::Protocol(format!(
                "unknown ack status: {other}"
            ))),
        }
    }
}

/// A serializable FlowFile representation for transport.
#[derive(Debug, Clone)]
pub struct WireFlowFile {
    pub attributes: Vec<(Arc<str>, Arc<str>)>,
    pub content: Bytes,
}

// --- Encoding ---

/// Encode a handshake frame.
pub fn encode_handshake(capabilities: Capabilities) -> BytesMut {
    let mut buf = BytesMut::with_capacity(11);
    buf.put_u8(FrameType::Handshake as u8);
    buf.put_slice(MAGIC);
    buf.put_u16(PROTOCOL_VERSION);
    buf.put_u32(capabilities.to_bits());
    buf
}

/// Encode a single FlowFile frame.
pub fn encode_flowfile(ff: &WireFlowFile) -> BytesMut {
    let attr_size: usize = ff
        .attributes
        .iter()
        .map(|(k, v)| 4 + k.len() + v.len()) // 2 bytes key_len + 2 bytes val_len
        .sum();

    let total = 1 + 2 + attr_size + 8 + ff.content.len() + 32;
    let mut buf = BytesMut::with_capacity(total);

    buf.put_u8(FrameType::FlowFile as u8);

    // Attributes
    buf.put_u16(ff.attributes.len() as u16);
    for (key, val) in &ff.attributes {
        buf.put_u16(key.len() as u16);
        buf.put_slice(key.as_bytes());
        buf.put_u16(val.len() as u16);
        buf.put_slice(val.as_bytes());
    }

    // Content
    buf.put_u64(ff.content.len() as u64);
    buf.put_slice(&ff.content);

    // Integrity hash
    let hash = blake3::hash(&ff.content);
    buf.put_slice(hash.as_bytes());

    buf
}

/// Encode a batch of small FlowFiles (inline stream strategy).
pub fn encode_flowfile_batch(files: &[WireFlowFile]) -> BytesMut {
    // Calculate total size
    let mut total = 1 + 4; // frame_type + count
    for ff in files {
        let attr_size: usize = ff
            .attributes
            .iter()
            .map(|(k, v)| 4 + k.len() + v.len())
            .sum();
        total += 2 + attr_size + 8 + ff.content.len() + 32;
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(FrameType::FlowFileBatch as u8);
    buf.put_u32(files.len() as u32);

    for ff in files {
        // Attributes
        buf.put_u16(ff.attributes.len() as u16);
        for (key, val) in &ff.attributes {
            buf.put_u16(key.len() as u16);
            buf.put_slice(key.as_bytes());
            buf.put_u16(val.len() as u16);
            buf.put_slice(val.as_bytes());
        }

        // Content
        buf.put_u64(ff.content.len() as u64);
        buf.put_slice(&ff.content);

        // Hash
        let hash = blake3::hash(&ff.content);
        buf.put_slice(hash.as_bytes());
    }

    buf
}

/// Encode an acknowledgment.
pub fn encode_ack(status: AckStatus, message: Option<&str>) -> BytesMut {
    let msg_bytes = message.map(|m| m.as_bytes()).unwrap_or(&[]);
    let mut buf = BytesMut::with_capacity(1 + 1 + 2 + msg_bytes.len());
    buf.put_u8(FrameType::Ack as u8);
    buf.put_u8(status as u8);
    buf.put_u16(msg_bytes.len() as u16);
    buf.put_slice(msg_bytes);
    buf
}

/// Encode a back-pressure signal.
pub fn encode_back_pressure(available_capacity: u64) -> BytesMut {
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(FrameType::BackPressure as u8);
    buf.put_u64(available_capacity);
    buf
}

/// Encode end-of-stream marker.
pub fn encode_end_of_stream() -> BytesMut {
    let mut buf = BytesMut::with_capacity(1);
    buf.put_u8(FrameType::EndOfStream as u8);
    buf
}

// --- Decoding ---

/// Decoded handshake data.
#[derive(Debug)]
pub struct Handshake {
    pub version: u16,
    pub capabilities: Capabilities,
}

/// Decoded acknowledgment.
#[derive(Debug)]
pub struct Ack {
    pub status: AckStatus,
    pub message: Option<String>,
}

/// A decoded frame from the wire.
#[derive(Debug)]
pub enum DecodedFrame {
    Handshake(Handshake),
    FlowFile(WireFlowFile),
    FlowFileBatch(Vec<WireFlowFile>),
    Ack(Ack),
    BackPressure { available_capacity: u64 },
    EndOfStream,
}

/// Decode a handshake from raw bytes.
pub fn decode_handshake(data: &[u8]) -> TransportResult<Handshake> {
    if data.len() < 10 {
        return Err(TransportError::Protocol("handshake too short".into()));
    }

    let mut buf = data;

    // Frame type already consumed by caller, or included
    if buf[0] == FrameType::Handshake as u8 {
        buf = &buf[1..];
    }

    if &buf[..4] != MAGIC {
        return Err(TransportError::Handshake(format!(
            "invalid magic: expected RNFI, got {:?}",
            &buf[..4]
        )));
    }
    buf = &buf[4..];

    let version = u16::from_be_bytes([buf[0], buf[1]]);
    buf = &buf[2..];

    if version != PROTOCOL_VERSION {
        return Err(TransportError::UnsupportedVersion(version));
    }

    let cap_bits = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let capabilities = Capabilities::from_bits(cap_bits);

    Ok(Handshake {
        version,
        capabilities,
    })
}

/// Decode attributes from a buffer, advancing the cursor.
fn decode_attributes(buf: &mut &[u8]) -> TransportResult<Vec<(Arc<str>, Arc<str>)>> {
    if buf.remaining() < 2 {
        return Err(TransportError::Protocol("missing attribute count".into()));
    }
    let count = buf.get_u16() as usize;
    let mut attrs = Vec::with_capacity(count);

    for _ in 0..count {
        if buf.remaining() < 2 {
            return Err(TransportError::Protocol(
                "truncated attribute key length".into(),
            ));
        }
        let key_len = buf.get_u16() as usize;
        if buf.remaining() < key_len {
            return Err(TransportError::Protocol("truncated attribute key".into()));
        }
        let key = std::str::from_utf8(&buf[..key_len])
            .map_err(|e| TransportError::Protocol(format!("invalid UTF-8 in key: {e}")))?;
        let key: Arc<str> = Arc::from(key);
        buf.advance(key_len);

        if buf.remaining() < 2 {
            return Err(TransportError::Protocol(
                "truncated attribute value length".into(),
            ));
        }
        let val_len = buf.get_u16() as usize;
        if buf.remaining() < val_len {
            return Err(TransportError::Protocol("truncated attribute value".into()));
        }
        let val = std::str::from_utf8(&buf[..val_len])
            .map_err(|e| TransportError::Protocol(format!("invalid UTF-8 in value: {e}")))?;
        let val: Arc<str> = Arc::from(val);
        buf.advance(val_len);

        attrs.push((key, val));
    }

    Ok(attrs)
}

/// Decode a single FlowFile from a buffer, verifying integrity.
fn decode_single_flowfile(buf: &mut &[u8]) -> TransportResult<WireFlowFile> {
    let attributes = decode_attributes(buf)?;

    if buf.remaining() < 8 {
        return Err(TransportError::Protocol("missing content length".into()));
    }
    let content_len = buf.get_u64() as usize;
    if buf.remaining() < content_len {
        return Err(TransportError::Protocol("truncated content".into()));
    }
    let content = Bytes::copy_from_slice(&buf[..content_len]);
    buf.advance(content_len);

    if buf.remaining() < 32 {
        return Err(TransportError::Protocol("missing blake3 hash".into()));
    }
    let mut expected_hash = [0u8; 32];
    expected_hash.copy_from_slice(&buf[..32]);
    buf.advance(32);

    // Verify integrity
    let actual_hash = blake3::hash(&content);
    if actual_hash.as_bytes() != &expected_hash {
        return Err(TransportError::IntegrityMismatch {
            expected: hex::encode(expected_hash),
            actual: actual_hash.to_hex().to_string(),
        });
    }

    Ok(WireFlowFile {
        attributes,
        content,
    })
}

/// Decode a frame from raw bytes. The first byte must be the frame type.
pub fn decode_frame(data: &[u8]) -> TransportResult<DecodedFrame> {
    if data.is_empty() {
        return Err(TransportError::Protocol("empty frame".into()));
    }

    let frame_type = FrameType::from_u8(data[0])?;
    let mut buf = &data[1..];

    match frame_type {
        FrameType::Handshake => {
            if buf.remaining() < 10 {
                return Err(TransportError::Protocol("handshake too short".into()));
            }
            if &buf[..4] != MAGIC {
                return Err(TransportError::Handshake("invalid magic".into()));
            }
            buf.advance(4);
            let version = buf.get_u16();
            if version != PROTOCOL_VERSION {
                return Err(TransportError::UnsupportedVersion(version));
            }
            let cap_bits = buf.get_u32();
            Ok(DecodedFrame::Handshake(Handshake {
                version,
                capabilities: Capabilities::from_bits(cap_bits),
            }))
        }
        FrameType::FlowFile => {
            let ff = decode_single_flowfile(&mut buf)?;
            Ok(DecodedFrame::FlowFile(ff))
        }
        FrameType::FlowFileBatch => {
            if buf.remaining() < 4 {
                return Err(TransportError::Protocol("missing batch count".into()));
            }
            let count = buf.get_u32() as usize;
            let mut files = Vec::with_capacity(count);
            for _ in 0..count {
                files.push(decode_single_flowfile(&mut buf)?);
            }
            Ok(DecodedFrame::FlowFileBatch(files))
        }
        FrameType::Ack => {
            if buf.remaining() < 3 {
                return Err(TransportError::Protocol("ack too short".into()));
            }
            let status = AckStatus::from_u8(buf.get_u8())?;
            let msg_len = buf.get_u16() as usize;
            let message = if msg_len > 0 {
                if buf.remaining() < msg_len {
                    return Err(TransportError::Protocol("truncated ack message".into()));
                }
                let msg = std::str::from_utf8(&buf[..msg_len])
                    .map_err(|e| TransportError::Protocol(format!("invalid ack message: {e}")))?
                    .to_string();
                buf.advance(msg_len);
                Some(msg)
            } else {
                None
            };
            Ok(DecodedFrame::Ack(Ack { status, message }))
        }
        FrameType::BackPressure => {
            if buf.remaining() < 8 {
                return Err(TransportError::Protocol("back-pressure too short".into()));
            }
            let available_capacity = buf.get_u64();
            Ok(DecodedFrame::BackPressure { available_capacity })
        }
        FrameType::EndOfStream => Ok(DecodedFrame::EndOfStream),
    }
}

/// Conversion helpers between FlowFile types.
impl WireFlowFile {
    /// Create a WireFlowFile from plugin-api FlowFile with content bytes.
    pub fn from_flowfile(ff: &runifi_plugin_api::FlowFile, content: Bytes) -> Self {
        WireFlowFile {
            attributes: ff.attributes.clone(),
            content,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trip() {
        let caps = Capabilities::all();
        let encoded = encode_handshake(caps);
        let decoded = decode_frame(&encoded).unwrap();
        match decoded {
            DecodedFrame::Handshake(h) => {
                assert_eq!(h.version, PROTOCOL_VERSION);
                assert!(h.capabilities.inline_stream);
                assert!(h.capabilities.chunked_stream);
                assert!(h.capabilities.zero_copy);
                assert!(h.capabilities.resumable);
            }
            _ => panic!("expected Handshake frame"),
        }
    }

    #[test]
    fn flowfile_round_trip() {
        let ff = WireFlowFile {
            attributes: vec![
                (Arc::from("filename"), Arc::from("test.txt")),
                (Arc::from("mime.type"), Arc::from("text/plain")),
            ],
            content: Bytes::from_static(b"hello world"),
        };

        let encoded = encode_flowfile(&ff);
        let decoded = decode_frame(&encoded).unwrap();

        match decoded {
            DecodedFrame::FlowFile(decoded_ff) => {
                assert_eq!(decoded_ff.attributes.len(), 2);
                assert_eq!(decoded_ff.attributes[0].0.as_ref(), "filename");
                assert_eq!(decoded_ff.attributes[0].1.as_ref(), "test.txt");
                assert_eq!(decoded_ff.content, Bytes::from_static(b"hello world"));
            }
            _ => panic!("expected FlowFile frame"),
        }
    }

    #[test]
    fn flowfile_batch_round_trip() {
        let files = vec![
            WireFlowFile {
                attributes: vec![(Arc::from("id"), Arc::from("1"))],
                content: Bytes::from_static(b"data1"),
            },
            WireFlowFile {
                attributes: vec![(Arc::from("id"), Arc::from("2"))],
                content: Bytes::from_static(b"data2"),
            },
            WireFlowFile {
                attributes: vec![(Arc::from("id"), Arc::from("3"))],
                content: Bytes::from_static(b"data3"),
            },
        ];

        let encoded = encode_flowfile_batch(&files);
        let decoded = decode_frame(&encoded).unwrap();

        match decoded {
            DecodedFrame::FlowFileBatch(batch) => {
                assert_eq!(batch.len(), 3);
                assert_eq!(batch[0].content, Bytes::from_static(b"data1"));
                assert_eq!(batch[1].content, Bytes::from_static(b"data2"));
                assert_eq!(batch[2].content, Bytes::from_static(b"data3"));
            }
            _ => panic!("expected FlowFileBatch frame"),
        }
    }

    #[test]
    fn ack_round_trip() {
        let encoded = encode_ack(AckStatus::Success, None);
        let decoded = decode_frame(&encoded).unwrap();
        match decoded {
            DecodedFrame::Ack(ack) => {
                assert_eq!(ack.status, AckStatus::Success);
                assert!(ack.message.is_none());
            }
            _ => panic!("expected Ack frame"),
        }

        let encoded = encode_ack(AckStatus::Failure, Some("disk full"));
        let decoded = decode_frame(&encoded).unwrap();
        match decoded {
            DecodedFrame::Ack(ack) => {
                assert_eq!(ack.status, AckStatus::Failure);
                assert_eq!(ack.message.as_deref(), Some("disk full"));
            }
            _ => panic!("expected Ack frame"),
        }
    }

    #[test]
    fn back_pressure_round_trip() {
        let encoded = encode_back_pressure(42000);
        let decoded = decode_frame(&encoded).unwrap();
        match decoded {
            DecodedFrame::BackPressure { available_capacity } => {
                assert_eq!(available_capacity, 42000);
            }
            _ => panic!("expected BackPressure frame"),
        }
    }

    #[test]
    fn end_of_stream_round_trip() {
        let encoded = encode_end_of_stream();
        let decoded = decode_frame(&encoded).unwrap();
        assert!(matches!(decoded, DecodedFrame::EndOfStream));
    }

    #[test]
    fn integrity_mismatch_detected() {
        let ff = WireFlowFile {
            attributes: vec![],
            content: Bytes::from_static(b"good data"),
        };
        let mut encoded = encode_flowfile(&ff);

        // Corrupt the content (byte after frame_type + attr_count + content_length)
        let content_start = 1 + 2 + 8; // frame type + 0 attrs (2 bytes) + content len (8 bytes)
        encoded[content_start] ^= 0xFF;

        let result = decode_frame(&encoded);
        assert!(result.is_err());
        match result.unwrap_err() {
            TransportError::IntegrityMismatch { .. } => {}
            other => panic!("expected IntegrityMismatch, got: {other}"),
        }
    }

    #[test]
    fn capabilities_bits_round_trip() {
        let caps = Capabilities {
            inline_stream: true,
            chunked_stream: false,
            zero_copy: true,
            resumable: false,
        };
        let bits = caps.to_bits();
        let decoded = Capabilities::from_bits(bits);
        assert!(decoded.inline_stream);
        assert!(!decoded.chunked_stream);
        assert!(decoded.zero_copy);
        assert!(!decoded.resumable);
    }

    #[test]
    fn empty_flowfile_round_trip() {
        let ff = WireFlowFile {
            attributes: vec![],
            content: Bytes::new(),
        };

        let encoded = encode_flowfile(&ff);
        let decoded = decode_frame(&encoded).unwrap();

        match decoded {
            DecodedFrame::FlowFile(decoded_ff) => {
                assert!(decoded_ff.attributes.is_empty());
                assert!(decoded_ff.content.is_empty());
            }
            _ => panic!("expected FlowFile frame"),
        }
    }
}
