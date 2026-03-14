//! Binary encode/decode for provenance event records.
//!
//! ## Segment file format
//! ```text
//! HEADER: [8 bytes: "RNFPRV01"]
//! RECORDS (repeated):
//!   [1B tag=0x10] [4B payload_len (u32 LE)] [N bytes payload] [4B CRC32]
//! ```
//!
//! Payload layout for a ProvenanceEvent:
//! ```text
//! [8B event_id] [8B flowfile_id] [1B event_type_tag] [8B timestamp_nanos]
//! [8B content_size] [8B lineage_start_id]
//! [4B processor_name_len] [N bytes processor_name]
//! [4B processor_type_len] [N bytes processor_type]
//! [4B attr_count] [repeated: [4B key_len] [key] [4B val_len] [val]]
//! [1B has_relationship] [optional: [4B rel_len] [rel]]
//! [1B has_source_ffid] [optional: 8B source_flowfile_id]
//! [4B details_len] [N bytes details]
//! [4B parent_count] [repeated: 8B parent_id]
//! [4B child_count] [repeated: 8B child_id]
//! [1B has_transit_uri] [optional: [4B uri_len] [uri]]
//! ```

use crate::error::{Result, RuniFiError};
use crate::repository::provenance_repo::{ProvenanceEvent, ProvenanceEventType};

pub const PROVENANCE_MAGIC: &[u8; 8] = b"RNFPRV01";
pub const TAG_PROVENANCE_EVENT: u8 = 0x10;

/// Encode a provenance event into binary payload bytes.
pub fn encode_provenance_event(event: &ProvenanceEvent) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(&event.event_id.to_le_bytes());
    buf.extend_from_slice(&event.flowfile_id.to_le_bytes());
    buf.push(event.event_type.to_tag());
    buf.extend_from_slice(&event.timestamp_nanos.to_le_bytes());
    buf.extend_from_slice(&event.content_size.to_le_bytes());
    buf.extend_from_slice(&event.lineage_start_id.to_le_bytes());

    // Processor name.
    let name_bytes = event.processor_name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);

    // Processor type.
    let type_bytes = event.processor_type.as_bytes();
    buf.extend_from_slice(&(type_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(type_bytes);

    // Attributes.
    buf.extend_from_slice(&(event.attributes.len() as u32).to_le_bytes());
    for (k, v) in &event.attributes {
        let kb = k.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        let vb = v.as_bytes();
        buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        buf.extend_from_slice(vb);
    }

    // Relationship.
    match &event.relationship {
        Some(rel) => {
            buf.push(1);
            let rb = rel.as_bytes();
            buf.extend_from_slice(&(rb.len() as u32).to_le_bytes());
            buf.extend_from_slice(rb);
        }
        None => buf.push(0),
    }

    // Source FlowFile ID.
    match event.source_flowfile_id {
        Some(id) => {
            buf.push(1);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        None => buf.push(0),
    }

    // Details.
    let details_bytes = event.details.as_bytes();
    buf.extend_from_slice(&(details_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(details_bytes);

    // Parent FlowFile IDs.
    buf.extend_from_slice(&(event.parent_flowfile_ids.len() as u32).to_le_bytes());
    for &pid in &event.parent_flowfile_ids {
        buf.extend_from_slice(&pid.to_le_bytes());
    }

    // Child FlowFile IDs.
    buf.extend_from_slice(&(event.child_flowfile_ids.len() as u32).to_le_bytes());
    for &cid in &event.child_flowfile_ids {
        buf.extend_from_slice(&cid.to_le_bytes());
    }

    // Transit URI.
    match &event.transit_uri {
        Some(uri) => {
            buf.push(1);
            let ub = uri.as_bytes();
            buf.extend_from_slice(&(ub.len() as u32).to_le_bytes());
            buf.extend_from_slice(ub);
        }
        None => buf.push(0),
    }

    // Content claim ID.
    match event.content_claim_id {
        Some(id) => {
            buf.push(1);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        None => buf.push(0),
    }

    // Previous attributes.
    buf.extend_from_slice(&(event.previous_attributes.len() as u32).to_le_bytes());
    for (k, v) in &event.previous_attributes {
        let kb = k.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        let vb = v.as_bytes();
        buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        buf.extend_from_slice(vb);
    }

    buf
}

/// Decode a provenance event from binary payload bytes.
pub fn decode_provenance_event(data: &[u8]) -> Result<ProvenanceEvent> {
    let mut pos = 0;

    let read_u64 = |pos: &mut usize| -> Result<u64> {
        if *pos + 8 > data.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of payload".into(),
            });
        }
        let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Ok(val)
    };

    let read_u32 = |pos: &mut usize| -> Result<u32> {
        if *pos + 4 > data.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of payload".into(),
            });
        }
        let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Ok(val)
    };

    let read_u8 = |pos: &mut usize| -> Result<u8> {
        if *pos >= data.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of payload".into(),
            });
        }
        let val = data[*pos];
        *pos += 1;
        Ok(val)
    };

    let read_string = |pos: &mut usize| -> Result<String> {
        let len = read_u32(pos)? as usize;
        if *pos + len > data.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "string truncated".into(),
            });
        }
        let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
        *pos += len;
        Ok(s)
    };

    let event_id = read_u64(&mut pos)?;
    let flowfile_id = read_u64(&mut pos)?;
    let event_type_tag = read_u8(&mut pos)?;
    let event_type = ProvenanceEventType::from_tag(event_type_tag).ok_or_else(|| {
        RuniFiError::ProvenanceCorrupted {
            offset: pos as u64,
            reason: format!("unknown event type tag: {event_type_tag}"),
        }
    })?;
    let timestamp_nanos = read_u64(&mut pos)?;
    let content_size = read_u64(&mut pos)?;
    let lineage_start_id = read_u64(&mut pos)?;

    let processor_name = read_string(&mut pos)?;
    let processor_type = read_string(&mut pos)?;

    let attr_count = read_u32(&mut pos)? as usize;
    let mut attributes = Vec::with_capacity(attr_count);
    for _ in 0..attr_count {
        let key = read_string(&mut pos)?;
        let val = read_string(&mut pos)?;
        attributes.push((key, val));
    }

    let has_rel = read_u8(&mut pos)?;
    let relationship = if has_rel == 1 {
        Some(read_string(&mut pos)?)
    } else {
        None
    };

    let has_source = read_u8(&mut pos)?;
    let source_flowfile_id = if has_source == 1 {
        Some(read_u64(&mut pos)?)
    } else {
        None
    };

    let details = read_string(&mut pos)?;

    let parent_count = read_u32(&mut pos)? as usize;
    let mut parent_flowfile_ids = Vec::with_capacity(parent_count);
    for _ in 0..parent_count {
        parent_flowfile_ids.push(read_u64(&mut pos)?);
    }

    let child_count = read_u32(&mut pos)? as usize;
    let mut child_flowfile_ids = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        child_flowfile_ids.push(read_u64(&mut pos)?);
    }

    let has_transit = read_u8(&mut pos)?;
    let transit_uri = if has_transit == 1 {
        Some(read_string(&mut pos)?)
    } else {
        None
    };

    // Content claim ID (added in extended format).
    let content_claim_id = if pos < data.len() {
        let has_claim = read_u8(&mut pos)?;
        if has_claim == 1 {
            Some(read_u64(&mut pos)?)
        } else {
            None
        }
    } else {
        None
    };

    // Previous attributes (added in extended format).
    let previous_attributes = if pos < data.len() {
        let prev_count = read_u32(&mut pos)? as usize;
        let mut prev = Vec::with_capacity(prev_count);
        for _ in 0..prev_count {
            let key = read_string(&mut pos)?;
            let val = read_string(&mut pos)?;
            prev.push((key, val));
        }
        prev
    } else {
        Vec::new()
    };

    Ok(ProvenanceEvent {
        event_id,
        flowfile_id,
        event_type,
        processor_name,
        processor_type,
        timestamp_nanos,
        attributes,
        content_size,
        lineage_start_id,
        relationship,
        source_flowfile_id,
        details,
        parent_flowfile_ids,
        child_flowfile_ids,
        transit_uri,
        content_claim_id,
        previous_attributes,
    })
}

/// Write a provenance segment file header.
pub fn write_provenance_header(writer: &mut impl std::io::Write) -> std::io::Result<()> {
    writer.write_all(PROVENANCE_MAGIC)
}

/// Validate and skip the provenance segment file header.
pub fn read_provenance_header(reader: &mut impl std::io::Read) -> Result<bool> {
    let mut buf = [0u8; 8];
    match reader.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(RuniFiError::Io(e)),
    }
    Ok(buf == *PROVENANCE_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event() -> ProvenanceEvent {
        ProvenanceEvent {
            event_id: 42,
            flowfile_id: 100,
            event_type: ProvenanceEventType::Create,
            processor_name: "gen-test".to_string(),
            processor_type: "GenerateFlowFile".to_string(),
            timestamp_nanos: 1_700_000_000_000_000_000,
            attributes: vec![
                ("filename".to_string(), "test.txt".to_string()),
                ("path".to_string(), "/tmp".to_string()),
            ],
            content_size: 5120,
            lineage_start_id: 100,
            relationship: Some("success".to_string()),
            source_flowfile_id: None,
            details: "Generated test data".to_string(),
            parent_flowfile_ids: Vec::new(),
            child_flowfile_ids: Vec::new(),
            transit_uri: None,
            content_claim_id: None,
            previous_attributes: Vec::new(),
        }
    }

    #[test]
    fn encode_decode_roundtrip_basic() {
        let event = make_test_event();
        let payload = encode_provenance_event(&event);
        let decoded = decode_provenance_event(&payload).unwrap();

        assert_eq!(decoded.event_id, 42);
        assert_eq!(decoded.flowfile_id, 100);
        assert_eq!(decoded.event_type, ProvenanceEventType::Create);
        assert_eq!(decoded.processor_name, "gen-test");
        assert_eq!(decoded.processor_type, "GenerateFlowFile");
        assert_eq!(decoded.timestamp_nanos, 1_700_000_000_000_000_000);
        assert_eq!(decoded.attributes.len(), 2);
        assert_eq!(
            decoded.attributes[0],
            ("filename".into(), "test.txt".into())
        );
        assert_eq!(decoded.content_size, 5120);
        assert_eq!(decoded.lineage_start_id, 100);
        assert_eq!(decoded.relationship.as_deref(), Some("success"));
        assert!(decoded.source_flowfile_id.is_none());
        assert_eq!(decoded.details, "Generated test data");
    }

    #[test]
    fn encode_decode_roundtrip_all_event_types() {
        for et in ProvenanceEventType::all() {
            let mut event = make_test_event();
            event.event_type = *et;
            let payload = encode_provenance_event(&event);
            let decoded = decode_provenance_event(&payload).unwrap();
            assert_eq!(decoded.event_type, *et);
        }
    }

    #[test]
    fn encode_decode_with_fork_join_fields() {
        let mut event = make_test_event();
        event.event_type = ProvenanceEventType::Fork;
        event.parent_flowfile_ids = vec![10, 20];
        event.child_flowfile_ids = vec![30, 40, 50];
        event.source_flowfile_id = Some(10);
        event.transit_uri = Some("nifi://host:8080".to_string());

        let payload = encode_provenance_event(&event);
        let decoded = decode_provenance_event(&payload).unwrap();

        assert_eq!(decoded.parent_flowfile_ids, vec![10, 20]);
        assert_eq!(decoded.child_flowfile_ids, vec![30, 40, 50]);
        assert_eq!(decoded.source_flowfile_id, Some(10));
        assert_eq!(decoded.transit_uri.as_deref(), Some("nifi://host:8080"));
    }

    #[test]
    fn encode_decode_unicode_attributes() {
        let mut event = make_test_event();
        event.attributes = vec![("名前".to_string(), "テスト🚀".to_string())];

        let payload = encode_provenance_event(&event);
        let decoded = decode_provenance_event(&payload).unwrap();
        assert_eq!(decoded.attributes[0].0, "名前");
        assert_eq!(decoded.attributes[0].1, "テスト🚀");
    }

    #[test]
    fn decode_truncated_payload_returns_error() {
        let event = make_test_event();
        let payload = encode_provenance_event(&event);
        // Truncate to just first 10 bytes.
        let result = decode_provenance_event(&payload[..10]);
        assert!(result.is_err());
    }

    #[test]
    fn encode_decode_with_content_claim_and_previous_attrs() {
        let mut event = make_test_event();
        event.content_claim_id = Some(42);
        event.previous_attributes = vec![
            ("filename".to_string(), "old.txt".to_string()),
            ("path".to_string(), "/old".to_string()),
        ];

        let payload = encode_provenance_event(&event);
        let decoded = decode_provenance_event(&payload).unwrap();

        assert_eq!(decoded.content_claim_id, Some(42));
        assert_eq!(decoded.previous_attributes.len(), 2);
        assert_eq!(
            decoded.previous_attributes[0],
            ("filename".into(), "old.txt".into())
        );
    }

    #[test]
    fn provenance_header_roundtrip() {
        let mut buf = Vec::new();
        write_provenance_header(&mut buf).unwrap();
        assert_eq!(buf.len(), 8);

        let mut reader = std::io::Cursor::new(&buf);
        assert!(read_provenance_header(&mut reader).unwrap());
    }
}
