//! Per-segment in-memory index for fast provenance event lookups.
//!
//! `SegmentIndex` is built incrementally while writing a segment, or
//! reconstructed when recovering from disk. It tracks event offsets,
//! time/ID ranges, and secondary indexes for processor, FlowFile, event
//! type, and lineage lookups.
//!
//! ## Index file format (`segment-NNNNNNNN.idx`)
//! ```text
//! HEADER: [8B "RNFIDX01"] [4B version=1] [8B segment_id] [8B event_count]
//!         [8B min_timestamp] [8B max_timestamp] [8B min_event_id] [8B max_event_id]
//! BODY:   (serialized HashMaps using length-prefixed entries)
//! FOOTER: [4B CRC32 over header+body]
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::error::{Result, RuniFiError};
use crate::repository::provenance_repo::{ProvenanceEvent, ProvenanceEventType};

const INDEX_MAGIC: &[u8; 8] = b"RNFIDX01";
const INDEX_VERSION: u32 = 1;

/// In-memory index for a segment of provenance events.
/// Built incrementally while writing, or reconstructed from a segment file.
#[derive(Debug, Default)]
pub struct SegmentIndex {
    /// Segment ID (matches the numeric suffix of the segment filename).
    pub segment_id: u64,
    /// Number of events in this segment.
    pub event_count: u64,
    /// Minimum timestamp (nanos since epoch) of any event in this segment.
    pub min_timestamp: u64,
    /// Maximum timestamp (nanos since epoch) of any event in this segment.
    pub max_timestamp: u64,
    /// Minimum event ID in this segment.
    pub min_event_id: u64,
    /// Maximum event ID in this segment.
    pub max_event_id: u64,
    /// Event ID -> file offset (byte position after header).
    pub event_offsets: HashMap<u64, u64>,
    /// Processor name -> list of event IDs.
    pub by_processor: HashMap<String, Vec<u64>>,
    /// FlowFile ID -> list of event IDs.
    pub by_flowfile: HashMap<u64, Vec<u64>>,
    /// Event type -> list of event IDs.
    pub by_event_type: HashMap<ProvenanceEventType, Vec<u64>>,
    /// Lineage start ID -> list of event IDs.
    pub by_lineage: HashMap<u64, Vec<u64>>,
    /// Content claim IDs referenced in this segment (for unpinning on prune).
    pub content_claim_ids: Vec<u64>,
}

impl SegmentIndex {
    /// Create a new empty segment index.
    pub fn new(segment_id: u64) -> Self {
        Self {
            segment_id,
            event_count: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            min_event_id: u64::MAX,
            max_event_id: 0,
            event_offsets: HashMap::new(),
            by_processor: HashMap::new(),
            by_flowfile: HashMap::new(),
            by_event_type: HashMap::new(),
            by_lineage: HashMap::new(),
            content_claim_ids: Vec::new(),
        }
    }

    /// Add an event to the index.
    pub fn add_event(&mut self, event: &ProvenanceEvent, file_offset: u64) {
        let eid = event.event_id;

        self.event_count += 1;
        self.min_timestamp = self.min_timestamp.min(event.timestamp_nanos);
        self.max_timestamp = self.max_timestamp.max(event.timestamp_nanos);
        self.min_event_id = self.min_event_id.min(eid);
        self.max_event_id = self.max_event_id.max(eid);

        self.event_offsets.insert(eid, file_offset);

        self.by_processor
            .entry(event.processor_name.clone())
            .or_default()
            .push(eid);

        self.by_flowfile
            .entry(event.flowfile_id)
            .or_default()
            .push(eid);

        self.by_event_type
            .entry(event.event_type)
            .or_default()
            .push(eid);

        self.by_lineage
            .entry(event.lineage_start_id)
            .or_default()
            .push(eid);

        if let Some(claim_id) = event.content_claim_id {
            self.content_claim_ids.push(claim_id);
        }
    }

    /// Check if an event might be in this segment based on time range.
    pub fn may_contain_time_range(&self, start: Option<u64>, end: Option<u64>) -> bool {
        if self.event_count == 0 {
            return false;
        }
        if let Some(start) = start
            && self.max_timestamp < start
        {
            return false;
        }
        if let Some(end) = end
            && self.min_timestamp > end
        {
            return false;
        }
        true
    }

    /// Check if an event might be in this segment based on event ID.
    pub fn may_contain_event_id(&self, event_id: u64) -> bool {
        if self.event_count == 0 {
            return false;
        }
        event_id >= self.min_event_id && event_id <= self.max_event_id
    }
}

// ── Index file serialization ──────────────────────────────────────────────

/// Write an index file for a finalized segment.
pub fn write_index(path: &Path, index: &SegmentIndex) -> Result<()> {
    let tmp_path = path.with_extension("idx.tmp");

    let mut buf = Vec::with_capacity(4096);

    // Header.
    buf.extend_from_slice(INDEX_MAGIC);
    buf.extend_from_slice(&INDEX_VERSION.to_le_bytes());
    buf.extend_from_slice(&index.segment_id.to_le_bytes());
    buf.extend_from_slice(&index.event_count.to_le_bytes());
    buf.extend_from_slice(&index.min_timestamp.to_le_bytes());
    buf.extend_from_slice(&index.max_timestamp.to_le_bytes());
    buf.extend_from_slice(&index.min_event_id.to_le_bytes());
    buf.extend_from_slice(&index.max_event_id.to_le_bytes());

    // Event offsets: [4B count] [repeated: 8B event_id, 8B offset].
    buf.extend_from_slice(&(index.event_offsets.len() as u32).to_le_bytes());
    for (&eid, &offset) in &index.event_offsets {
        buf.extend_from_slice(&eid.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    // By processor: [4B count] [repeated: 4B name_len, name, 4B id_count, repeated 8B ids].
    buf.extend_from_slice(&(index.by_processor.len() as u32).to_le_bytes());
    for (name, ids) in &index.by_processor {
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for &id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
    }

    // By FlowFile: [4B count] [repeated: 8B ffid, 4B id_count, repeated 8B ids].
    buf.extend_from_slice(&(index.by_flowfile.len() as u32).to_le_bytes());
    for (&ffid, ids) in &index.by_flowfile {
        buf.extend_from_slice(&ffid.to_le_bytes());
        buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for &id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
    }

    // By event type: [4B count] [repeated: 1B type_tag, 4B id_count, repeated 8B ids].
    buf.extend_from_slice(&(index.by_event_type.len() as u32).to_le_bytes());
    for (et, ids) in &index.by_event_type {
        buf.push(et.to_tag());
        buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for &id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
    }

    // By lineage: [4B count] [repeated: 8B lineage_id, 4B id_count, repeated 8B ids].
    buf.extend_from_slice(&(index.by_lineage.len() as u32).to_le_bytes());
    for (&lineage_id, ids) in &index.by_lineage {
        buf.extend_from_slice(&lineage_id.to_le_bytes());
        buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for &id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
    }

    // Content claim IDs: [4B count] [repeated: 8B claim_id].
    buf.extend_from_slice(&(index.content_claim_ids.len() as u32).to_le_bytes());
    for &cid in &index.content_claim_ids {
        buf.extend_from_slice(&cid.to_le_bytes());
    }

    // Footer CRC over all content.
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Write atomically via temp file + rename.
    fs::write(&tmp_path, &buf).map_err(|e| RuniFiError::ProvenanceError {
        path: tmp_path.display().to_string(),
        reason: format!("failed to write index: {e}"),
    })?;

    fs::rename(&tmp_path, path).map_err(|e| RuniFiError::ProvenanceError {
        path: path.display().to_string(),
        reason: format!("failed to rename index: {e}"),
    })?;

    Ok(())
}

/// Read an index file. Returns `None` if the file does not exist.
pub fn read_index(path: &Path) -> Result<Option<SegmentIndex>> {
    if !path.exists() {
        return Ok(None);
    }

    let data = fs::read(path).map_err(|e| RuniFiError::ProvenanceError {
        path: path.display().to_string(),
        reason: format!("failed to read index: {e}"),
    })?;

    // Minimum size: 8 magic + 4 version + 8 seg_id + 8 count + 8*4 ranges + 4 CRC = 64.
    if data.len() < 64 {
        return Err(RuniFiError::ProvenanceCorrupted {
            offset: 0,
            reason: "index file too small".into(),
        });
    }

    // Verify footer CRC.
    let content = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
    let computed_crc = crc32fast::hash(content);
    if stored_crc != computed_crc {
        return Err(RuniFiError::ProvenanceCorrupted {
            offset: 0,
            reason: format!(
                "index CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
            ),
        });
    }

    let mut pos;

    let read_u8 = |pos: &mut usize| -> Result<u8> {
        if *pos >= content.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of index".into(),
            });
        }
        let val = content[*pos];
        *pos += 1;
        Ok(val)
    };

    let read_u32 = |pos: &mut usize| -> Result<u32> {
        if *pos + 4 > content.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of index".into(),
            });
        }
        let val = u32::from_le_bytes(content[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Ok(val)
    };

    let read_u64_fn = |pos: &mut usize| -> Result<u64> {
        if *pos + 8 > content.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of index".into(),
            });
        }
        let val = u64::from_le_bytes(content[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Ok(val)
    };

    let read_string = |pos: &mut usize| -> Result<String> {
        let len = read_u32(pos)? as usize;
        if *pos + len > content.len() {
            return Err(RuniFiError::ProvenanceCorrupted {
                offset: *pos as u64,
                reason: "string truncated in index".into(),
            });
        }
        let s = String::from_utf8_lossy(&content[*pos..*pos + len]).into_owned();
        *pos += len;
        Ok(s)
    };

    // Validate magic.
    if &data[..8] != INDEX_MAGIC {
        return Err(RuniFiError::ProvenanceCorrupted {
            offset: 0,
            reason: "invalid index magic".into(),
        });
    }
    pos = 8;

    let version = read_u32(&mut pos)?;
    if version != INDEX_VERSION {
        return Err(RuniFiError::ProvenanceCorrupted {
            offset: 8,
            reason: format!("unsupported index version: {version}"),
        });
    }

    let segment_id = read_u64_fn(&mut pos)?;
    let event_count = read_u64_fn(&mut pos)?;
    let min_timestamp = read_u64_fn(&mut pos)?;
    let max_timestamp = read_u64_fn(&mut pos)?;
    let min_event_id = read_u64_fn(&mut pos)?;
    let max_event_id = read_u64_fn(&mut pos)?;

    // Event offsets.
    let offset_count = read_u32(&mut pos)? as usize;
    let mut event_offsets = HashMap::with_capacity(offset_count);
    for _ in 0..offset_count {
        let eid = read_u64_fn(&mut pos)?;
        let offset = read_u64_fn(&mut pos)?;
        event_offsets.insert(eid, offset);
    }

    // By processor.
    let proc_count = read_u32(&mut pos)? as usize;
    let mut by_processor = HashMap::with_capacity(proc_count);
    for _ in 0..proc_count {
        let name = read_string(&mut pos)?;
        let id_count = read_u32(&mut pos)? as usize;
        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            ids.push(read_u64_fn(&mut pos)?);
        }
        by_processor.insert(name, ids);
    }

    // By FlowFile.
    let ff_count = read_u32(&mut pos)? as usize;
    let mut by_flowfile = HashMap::with_capacity(ff_count);
    for _ in 0..ff_count {
        let ffid = read_u64_fn(&mut pos)?;
        let id_count = read_u32(&mut pos)? as usize;
        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            ids.push(read_u64_fn(&mut pos)?);
        }
        by_flowfile.insert(ffid, ids);
    }

    // By event type.
    let et_count = read_u32(&mut pos)? as usize;
    let mut by_event_type = HashMap::with_capacity(et_count);
    for _ in 0..et_count {
        let tag = read_u8(&mut pos)?;
        let et =
            ProvenanceEventType::from_tag(tag).ok_or_else(|| RuniFiError::ProvenanceCorrupted {
                offset: pos as u64,
                reason: format!("unknown event type tag in index: {tag}"),
            })?;
        let id_count = read_u32(&mut pos)? as usize;
        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            ids.push(read_u64_fn(&mut pos)?);
        }
        by_event_type.insert(et, ids);
    }

    // By lineage.
    let lin_count = read_u32(&mut pos)? as usize;
    let mut by_lineage = HashMap::with_capacity(lin_count);
    for _ in 0..lin_count {
        let lineage_id = read_u64_fn(&mut pos)?;
        let id_count = read_u32(&mut pos)? as usize;
        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            ids.push(read_u64_fn(&mut pos)?);
        }
        by_lineage.insert(lineage_id, ids);
    }

    // Content claim IDs.
    let claim_count = read_u32(&mut pos)? as usize;
    let mut content_claim_ids = Vec::with_capacity(claim_count);
    for _ in 0..claim_count {
        content_claim_ids.push(read_u64_fn(&mut pos)?);
    }

    Ok(Some(SegmentIndex {
        segment_id,
        event_count,
        min_timestamp,
        max_timestamp,
        min_event_id,
        max_event_id,
        event_offsets,
        by_processor,
        by_flowfile,
        by_event_type,
        by_lineage,
        content_claim_ids,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event(eid: u64, ffid: u64, processor: &str, lineage_id: u64) -> ProvenanceEvent {
        ProvenanceEvent {
            event_id: eid,
            flowfile_id: ffid,
            event_type: ProvenanceEventType::Create,
            processor_name: processor.to_string(),
            processor_type: "TestProcessor".to_string(),
            timestamp_nanos: 1_000_000_000 + eid * 1_000_000,
            attributes: Vec::new(),
            content_size: 1024,
            lineage_start_id: lineage_id,
            relationship: None,
            source_flowfile_id: None,
            details: String::new(),
            parent_flowfile_ids: Vec::new(),
            child_flowfile_ids: Vec::new(),
            transit_uri: None,
            content_claim_id: if eid % 2 == 0 { Some(eid * 10) } else { None },
            previous_attributes: Vec::new(),
        }
    }

    #[test]
    fn index_build_and_query() {
        let mut index = SegmentIndex::new(0);

        let e1 = make_test_event(1, 100, "gen", 100);
        let e2 = make_test_event(2, 100, "gen", 100);
        let e3 = make_test_event(3, 200, "log", 200);

        index.add_event(&e1, 8);
        index.add_event(&e2, 100);
        index.add_event(&e3, 200);

        assert_eq!(index.event_count, 3);
        assert_eq!(index.min_event_id, 1);
        assert_eq!(index.max_event_id, 3);

        // By processor.
        assert_eq!(index.by_processor["gen"].len(), 2);
        assert_eq!(index.by_processor["log"].len(), 1);

        // By FlowFile.
        assert_eq!(index.by_flowfile[&100].len(), 2);
        assert_eq!(index.by_flowfile[&200].len(), 1);

        // By event type.
        assert_eq!(index.by_event_type[&ProvenanceEventType::Create].len(), 3);

        // By lineage.
        assert_eq!(index.by_lineage[&100].len(), 2);
        assert_eq!(index.by_lineage[&200].len(), 1);

        // Content claim IDs (event 2 has claim_id=20).
        assert_eq!(index.content_claim_ids, vec![20]);

        // Event offsets.
        assert_eq!(index.event_offsets[&1], 8);
        assert_eq!(index.event_offsets[&2], 100);
        assert_eq!(index.event_offsets[&3], 200);
    }

    #[test]
    fn time_range_check() {
        let mut index = SegmentIndex::new(0);
        let e1 = make_test_event(1, 100, "gen", 100);
        let e2 = make_test_event(5, 100, "gen", 100);
        index.add_event(&e1, 0);
        index.add_event(&e2, 100);

        // Segment timestamps: min=1_001_000_000, max=1_005_000_000.
        assert!(index.may_contain_time_range(None, None));
        assert!(index.may_contain_time_range(Some(1_000_000_000), None));
        assert!(index.may_contain_time_range(None, Some(2_000_000_000)));
        assert!(index.may_contain_time_range(Some(1_001_000_000), Some(1_005_000_000)));

        // Completely before.
        assert!(!index.may_contain_time_range(Some(2_000_000_000), None));
        // Completely after.
        assert!(!index.may_contain_time_range(None, Some(999_999_999)));
    }

    #[test]
    fn event_id_check() {
        let mut index = SegmentIndex::new(0);
        let e1 = make_test_event(10, 100, "gen", 100);
        let e2 = make_test_event(20, 100, "gen", 100);
        index.add_event(&e1, 0);
        index.add_event(&e2, 100);

        assert!(index.may_contain_event_id(10));
        assert!(index.may_contain_event_id(15));
        assert!(index.may_contain_event_id(20));
        assert!(!index.may_contain_event_id(5));
        assert!(!index.may_contain_event_id(25));
    }

    #[test]
    fn empty_index_contains_nothing() {
        let index = SegmentIndex::new(0);
        assert!(!index.may_contain_time_range(Some(0), Some(u64::MAX)));
        assert!(!index.may_contain_event_id(1));
    }

    #[test]
    fn index_file_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.idx");

        let mut index = SegmentIndex::new(42);
        let e1 = make_test_event(1, 100, "gen", 100);
        let e2 = make_test_event(2, 200, "log", 200);
        let mut e3 = make_test_event(3, 100, "gen", 100);
        e3.event_type = ProvenanceEventType::Send;
        index.add_event(&e1, 8);
        index.add_event(&e2, 200);
        index.add_event(&e3, 400);

        write_index(&path, &index).unwrap();
        let loaded = read_index(&path).unwrap().unwrap();

        assert_eq!(loaded.segment_id, 42);
        assert_eq!(loaded.event_count, 3);
        assert_eq!(loaded.min_event_id, 1);
        assert_eq!(loaded.max_event_id, 3);
        assert_eq!(loaded.event_offsets.len(), 3);
        assert_eq!(loaded.event_offsets[&1], 8);
        assert_eq!(loaded.by_processor["gen"].len(), 2);
        assert_eq!(loaded.by_processor["log"].len(), 1);
        assert_eq!(loaded.by_flowfile[&100].len(), 2);
        assert_eq!(loaded.by_lineage[&100].len(), 2);
        assert_eq!(loaded.by_event_type[&ProvenanceEventType::Create].len(), 2);
        assert_eq!(loaded.by_event_type[&ProvenanceEventType::Send].len(), 1);
        // Even event IDs get content_claim_id: event 2 -> claim_id 20.
        assert_eq!(loaded.content_claim_ids, vec![20]);
    }

    #[test]
    fn index_file_missing_returns_none() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.idx");
        assert!(read_index(&path).unwrap().is_none());
    }

    #[test]
    fn index_file_corrupted_crc_detected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("corrupt.idx");

        let mut index = SegmentIndex::new(0);
        let e1 = make_test_event(1, 100, "gen", 100);
        index.add_event(&e1, 0);
        write_index(&path, &index).unwrap();

        // Corrupt a byte in the middle.
        let mut data = fs::read(&path).unwrap();
        if data.len() > 20 {
            data[20] ^= 0xFF;
        }
        fs::write(&path, &data).unwrap();

        let result = read_index(&path);
        assert!(result.is_err());
    }
}
