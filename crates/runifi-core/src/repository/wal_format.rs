//! Binary encode/decode for WAL records and checkpoint files.
//!
//! ## WAL File (`wal.dat`)
//! ```text
//! HEADER: [8 bytes: "RNFWAL01"]
//! RECORDS (repeated):
//!   [1B tag] [4B payload_len (u32 LE)] [N bytes payload] [4B CRC32]
//! ```
//!
//! ## Checkpoint File (`checkpoint.dat`)
//! ```text
//! HEADER: [8B "RNFCHK01"] [4B version=1] [8B max_id] [8B timestamp]
//! BODY: repeated UPSERT records (same encoding as WAL)
//! FOOTER: [4B CRC32 over entire file]
//! ```

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use runifi_plugin_api::{ContentClaim, FlowFile};

use crate::error::{Result, RuniFiError};

// ── Constants ──────────────────────────────────────────────────────────────

const WAL_MAGIC: &[u8; 8] = b"RNFWAL01";
const CHECKPOINT_MAGIC: &[u8; 8] = b"RNFCHK01";
const CHECKPOINT_VERSION: u32 = 1;

pub const TAG_UPSERT: u8 = 0x01;
pub const TAG_DELETE: u8 = 0x02;
pub const TAG_BATCH_END: u8 = 0x03;

// ── Intermediate types ─────────────────────────────────────────────────────

/// Deserialized FlowFile with owned strings (before Arc conversion).
#[derive(Debug, Clone)]
pub struct SerializedFlowFile {
    pub id: u64,
    pub size: u64,
    pub created_at_nanos: u64,
    pub lineage_start_id: u64,
    pub penalized_until_nanos: u64,
    pub content_claim: Option<ContentClaim>,
    pub attributes: Vec<(String, String)>,
    pub queue_id: String,
}

/// Batch-end marker payload.
#[derive(Debug, Clone)]
pub struct BatchEnd {
    pub timestamp_nanos: u64,
    pub op_count: u32,
}

/// A decoded WAL record.
#[derive(Debug)]
pub enum WalRecord {
    Upsert(SerializedFlowFile),
    Delete(u64),
    BatchEnd(BatchEnd),
}

// ── UPSERT encode/decode ───────────────────────────────────────────────────

/// Encode a FlowFile + queue_id into UPSERT payload bytes.
pub fn encode_upsert(ff: &FlowFile, queue_id: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(&ff.id.to_le_bytes());
    buf.extend_from_slice(&ff.size.to_le_bytes());
    buf.extend_from_slice(&ff.created_at_nanos.to_le_bytes());
    buf.extend_from_slice(&ff.lineage_start_id.to_le_bytes());
    buf.extend_from_slice(&ff.penalized_until_nanos.to_le_bytes());

    match &ff.content_claim {
        Some(claim) => {
            buf.push(1);
            buf.extend_from_slice(&claim.resource_id.to_le_bytes());
            buf.extend_from_slice(&claim.offset.to_le_bytes());
            buf.extend_from_slice(&claim.length.to_le_bytes());
        }
        None => buf.push(0),
    }

    let attr_count = ff.attributes.len() as u32;
    buf.extend_from_slice(&attr_count.to_le_bytes());
    for (k, v) in &ff.attributes {
        let key_bytes = k.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(key_bytes);
        let val_bytes = v.as_bytes();
        buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(val_bytes);
    }

    let queue_bytes = queue_id.as_bytes();
    buf.extend_from_slice(&(queue_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(queue_bytes);

    buf
}

/// Decode UPSERT payload bytes into a `SerializedFlowFile`.
pub fn decode_upsert(data: &[u8]) -> Result<SerializedFlowFile> {
    let mut pos = 0;

    let read_u64 = |pos: &mut usize, data: &[u8]| -> Result<u64> {
        if *pos + 8 > data.len() {
            return Err(RuniFiError::WalCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of upsert payload".into(),
            });
        }
        let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
        *pos += 8;
        Ok(val)
    };

    let read_u32 = |pos: &mut usize, data: &[u8]| -> Result<u32> {
        if *pos + 4 > data.len() {
            return Err(RuniFiError::WalCorrupted {
                offset: *pos as u64,
                reason: "unexpected end of upsert payload".into(),
            });
        }
        let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
        *pos += 4;
        Ok(val)
    };

    let id = read_u64(&mut pos, data)?;
    let size = read_u64(&mut pos, data)?;
    let created_at_nanos = read_u64(&mut pos, data)?;
    let lineage_start_id = read_u64(&mut pos, data)?;
    let penalized_until_nanos = read_u64(&mut pos, data)?;

    if pos >= data.len() {
        return Err(RuniFiError::WalCorrupted {
            offset: pos as u64,
            reason: "missing has_claim byte".into(),
        });
    }
    let has_claim = data[pos];
    pos += 1;

    let content_claim = if has_claim == 1 {
        let resource_id = read_u64(&mut pos, data)?;
        let offset = read_u64(&mut pos, data)?;
        let length = read_u64(&mut pos, data)?;
        Some(ContentClaim {
            resource_id,
            offset,
            length,
        })
    } else {
        None
    };

    let attr_count = read_u32(&mut pos, data)? as usize;
    let mut attributes = Vec::with_capacity(attr_count);
    for _ in 0..attr_count {
        let key_len = read_u32(&mut pos, data)? as usize;
        if pos + key_len > data.len() {
            return Err(RuniFiError::WalCorrupted {
                offset: pos as u64,
                reason: "attribute key truncated".into(),
            });
        }
        let key = String::from_utf8_lossy(&data[pos..pos + key_len]).into_owned();
        pos += key_len;

        let val_len = read_u32(&mut pos, data)? as usize;
        if pos + val_len > data.len() {
            return Err(RuniFiError::WalCorrupted {
                offset: pos as u64,
                reason: "attribute value truncated".into(),
            });
        }
        let val = String::from_utf8_lossy(&data[pos..pos + val_len]).into_owned();
        pos += val_len;

        attributes.push((key, val));
    }

    let queue_id_len = read_u32(&mut pos, data)? as usize;
    if pos + queue_id_len > data.len() {
        return Err(RuniFiError::WalCorrupted {
            offset: pos as u64,
            reason: "queue_id truncated".into(),
        });
    }
    let queue_id = String::from_utf8_lossy(&data[pos..pos + queue_id_len]).into_owned();

    Ok(SerializedFlowFile {
        id,
        size,
        created_at_nanos,
        lineage_start_id,
        penalized_until_nanos,
        content_claim,
        attributes,
        queue_id,
    })
}

// ── DELETE encode/decode ───────────────────────────────────────────────────

pub fn encode_delete(id: u64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

pub fn decode_delete(data: &[u8]) -> Result<u64> {
    if data.len() < 8 {
        return Err(RuniFiError::WalCorrupted {
            offset: 0,
            reason: "delete payload too short".into(),
        });
    }
    Ok(u64::from_le_bytes(data[..8].try_into().unwrap()))
}

// ── BATCH_END encode/decode ────────────────────────────────────────────────

pub fn encode_batch_end(timestamp_nanos: u64, op_count: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12);
    buf.extend_from_slice(&timestamp_nanos.to_le_bytes());
    buf.extend_from_slice(&op_count.to_le_bytes());
    buf
}

pub fn decode_batch_end(data: &[u8]) -> Result<BatchEnd> {
    if data.len() < 12 {
        return Err(RuniFiError::WalCorrupted {
            offset: 0,
            reason: "batch_end payload too short".into(),
        });
    }
    let timestamp_nanos = u64::from_le_bytes(data[..8].try_into().unwrap());
    let op_count = u32::from_le_bytes(data[8..12].try_into().unwrap());
    Ok(BatchEnd {
        timestamp_nanos,
        op_count,
    })
}

// ── Record-level I/O ───────────────────────────────────────────────────────

/// Write a single record: [1B tag] [4B payload_len] [payload] [4B CRC32].
pub fn write_record<W: Write>(writer: &mut W, tag: u8, payload: &[u8]) -> io::Result<()> {
    writer.write_all(&[tag])?;
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(payload)?;
    let crc = crc32fast::hash(payload);
    writer.write_all(&crc.to_le_bytes())?;
    Ok(())
}

/// Read a single record. Returns `None` on clean EOF or truncated tail.
/// Returns `Err` on CRC mismatch.
pub fn read_record<R: Read>(reader: &mut R) -> Result<Option<(u8, Vec<u8>)>> {
    // Read tag byte.
    let mut tag_buf = [0u8; 1];
    match reader.read_exact(&mut tag_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RuniFiError::Io(e)),
    }
    let tag = tag_buf[0];

    // Read payload length.
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RuniFiError::Io(e)),
    }
    let payload_len = u32::from_le_bytes(len_buf) as usize;

    // Read payload.
    let mut payload = vec![0u8; payload_len];
    match reader.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RuniFiError::Io(e)),
    }

    // Read CRC.
    let mut crc_buf = [0u8; 4];
    match reader.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RuniFiError::Io(e)),
    }

    let stored_crc = u32::from_le_bytes(crc_buf);
    let computed_crc = crc32fast::hash(&payload);
    if stored_crc != computed_crc {
        return Err(RuniFiError::WalCorrupted {
            offset: 0,
            reason: format!(
                "CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
            ),
        });
    }

    Ok(Some((tag, payload)))
}

// ── WAL file I/O ───────────────────────────────────────────────────────────

/// Write the WAL file header.
pub fn write_wal_header<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(WAL_MAGIC)
}

/// Validate and skip the WAL file header. Returns `false` if header is invalid.
pub fn read_wal_header<R: Read>(reader: &mut R) -> Result<bool> {
    let mut buf = [0u8; 8];
    match reader.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(RuniFiError::Io(e)),
    }
    Ok(buf == *WAL_MAGIC)
}

/// Read all records from a WAL file, stopping at EOF or first truncated record.
/// CRC errors are propagated.
pub fn read_wal_records(path: &Path) -> Result<Vec<WalRecord>> {
    let file = fs::File::open(path).map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("failed to open: {e}"),
    })?;
    let mut reader = BufReader::new(file);

    if !read_wal_header(&mut reader)? {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    while let Some((tag, payload)) = read_record(&mut reader)? {
        let record = match tag {
            TAG_UPSERT => WalRecord::Upsert(decode_upsert(&payload)?),
            TAG_DELETE => WalRecord::Delete(decode_delete(&payload)?),
            TAG_BATCH_END => WalRecord::BatchEnd(decode_batch_end(&payload)?),
            _ => {
                return Err(RuniFiError::WalCorrupted {
                    offset: 0,
                    reason: format!("unknown record tag: {tag:#04x}"),
                });
            }
        };
        records.push(record);
    }

    Ok(records)
}

/// In-memory checkpoint state: flowfile_id → (FlowFile, queue_id).
pub type CheckpointState = HashMap<u64, (FlowFile, String)>;

// ── Checkpoint I/O ─────────────────────────────────────────────────────────

/// Write a checkpoint file from in-memory state.
///
/// Format: header → repeated UPSERT records → footer CRC32.
/// Written to a temp file, then atomically renamed.
pub fn write_checkpoint(path: &Path, state: &CheckpointState, max_id: u64) -> Result<()> {
    let tmp_path = path.with_extension("dat.tmp");

    let file = fs::File::create(&tmp_path).map_err(|e| RuniFiError::CheckpointError {
        path: path.display().to_string(),
        reason: format!("failed to create temp: {e}"),
    })?;
    let mut writer = BufWriter::new(file);

    // Collect all bytes for CRC computation.
    let mut all_bytes = Vec::new();

    // Header.
    all_bytes.extend_from_slice(CHECKPOINT_MAGIC);
    all_bytes.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
    all_bytes.extend_from_slice(&max_id.to_le_bytes());

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    all_bytes.extend_from_slice(&timestamp.to_le_bytes());

    // Body: UPSERT records.
    for (ff, queue_id) in state.values() {
        let payload = encode_upsert(ff, queue_id);
        // Record format: [1B tag] [4B len] [payload] [4B CRC]
        all_bytes.push(TAG_UPSERT);
        all_bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        all_bytes.extend_from_slice(&payload);
        let crc = crc32fast::hash(&payload);
        all_bytes.extend_from_slice(&crc.to_le_bytes());
    }

    // Write everything except footer CRC.
    writer
        .write_all(&all_bytes)
        .map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("write failed: {e}"),
        })?;

    // Footer CRC over the entire content written so far.
    let footer_crc = crc32fast::hash(&all_bytes);
    writer
        .write_all(&footer_crc.to_le_bytes())
        .map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("footer write failed: {e}"),
        })?;

    writer.flush().map_err(|e| RuniFiError::CheckpointError {
        path: path.display().to_string(),
        reason: format!("flush failed: {e}"),
    })?;

    // Fsync before rename for durability.
    writer
        .into_inner()
        .map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("into_inner failed: {e}"),
        })?
        .sync_all()
        .map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("fsync failed: {e}"),
        })?;

    // Atomic rename.
    fs::rename(&tmp_path, path).map_err(|e| RuniFiError::CheckpointError {
        path: path.display().to_string(),
        reason: format!("rename failed: {e}"),
    })?;

    Ok(())
}

/// Read a checkpoint file, returning the in-memory state and max_id.
pub fn read_checkpoint(path: &Path) -> Result<Option<(CheckpointState, u64)>> {
    if !path.exists() {
        return Ok(None);
    }

    let data = fs::read(path).map_err(|e| RuniFiError::CheckpointError {
        path: path.display().to_string(),
        reason: format!("failed to read: {e}"),
    })?;

    // Minimum: 8 magic + 4 version + 8 max_id + 8 timestamp + 4 footer CRC = 32
    if data.len() < 32 {
        return Err(RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: "file too small".into(),
        });
    }

    // Verify footer CRC.
    let content = &data[..data.len() - 4];
    let footer_crc_bytes: [u8; 4] = data[data.len() - 4..].try_into().unwrap();
    let stored_crc = u32::from_le_bytes(footer_crc_bytes);
    let computed_crc = crc32fast::hash(content);
    if stored_crc != computed_crc {
        return Err(RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!(
                "CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
            ),
        });
    }

    // Parse header.
    if &data[..8] != CHECKPOINT_MAGIC {
        return Err(RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: "invalid magic".into(),
        });
    }

    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if version != CHECKPOINT_VERSION {
        return Err(RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("unsupported version: {version}"),
        });
    }

    let max_id = u64::from_le_bytes(data[12..20].try_into().unwrap());
    // timestamp at 20..28, currently unused on read

    // Parse body records (starting at offset 28, up to content end).
    let body = &content[28..];
    let mut reader = io::Cursor::new(body);
    let mut state = HashMap::new();

    while let Some((tag, payload)) = read_record(&mut reader)? {
        if tag != TAG_UPSERT {
            return Err(RuniFiError::CheckpointError {
                path: path.display().to_string(),
                reason: format!("unexpected tag in checkpoint: {tag:#04x}"),
            });
        }
        let sff = decode_upsert(&payload)?;
        let ff = to_flowfile(&sff);
        state.insert(ff.id, (ff, sff.queue_id));
    }

    Ok(Some((state, max_id)))
}

// ── Conversion ─────────────────────────────────────────────────────────────

/// Reconstruct a `FlowFile` from a `SerializedFlowFile` with `Arc<str>` attributes.
pub fn to_flowfile(sff: &SerializedFlowFile) -> FlowFile {
    FlowFile {
        id: sff.id,
        attributes: sff
            .attributes
            .iter()
            .map(|(k, v)| (Arc::from(k.as_str()), Arc::from(v.as_str())))
            .collect(),
        content_claim: sff.content_claim.clone(),
        size: sff.size,
        created_at_nanos: sff.created_at_nanos,
        lineage_start_id: sff.lineage_start_id,
        penalized_until_nanos: sff.penalized_until_nanos,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_flowfile(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: vec![
                (Arc::from("filename"), Arc::from("test.txt")),
                (Arc::from("mime.type"), Arc::from("text/plain")),
            ],
            content_claim: Some(ContentClaim {
                resource_id: 42,
                offset: 100,
                length: 5000,
            }),
            size: 5000,
            created_at_nanos: 1234567890,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    fn test_flowfile_no_claim(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 999,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    #[test]
    fn upsert_roundtrip_with_claim() {
        let ff = test_flowfile(1);
        let payload = encode_upsert(&ff, "conn-0");
        let sff = decode_upsert(&payload).unwrap();

        assert_eq!(sff.id, 1);
        assert_eq!(sff.size, 5000);
        assert_eq!(sff.created_at_nanos, 1234567890);
        assert_eq!(sff.lineage_start_id, 1);
        assert_eq!(sff.penalized_until_nanos, 0);
        assert_eq!(sff.content_claim.as_ref().unwrap().resource_id, 42);
        assert_eq!(sff.content_claim.as_ref().unwrap().offset, 100);
        assert_eq!(sff.content_claim.as_ref().unwrap().length, 5000);
        assert_eq!(sff.attributes.len(), 2);
        assert_eq!(sff.attributes[0], ("filename".into(), "test.txt".into()));
        assert_eq!(sff.queue_id, "conn-0");

        let reconstructed = to_flowfile(&sff);
        assert_eq!(reconstructed.id, ff.id);
        assert_eq!(reconstructed.size, ff.size);
        assert_eq!(reconstructed.content_claim, ff.content_claim);
    }

    #[test]
    fn upsert_roundtrip_without_claim() {
        let ff = test_flowfile_no_claim(7);
        let payload = encode_upsert(&ff, "q");
        let sff = decode_upsert(&payload).unwrap();

        assert_eq!(sff.id, 7);
        assert!(sff.content_claim.is_none());
        assert!(sff.attributes.is_empty());
        assert_eq!(sff.queue_id, "q");
    }

    #[test]
    fn upsert_roundtrip_unicode_attributes() {
        let ff = FlowFile {
            id: 10,
            attributes: vec![
                (Arc::from("名前"), Arc::from("テスト")),
                (Arc::from("emoji"), Arc::from("🚀💾")),
            ],
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: 10,
            penalized_until_nanos: 0,
        };
        let payload = encode_upsert(&ff, "conn-unicode");
        let sff = decode_upsert(&payload).unwrap();

        assert_eq!(sff.attributes[0], ("名前".into(), "テスト".into()));
        assert_eq!(sff.attributes[1], ("emoji".into(), "🚀💾".into()));
    }

    #[test]
    fn delete_roundtrip() {
        let payload = encode_delete(42);
        let id = decode_delete(&payload).unwrap();
        assert_eq!(id, 42);
    }

    #[test]
    fn batch_end_roundtrip() {
        let payload = encode_batch_end(999_000_000, 5);
        let be = decode_batch_end(&payload).unwrap();
        assert_eq!(be.timestamp_nanos, 999_000_000);
        assert_eq!(be.op_count, 5);
    }

    #[test]
    fn record_roundtrip() {
        let mut buf = Vec::new();
        let payload = b"hello world";
        write_record(&mut buf, TAG_UPSERT, payload).unwrap();

        let mut reader = io::Cursor::new(&buf);
        let (tag, data) = read_record(&mut reader).unwrap().unwrap();
        assert_eq!(tag, TAG_UPSERT);
        assert_eq!(data, payload);
    }

    #[test]
    fn crc_corruption_detected() {
        let mut buf = Vec::new();
        write_record(&mut buf, TAG_DELETE, b"test").unwrap();

        // Corrupt the payload.
        buf[5] ^= 0xFF;

        let mut reader = io::Cursor::new(&buf);
        let result = read_record(&mut reader);
        assert!(result.is_err());
        match result.unwrap_err() {
            RuniFiError::WalCorrupted { reason, .. } => {
                assert!(reason.contains("CRC mismatch"));
            }
            other => panic!("expected WalCorrupted, got: {other}"),
        }
    }

    #[test]
    fn truncated_record_returns_none() {
        // Write a valid record then truncate it.
        let mut buf = Vec::new();
        write_record(&mut buf, TAG_UPSERT, b"full record").unwrap();

        // Truncate to just the tag + partial length.
        let truncated = &buf[..3];
        let mut reader = io::Cursor::new(truncated);
        let result = read_record(&mut reader).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn empty_read_returns_none() {
        let buf: Vec<u8> = Vec::new();
        let mut reader = io::Cursor::new(&buf);
        let result = read_record(&mut reader).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn checkpoint_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("checkpoint.dat");

        let mut state = HashMap::new();
        let ff1 = test_flowfile(1);
        let ff2 = test_flowfile_no_claim(2);
        state.insert(1, (ff1, "conn-0".to_string()));
        state.insert(2, (ff2, "conn-1".to_string()));

        write_checkpoint(&path, &state, 100).unwrap();

        let (recovered, max_id) = read_checkpoint(&path).unwrap().unwrap();
        assert_eq!(max_id, 100);
        assert_eq!(recovered.len(), 2);
        assert!(recovered.contains_key(&1));
        assert!(recovered.contains_key(&2));

        let (ff, q) = &recovered[&1];
        assert_eq!(ff.id, 1);
        assert_eq!(q, "conn-0");
        assert_eq!(ff.content_claim.as_ref().unwrap().resource_id, 42);

        let (ff2r, q2) = &recovered[&2];
        assert_eq!(ff2r.id, 2);
        assert_eq!(q2, "conn-1");
        assert!(ff2r.content_claim.is_none());
    }

    #[test]
    fn checkpoint_missing_file_returns_none() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.dat");
        assert!(read_checkpoint(&path).unwrap().is_none());
    }

    #[test]
    fn checkpoint_empty_state() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("checkpoint.dat");

        write_checkpoint(&path, &HashMap::new(), 0).unwrap();
        let (recovered, max_id) = read_checkpoint(&path).unwrap().unwrap();
        assert_eq!(max_id, 0);
        assert!(recovered.is_empty());
    }
}
