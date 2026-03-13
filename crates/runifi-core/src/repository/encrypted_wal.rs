//! Encrypted wrapper for the WAL-based FlowFile repository.
//!
//! Transparently encrypts WAL entries and checkpoint data using AES-256-GCM.
//! Each WAL record payload is encrypted with a per-record random nonce, and
//! the key ID is embedded so key rotation is supported: old WAL entries and
//! checkpoints encrypted with a previous key remain decryptable.
//!
//! ## Approach
//!
//! This module wraps `WalFlowFileRepository` by encrypting the serialized
//! FlowFile data at the WAL format level. Rather than modifying the WAL
//! binary format, encryption is applied to the payload bytes of each record.
//! The WAL record structure (tag + length + payload + CRC) is preserved,
//! but the payload is an encrypted envelope containing:
//!
//! ```text
//! [key_id_len: 1B][key_id: N bytes][nonce: 12B][ciphertext + GCM tag: M bytes]
//! ```
//!
//! This approach keeps the WAL format backward-compatible at the record level
//! and allows mixing encrypted and unencrypted records during migration.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use parking_lot::Mutex;
use runifi_plugin_api::FlowFile;

use super::flowfile_repo::{FlowFileOp, FlowFileRepository, RecoveryState};
use super::key_provider::KeyProvider;
use super::wal_format::{
    self, CheckpointState, TAG_BATCH_END, TAG_DELETE, TAG_UPSERT, WalRecord, encode_batch_end,
    encode_delete, encode_upsert, to_flowfile, write_record, write_wal_header,
};
use crate::error::{Result, RuniFiError};
use crate::repository::flowfile_wal::FsyncMode;

/// Nonce size for AES-256-GCM (96 bits).
const NONCE_SIZE: usize = 12;

/// Tag indicating an encrypted payload (prepended to distinguish from unencrypted).
const ENCRYPTED_MARKER: u8 = 0xE0;

/// Configuration for the encrypted WAL FlowFile repository.
#[derive(Debug, Clone)]
pub struct EncryptedWalConfig {
    /// Directory for WAL and checkpoint files.
    pub dir: PathBuf,
    /// fsync behaviour.
    pub fsync_mode: FsyncMode,
    /// Checkpoint interval in seconds.
    pub checkpoint_interval_secs: u64,
}

impl Default for EncryptedWalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data/flowfile-repo"),
            fsync_mode: FsyncMode::Always,
            checkpoint_interval_secs: 120,
        }
    }
}

/// Internal mutable state protected by a mutex.
struct WalState {
    /// In-memory mirror: flowfile_id -> (FlowFile, queue_id).
    entries: CheckpointState,
    /// Active WAL writer.
    writer: BufWriter<File>,
    /// Highest FlowFile ID seen.
    max_id: u64,
}

/// Encrypted WAL-backed FlowFile repository.
///
/// Encrypts all WAL record payloads and checkpoint data using AES-256-GCM.
/// Supports key rotation: the key ID is embedded in each encrypted envelope,
/// allowing records encrypted with different keys to coexist.
pub struct EncryptedWalFlowFileRepository {
    config: EncryptedWalConfig,
    state: Mutex<WalState>,
    key_provider: Arc<dyn KeyProvider>,
}

impl EncryptedWalFlowFileRepository {
    /// Create a new encrypted WAL repository.
    pub fn new(config: EncryptedWalConfig, key_provider: Arc<dyn KeyProvider>) -> Result<Self> {
        fs::create_dir_all(&config.dir).map_err(|e| RuniFiError::WalError {
            path: config.dir.display().to_string(),
            reason: format!("failed to create dir: {e}"),
        })?;

        let wal_path = config.dir.join("wal.dat");
        let file = open_or_create_wal(&wal_path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            config,
            state: Mutex::new(WalState {
                entries: HashMap::new(),
                writer,
                max_id: 0,
            }),
            key_provider,
        })
    }

    fn wal_path(&self) -> PathBuf {
        self.config.dir.join("wal.dat")
    }

    fn checkpoint_path(&self) -> PathBuf {
        self.config.dir.join("checkpoint.dat")
    }

    /// Get the configured checkpoint interval.
    pub fn checkpoint_interval_secs(&self) -> u64 {
        self.config.checkpoint_interval_secs
    }

    /// Encrypt a payload using the active key.
    fn encrypt_payload(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let (key_id, key_bytes) = self
            .key_provider
            .active_key()
            .map_err(|e| RuniFiError::Config(format!("WAL encryption key error: {}", e)))?;

        let cipher = Aes256Gcm::new_from_slice(&key_bytes)
            .map_err(|e| RuniFiError::Config(format!("Invalid WAL encryption key: {}", e)))?;

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| RuniFiError::Config(format!("WAL encryption failed: {}", e)))?;

        let key_id_bytes = key_id.as_bytes();
        let key_id_len = key_id_bytes.len();
        if key_id_len > 255 {
            return Err(RuniFiError::Config(
                "Key ID too long (max 255 bytes)".to_string(),
            ));
        }

        // Envelope: [marker][key_id_len][key_id][nonce][ciphertext+tag]
        let total_len = 1 + 1 + key_id_len + NONCE_SIZE + ciphertext.len();
        let mut envelope = Vec::with_capacity(total_len);
        envelope.push(ENCRYPTED_MARKER);
        envelope.push(key_id_len as u8);
        envelope.extend_from_slice(key_id_bytes);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);

        Ok(envelope)
    }

    /// Decrypt an encrypted payload envelope.
    fn decrypt_payload(&self, envelope: &[u8]) -> Result<Vec<u8>> {
        // Check for encryption marker.
        if envelope.is_empty() || envelope[0] != ENCRYPTED_MARKER {
            // Not encrypted — return as-is (supports migration from unencrypted WAL).
            return Ok(envelope.to_vec());
        }

        // Minimum: 1 (marker) + 1 (key_id_len) + 0 (key_id) + 12 (nonce) + 16 (GCM tag)
        if envelope.len() < 1 + 1 + NONCE_SIZE + 16 {
            return Err(RuniFiError::WalCorrupted {
                offset: 0,
                reason: "Encrypted WAL payload too short".to_string(),
            });
        }

        let key_id_len = envelope[1] as usize;
        let header_len = 1 + 1 + key_id_len + NONCE_SIZE;
        if envelope.len() < header_len + 16 {
            return Err(RuniFiError::WalCorrupted {
                offset: 0,
                reason: "Encrypted WAL payload header is malformed".to_string(),
            });
        }

        let key_id = std::str::from_utf8(&envelope[2..2 + key_id_len])
            .map_err(|_| RuniFiError::WalCorrupted {
                offset: 0,
                reason: "Invalid key ID encoding in WAL payload".to_string(),
            })?
            .to_string();

        let nonce_start = 2 + key_id_len;
        let nonce = aes_gcm::Nonce::from_slice(&envelope[nonce_start..nonce_start + NONCE_SIZE]);

        let ciphertext = &envelope[header_len..];

        let key_bytes =
            self.key_provider
                .get_key(&key_id)
                .map_err(|e| RuniFiError::WalCorrupted {
                    offset: 0,
                    reason: format!("WAL decryption key error: {}", e),
                })?;

        let cipher =
            Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| RuniFiError::WalCorrupted {
                offset: 0,
                reason: format!("Invalid WAL decryption key: {}", e),
            })?;

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| RuniFiError::WalCorrupted {
                offset: 0,
                reason: "WAL decryption failed: authentication error".to_string(),
            })
    }

    /// Truncate the WAL file and re-open it with a fresh header.
    fn truncate_wal(&self) -> Result<()> {
        let wal_path = self.wal_path();
        let file = create_fresh_wal(&wal_path)?;
        let writer = BufWriter::new(file);
        self.state.lock().writer = writer;
        Ok(())
    }

    /// Write an encrypted checkpoint file.
    fn write_encrypted_checkpoint(
        &self,
        path: &Path,
        state: &CheckpointState,
        max_id: u64,
    ) -> Result<()> {
        // Serialize the checkpoint as normal WAL format into a buffer.
        let mut plaintext = Vec::new();

        for (ff, queue_id) in state.values() {
            let payload = encode_upsert(ff, queue_id);
            write_record(&mut plaintext, TAG_UPSERT, &payload).map_err(|e| {
                RuniFiError::CheckpointError {
                    path: path.display().to_string(),
                    reason: format!("serialize upsert failed: {e}"),
                }
            })?;
        }

        // Encrypt the serialized checkpoint body.
        let encrypted_body = self.encrypt_payload(&plaintext)?;

        // Write the checkpoint file with encrypted body.
        let tmp_path = path.with_extension("dat.tmp");
        let file = fs::File::create(&tmp_path).map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("failed to create temp: {e}"),
        })?;
        let mut writer = BufWriter::new(file);

        // Header: magic + version + max_id + timestamp
        let mut all_bytes = Vec::new();
        all_bytes.extend_from_slice(b"RNFCHK01");
        all_bytes.extend_from_slice(&1u32.to_le_bytes()); // version
        all_bytes.extend_from_slice(&max_id.to_le_bytes());

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        all_bytes.extend_from_slice(&timestamp.to_le_bytes());

        // Write encrypted body as a single record.
        write_record(
            &mut all_bytes,
            TAG_UPSERT, // Re-use UPSERT tag for the encrypted blob
            &encrypted_body,
        )
        .map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("write encrypted body failed: {e}"),
        })?;

        writer
            .write_all(&all_bytes)
            .map_err(|e| RuniFiError::CheckpointError {
                path: path.display().to_string(),
                reason: format!("write failed: {e}"),
            })?;

        // Footer CRC over the entire content.
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

        fs::rename(&tmp_path, path).map_err(|e| RuniFiError::CheckpointError {
            path: path.display().to_string(),
            reason: format!("rename failed: {e}"),
        })?;

        Ok(())
    }

    /// Read an encrypted checkpoint file.
    fn read_encrypted_checkpoint(&self, path: &Path) -> Result<Option<(CheckpointState, u64)>> {
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
        if &data[..8] != b"RNFCHK01" {
            return Err(RuniFiError::CheckpointError {
                path: path.display().to_string(),
                reason: "invalid magic".into(),
            });
        }

        let max_id = u64::from_le_bytes(data[12..20].try_into().unwrap());

        // The body starts at offset 28. It contains a single encrypted record.
        let body = &content[28..];
        let mut reader = std::io::Cursor::new(body);

        // Try to read one record — the encrypted checkpoint body.
        if let Some((tag, encrypted_body)) = wal_format::read_record(&mut reader)? {
            if tag != TAG_UPSERT {
                return Err(RuniFiError::CheckpointError {
                    path: path.display().to_string(),
                    reason: format!("unexpected tag in encrypted checkpoint: {tag:#04x}"),
                });
            }

            // Decrypt the body.
            let decrypted = self.decrypt_payload(&encrypted_body)?;

            // Parse the decrypted body as a sequence of UPSERT records.
            let mut inner_reader = std::io::Cursor::new(decrypted.as_slice());
            let mut state = HashMap::new();

            while let Some((inner_tag, payload)) = wal_format::read_record(&mut inner_reader)? {
                if inner_tag != TAG_UPSERT {
                    return Err(RuniFiError::CheckpointError {
                        path: path.display().to_string(),
                        reason: format!("unexpected tag in decrypted checkpoint: {inner_tag:#04x}"),
                    });
                }
                let sff = wal_format::decode_upsert(&payload)?;
                let ff = to_flowfile(&sff);
                state.insert(ff.id, (ff, sff.queue_id));
            }

            Ok(Some((state, max_id)))
        } else {
            // Empty checkpoint (no records).
            Ok(Some((HashMap::new(), max_id)))
        }
    }
}

impl FlowFileRepository for EncryptedWalFlowFileRepository {
    fn commit_batch(&self, ops: &[FlowFileOp<'_>]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock();
        let mut op_count: u32 = 0;

        for op in ops {
            match op {
                FlowFileOp::Upsert { flowfile, queue_id } => {
                    let plaintext = encode_upsert(flowfile, queue_id);
                    let encrypted = self.encrypt_payload(&plaintext)?;
                    write_record(&mut state.writer, TAG_UPSERT, &encrypted).map_err(|e| {
                        RuniFiError::WalError {
                            path: self.wal_path().display().to_string(),
                            reason: format!("write encrypted upsert failed: {e}"),
                        }
                    })?;
                    if flowfile.id > state.max_id {
                        state.max_id = flowfile.id;
                    }
                    state
                        .entries
                        .insert(flowfile.id, ((*flowfile).clone(), queue_id.to_string()));
                }
                FlowFileOp::Delete { id } => {
                    let plaintext = encode_delete(*id);
                    let encrypted = self.encrypt_payload(&plaintext)?;
                    write_record(&mut state.writer, TAG_DELETE, &encrypted).map_err(|e| {
                        RuniFiError::WalError {
                            path: self.wal_path().display().to_string(),
                            reason: format!("write encrypted delete failed: {e}"),
                        }
                    })?;
                    state.entries.remove(id);
                }
            }
            op_count += 1;
        }

        // Write BATCH_END marker (also encrypted).
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let batch_plaintext = encode_batch_end(timestamp, op_count);
        let batch_encrypted = self.encrypt_payload(&batch_plaintext)?;
        write_record(&mut state.writer, TAG_BATCH_END, &batch_encrypted).map_err(|e| {
            RuniFiError::WalError {
                path: self.wal_path().display().to_string(),
                reason: format!("write encrypted batch_end failed: {e}"),
            }
        })?;

        state.writer.flush().map_err(|e| RuniFiError::WalError {
            path: self.wal_path().display().to_string(),
            reason: format!("flush failed: {e}"),
        })?;

        if self.config.fsync_mode == FsyncMode::Always {
            state
                .writer
                .get_ref()
                .sync_all()
                .map_err(|e| RuniFiError::WalError {
                    path: self.wal_path().display().to_string(),
                    reason: format!("fsync failed: {e}"),
                })?;
        }

        Ok(())
    }

    fn recover(&self) -> Result<RecoveryState> {
        let checkpoint_path = self.checkpoint_path();
        let wal_path = self.wal_path();

        // Phase 1: Load checkpoint if present.
        let (mut entries, mut max_id) = match self.read_encrypted_checkpoint(&checkpoint_path)? {
            Some((state, mid)) => (state, mid),
            None => {
                // Try unencrypted checkpoint for migration.
                match wal_format::read_checkpoint(&checkpoint_path)? {
                    Some((state, mid)) => (state, mid),
                    None => (HashMap::new(), 0u64),
                }
            }
        };

        // Phase 2: Replay WAL on top.
        if wal_path.exists() {
            let records = self.read_encrypted_wal(&wal_path)?;

            for record in records {
                match record {
                    WalRecord::Upsert(sff) => {
                        if sff.id > max_id {
                            max_id = sff.id;
                        }
                        let queue_id = sff.queue_id.clone();
                        let ff = to_flowfile(&sff);
                        entries.insert(ff.id, (ff, queue_id));
                    }
                    WalRecord::Delete(id) => {
                        entries.remove(&id);
                    }
                    WalRecord::BatchEnd(_) => {}
                }
            }
        }

        // Build RecoveryState grouped by queue_id.
        let mut queued: HashMap<String, Vec<FlowFile>> = HashMap::new();
        for (ff, queue_id) in entries.values() {
            queued.entry(queue_id.clone()).or_default().push(ff.clone());
        }

        // Update in-memory state.
        let mut state = self.state.lock();
        state.entries = entries;
        state.max_id = max_id;

        drop(state);
        self.truncate_wal()?;

        Ok(RecoveryState { queued, max_id })
    }

    fn checkpoint(&self) -> Result<()> {
        let state = self.state.lock();
        self.write_encrypted_checkpoint(&self.checkpoint_path(), &state.entries, state.max_id)?;
        drop(state);

        self.truncate_wal()?;
        Ok(())
    }

    fn shutdown(&self) {
        let mut state = self.state.lock();
        let _ = state.writer.flush();
    }
}

impl EncryptedWalFlowFileRepository {
    /// Read and decrypt all records from a WAL file.
    fn read_encrypted_wal(&self, path: &Path) -> Result<Vec<WalRecord>> {
        let file = fs::File::open(path).map_err(|e| RuniFiError::WalError {
            path: path.display().to_string(),
            reason: format!("failed to open: {e}"),
        })?;
        let mut reader = std::io::BufReader::new(file);

        if !wal_format::read_wal_header(&mut reader)? {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        loop {
            let record_result = wal_format::read_record(&mut reader);
            let maybe_record = match record_result {
                Ok(r) => r,
                Err(RuniFiError::WalCorrupted { .. }) => {
                    tracing::warn!("Encrypted WAL corrupted during recovery, using records so far");
                    break;
                }
                Err(e) => return Err(e),
            };

            let Some((tag, encrypted_payload)) = maybe_record else {
                break;
            };
            // Decrypt the payload.
            let payload = match self.decrypt_payload(&encrypted_payload) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to decrypt WAL record, stopping replay"
                    );
                    break;
                }
            };

            let record = match tag {
                TAG_UPSERT => WalRecord::Upsert(wal_format::decode_upsert(&payload)?),
                TAG_DELETE => WalRecord::Delete(wal_format::decode_delete(&payload)?),
                TAG_BATCH_END => WalRecord::BatchEnd(wal_format::decode_batch_end(&payload)?),
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
}

/// Open an existing WAL for appending, or create a fresh one with a header.
fn open_or_create_wal(path: &Path) -> Result<File> {
    if path.exists() {
        let file =
            OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(|e| RuniFiError::WalError {
                    path: path.display().to_string(),
                    reason: format!("failed to open WAL for append: {e}"),
                })?;
        Ok(file)
    } else {
        create_fresh_wal(path)
    }
}

/// Create/truncate the WAL file and write the header.
fn create_fresh_wal(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| RuniFiError::WalError {
            path: path.display().to_string(),
            reason: format!("failed to create WAL: {e}"),
        })?;

    let mut writer = BufWriter::new(file);
    write_wal_header(&mut writer).map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("failed to write WAL header: {e}"),
    })?;
    writer.flush().map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("flush failed: {e}"),
    })?;

    writer.into_inner().map_err(|e| RuniFiError::WalError {
        path: path.display().to_string(),
        reason: format!("into_inner failed: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runifi_plugin_api::ContentClaim;

    use super::*;
    use crate::repository::static_key_provider::StaticKeyProvider;

    fn key_a() -> Vec<u8> {
        vec![0xAA; 32]
    }

    fn key_b() -> Vec<u8> {
        vec![0xBB; 32]
    }

    fn make_provider(keys: Vec<(&str, Vec<u8>)>, active: &str) -> Arc<dyn KeyProvider> {
        let mut map = HashMap::new();
        for (id, key) in keys {
            map.insert(id.to_string(), key);
        }
        Arc::new(StaticKeyProvider::new(map, active.to_string()).unwrap())
    }

    fn test_ff(id: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: vec![
                (Arc::from("key"), Arc::from("val")),
                (Arc::from("filename"), Arc::from("test.dat")),
            ],
            content_claim: Some(ContentClaim {
                resource_id: id * 10,
                offset: 0,
                length: 100,
            }),
            size: 100,
            created_at_nanos: 1000 + id,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    fn make_repo(dir: &Path, provider: Arc<dyn KeyProvider>) -> EncryptedWalFlowFileRepository {
        EncryptedWalFlowFileRepository::new(
            EncryptedWalConfig {
                dir: dir.to_path_buf(),
                fsync_mode: FsyncMode::Never,
                checkpoint_interval_secs: 60,
            },
            provider,
        )
        .unwrap()
    }

    #[test]
    fn write_and_recover() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());

        let ff1 = test_ff(1);
        let ff2 = test_ff(2);
        repo.commit_batch(&[
            FlowFileOp::Upsert {
                flowfile: &ff1,
                queue_id: "conn-0",
            },
            FlowFileOp::Upsert {
                flowfile: &ff2,
                queue_id: "conn-1",
            },
        ])
        .unwrap();

        // Simulate restart.
        let repo2 = make_repo(dir.path(), provider);
        let state = repo2.recover().unwrap();

        assert_eq!(state.max_id, 2);
        assert_eq!(state.queued["conn-0"].len(), 1);
        assert_eq!(state.queued["conn-1"].len(), 1);
        assert_eq!(state.queued["conn-0"][0].id, 1);
        assert_eq!(state.queued["conn-1"][0].id, 2);
    }

    #[test]
    fn wal_data_is_encrypted_on_disk() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider);

        let ff = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff,
            queue_id: "q",
        }])
        .unwrap();

        // Read the raw WAL file and verify it does not contain plaintext.
        let wal_data = fs::read(dir.path().join("wal.dat")).unwrap();
        let wal_str = String::from_utf8_lossy(&wal_data);

        // The attribute values should NOT appear in plaintext.
        assert!(
            !wal_str.contains("test.dat"),
            "WAL file should not contain plaintext attribute values"
        );
    }

    #[test]
    fn delete_removes_entry() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());

        let ff1 = test_ff(1);
        let ff2 = test_ff(2);
        repo.commit_batch(&[
            FlowFileOp::Upsert {
                flowfile: &ff1,
                queue_id: "q",
            },
            FlowFileOp::Upsert {
                flowfile: &ff2,
                queue_id: "q",
            },
        ])
        .unwrap();

        repo.commit_batch(&[FlowFileOp::Delete { id: 1 }]).unwrap();

        let repo2 = make_repo(dir.path(), provider);
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 1);
        assert_eq!(state.queued["q"][0].id, 2);
    }

    #[test]
    fn checkpoint_and_continue() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();

        repo.checkpoint().unwrap();

        let ff2 = test_ff(2);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff2,
            queue_id: "q",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path(), provider);
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 2);
        assert_eq!(state.max_id, 2);
    }

    #[test]
    fn checkpoint_data_is_encrypted() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider);

        let ff = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff,
            queue_id: "q",
        }])
        .unwrap();

        repo.checkpoint().unwrap();

        // Read the raw checkpoint file and verify it does not contain plaintext.
        let chk_data = fs::read(dir.path().join("checkpoint.dat")).unwrap();
        let chk_str = String::from_utf8_lossy(&chk_data);

        assert!(
            !chk_str.contains("test.dat"),
            "Checkpoint file should not contain plaintext attribute values"
        );
    }

    #[test]
    fn key_rotation_old_wal_readable() {
        let dir = tempfile::TempDir::new().unwrap();

        // Write with key-a.
        let provider_a = make_provider(vec![("key-a", key_a())], "key-a");
        let repo = make_repo(dir.path(), provider_a);

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();
        repo.shutdown();

        // Recover with key-b as active, but key-a still available.
        let provider_ab = make_provider(vec![("key-a", key_a()), ("key-b", key_b())], "key-b");
        let repo2 = make_repo(dir.path(), provider_ab);
        let state = repo2.recover().unwrap();

        assert_eq!(state.queued["q"].len(), 1);
        assert_eq!(state.queued["q"][0].id, 1);
    }

    #[test]
    fn key_rotation_new_writes_use_new_key() {
        let dir = tempfile::TempDir::new().unwrap();

        // Write with key-a, then checkpoint.
        let provider_a = make_provider(vec![("key-a", key_a())], "key-a");
        let repo = make_repo(dir.path(), provider_a);

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();
        repo.checkpoint().unwrap();
        repo.shutdown();

        // Write with key-b as active (key-a still available).
        let provider_ab = make_provider(vec![("key-a", key_a()), ("key-b", key_b())], "key-b");
        let repo2 = make_repo(dir.path(), provider_ab.clone());
        // Recover first to load checkpoint.
        let _state = repo2.recover().unwrap();

        let ff2 = test_ff(2);
        repo2
            .commit_batch(&[FlowFileOp::Upsert {
                flowfile: &ff2,
                queue_id: "q",
            }])
            .unwrap();
        repo2.shutdown();

        // Final recovery with both keys.
        let repo3 = make_repo(dir.path(), provider_ab);
        let state = repo3.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 2);
    }

    #[test]
    fn wrong_key_fails_recovery() {
        let dir = tempfile::TempDir::new().unwrap();

        let provider_a = make_provider(vec![("key-a", key_a())], "key-a");
        let repo = make_repo(dir.path(), provider_a);

        let ff = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff,
            queue_id: "q",
        }])
        .unwrap();
        repo.shutdown();

        // Try to recover with a different key (key-b only, no key-a).
        let provider_b = make_provider(vec![("key-b", key_b())], "key-b");
        let repo2 = make_repo(dir.path(), provider_b);

        // Recovery should succeed but with no records (decryption failure is treated
        // as corruption, which stops replay).
        let state = repo2.recover().unwrap();
        assert!(state.queued.is_empty());
    }

    #[test]
    fn empty_batch_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());
        repo.commit_batch(&[]).unwrap();

        let state = repo.recover().unwrap();
        assert!(state.queued.is_empty());
    }

    #[test]
    fn empty_recovery_returns_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider);
        let state = repo.recover().unwrap();
        assert!(state.queued.is_empty());
        assert_eq!(state.max_id, 0);
    }

    #[test]
    fn multiple_batches_accumulate() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());

        let ff1 = test_ff(1);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff1,
            queue_id: "q",
        }])
        .unwrap();

        let ff2 = test_ff(2);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff2,
            queue_id: "q",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path(), provider);
        let state = repo2.recover().unwrap();
        assert_eq!(state.queued["q"].len(), 2);
        assert_eq!(state.max_id, 2);
    }

    #[test]
    fn attributes_preserved_through_encryption() {
        let dir = tempfile::TempDir::new().unwrap();
        let provider = make_provider(vec![("key-1", key_a())], "key-1");
        let repo = make_repo(dir.path(), provider.clone());

        let ff = test_ff(42);
        repo.commit_batch(&[FlowFileOp::Upsert {
            flowfile: &ff,
            queue_id: "conn-0",
        }])
        .unwrap();

        let repo2 = make_repo(dir.path(), provider);
        let state = repo2.recover().unwrap();

        let recovered = &state.queued["conn-0"][0];
        assert_eq!(recovered.id, 42);
        assert_eq!(recovered.size, 100);
        assert_eq!(recovered.attributes.len(), 2);
        assert_eq!(recovered.content_claim.as_ref().unwrap().resource_id, 420);
    }
}
