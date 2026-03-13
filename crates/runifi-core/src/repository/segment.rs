use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;

use crate::error::{Result, RuniFiError};

/// Magic header for segment files: "RUNIFI01"
const SEGMENT_MAGIC: &[u8; 8] = b"RUNIFI01";

/// Per-record header: 8B resource_id + 8B length = 16 bytes
const RECORD_HEADER_SIZE: u64 = 16;

/// CRC32 trailer: 4 bytes
const CRC_SIZE: u64 = 4;

/// Location of a blob within a segment file.
#[derive(Debug, Clone)]
pub struct BlobLocation {
    pub segment_path: PathBuf,
    pub data_offset: u64,
    pub data_length: u64,
}

/// Append-only segment file writer.
///
/// Segment format:
/// ```text
/// [8 bytes: "RUNIFI01" magic]
/// [repeat: 8B resource_id | 8B length | N bytes data | 4B CRC32]
/// ```
pub struct SegmentWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    current_offset: u64,
    max_size: u64,
}

impl SegmentWriter {
    /// Create a new segment file and write the magic header.
    pub fn create(path: PathBuf, max_size: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| RuniFiError::SegmentError {
                path: path.display().to_string(),
                reason: format!("failed to create directory: {e}"),
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| RuniFiError::SegmentError {
                path: path.display().to_string(),
                reason: format!("failed to create segment: {e}"),
            })?;

        let mut writer = BufWriter::with_capacity(256 * 1024, file);
        writer
            .write_all(SEGMENT_MAGIC)
            .map_err(|e| RuniFiError::SegmentError {
                path: path.display().to_string(),
                reason: format!("failed to write magic: {e}"),
            })?;

        Ok(Self {
            writer,
            path,
            current_offset: SEGMENT_MAGIC.len() as u64,
            max_size,
        })
    }

    /// Check if appending `data_len` bytes would exceed the segment's max size.
    pub fn would_exceed(&self, data_len: u64) -> bool {
        let record_size = RECORD_HEADER_SIZE + data_len + CRC_SIZE;
        self.current_offset + record_size > self.max_size
    }

    /// Append a blob to the segment. Returns the location of the data within the file.
    pub fn append(&mut self, resource_id: u64, data: &[u8]) -> Result<BlobLocation> {
        let data_len = data.len() as u64;

        // Write resource_id (8 bytes LE)
        self.writer
            .write_all(&resource_id.to_le_bytes())
            .map_err(|e| self.io_err(&e))?;

        // Write data length (8 bytes LE)
        self.writer
            .write_all(&data_len.to_le_bytes())
            .map_err(|e| self.io_err(&e))?;

        // Record the offset where actual data starts
        let data_offset = self.current_offset + RECORD_HEADER_SIZE;

        // Write data
        self.writer.write_all(data).map_err(|e| self.io_err(&e))?;

        // Write CRC32
        let crc = crc32fast::hash(data);
        self.writer
            .write_all(&crc.to_le_bytes())
            .map_err(|e| self.io_err(&e))?;

        let location = BlobLocation {
            segment_path: self.path.clone(),
            data_offset,
            data_length: data_len,
        };

        self.current_offset += RECORD_HEADER_SIZE + data_len + CRC_SIZE;
        Ok(location)
    }

    /// Flush buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| self.io_err(&e))
    }

    /// The path of this segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Current write offset (file size so far).
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    fn io_err(&self, e: &std::io::Error) -> RuniFiError {
        RuniFiError::SegmentError {
            path: self.path.display().to_string(),
            reason: e.to_string(),
        }
    }
}

/// Cache of mmap-backed sealed (read-only) segments for zero-copy reads.
pub struct SegmentCache {
    /// Sealed segment mmaps keyed by path.
    maps: DashMap<PathBuf, Bytes>,
    /// Live blob count per segment path (for dead segment detection).
    live_counts: DashMap<PathBuf, AtomicU64>,
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentCache {
    pub fn new() -> Self {
        Self {
            maps: DashMap::new(),
            live_counts: DashMap::new(),
        }
    }

    /// Seal a segment file: mmap it and cache the mapping for zero-copy reads.
    pub fn seal_segment(&self, path: &Path) -> Result<()> {
        let file = File::open(path).map_err(|e| RuniFiError::SegmentError {
            path: path.display().to_string(),
            reason: format!("failed to open for sealing: {e}"),
        })?;

        // Safety: file is sealed (read-only from this point), and we trust the OS mmap.
        let mmap = unsafe {
            memmap2::Mmap::map(&file).map_err(|e| RuniFiError::SegmentError {
                path: path.display().to_string(),
                reason: format!("failed to mmap: {e}"),
            })?
        };

        let bytes = Bytes::from(Vec::from(mmap.as_ref()));
        self.maps.insert(path.to_path_buf(), bytes);
        Ok(())
    }

    /// Read data from a sealed (mmapped) segment — zero-copy via Bytes::slice.
    pub fn read_sealed(&self, location: &BlobLocation) -> Result<Bytes> {
        let entry =
            self.maps
                .get(&location.segment_path)
                .ok_or_else(|| RuniFiError::SegmentError {
                    path: location.segment_path.display().to_string(),
                    reason: "segment not in cache".to_string(),
                })?;

        let data = entry.value();
        let start = location.data_offset as usize;
        let end = start + location.data_length as usize;

        if end > data.len() {
            return Err(RuniFiError::SegmentError {
                path: location.segment_path.display().to_string(),
                reason: format!(
                    "read out of bounds: offset={start}, len={}, file_len={}",
                    location.data_length,
                    data.len()
                ),
            });
        }

        Ok(data.slice(start..end))
    }

    /// Increment live blob count for a segment.
    pub fn increment_live(&self, path: &Path) {
        self.live_counts
            .entry(path.to_path_buf())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement live blob count for a segment.
    pub fn decrement_live(&self, path: &Path) {
        if let Some(count) = self.live_counts.get(path) {
            count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get live blob count for a segment.
    pub fn live_count(&self, path: &Path) -> u64 {
        self.live_counts
            .get(path)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Remove a sealed segment from the cache and delete the file.
    pub fn remove_segment(&self, path: &Path) -> Result<()> {
        self.maps.remove(path);
        self.live_counts.remove(path);
        if path.exists() {
            fs::remove_file(path).map_err(|e| RuniFiError::SegmentError {
                path: path.display().to_string(),
                reason: format!("failed to delete: {e}"),
            })?;
        }
        Ok(())
    }

    /// List all sealed segment paths.
    pub fn sealed_paths(&self) -> Vec<PathBuf> {
        self.maps.iter().map(|e| e.key().clone()).collect()
    }
}

/// Read data from an active (non-sealed) segment using pread.
pub fn read_active_segment(location: &BlobLocation) -> Result<Bytes> {
    let file = File::open(&location.segment_path).map_err(|e| RuniFiError::SegmentError {
        path: location.segment_path.display().to_string(),
        reason: format!("failed to open: {e}"),
    })?;

    let mut buf = vec![0u8; location.data_length as usize];
    file.read_at(&mut buf, location.data_offset)
        .map_err(|e| RuniFiError::SegmentError {
            path: location.segment_path.display().to_string(),
            reason: format!("pread failed: {e}"),
        })?;

    // Verify CRC: read the 4-byte CRC immediately after the data
    let mut crc_buf = [0u8; 4];
    file.read_at(&mut crc_buf, location.data_offset + location.data_length)
        .map_err(|e| RuniFiError::SegmentError {
            path: location.segment_path.display().to_string(),
            reason: format!("CRC read failed: {e}"),
        })?;

    let stored_crc = u32::from_le_bytes(crc_buf);
    let computed_crc = crc32fast::hash(&buf);
    if stored_crc != computed_crc {
        return Err(RuniFiError::ContentCorrupted {
            resource_id: 0,
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    Ok(Bytes::from(buf))
}

/// Container represents a storage directory with its active segment writer.
pub struct Container {
    pub dir: PathBuf,
    pub writer: parking_lot::Mutex<SegmentWriter>,
    pub segment_counter: AtomicU64,
    pub max_segment_size: u64,
}

impl Container {
    /// Create a new container in the given directory.
    pub fn new(dir: PathBuf, max_segment_size: u64) -> Result<Self> {
        fs::create_dir_all(&dir).map_err(|e| RuniFiError::SegmentError {
            path: dir.display().to_string(),
            reason: format!("failed to create container dir: {e}"),
        })?;

        let segment_path = dir.join("segment-0000000001.dat");
        let writer = SegmentWriter::create(segment_path, max_segment_size)?;

        Ok(Self {
            dir,
            writer: parking_lot::Mutex::new(writer),
            segment_counter: AtomicU64::new(1),
            max_segment_size,
        })
    }

    /// Rotate to a new segment file. Returns the path of the old (now sealed) segment.
    pub fn rotate(&self, cache: &Arc<SegmentCache>) -> Result<PathBuf> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        let old_path = writer.path().to_path_buf();

        // Seal the old segment
        cache.seal_segment(&old_path)?;

        // Create new segment
        let next_id = self.segment_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let new_path = self.dir.join(format!("segment-{next_id:010}.dat"));
        *writer = SegmentWriter::create(new_path, self.max_segment_size)?;

        Ok(old_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_segment(max_size: u64) -> (TempDir, SegmentWriter) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test-segment.dat");
        let writer = SegmentWriter::create(path, max_size).unwrap();
        (dir, writer)
    }

    #[test]
    fn write_and_read_blob() {
        let (_dir, mut writer) = temp_segment(1024 * 1024);
        let data = b"hello segment world";
        let loc = writer.append(42, data).unwrap();
        writer.flush().unwrap();

        assert_eq!(loc.data_offset, 8 + 16); // magic + header
        assert_eq!(loc.data_length, data.len() as u64);

        let read_back = read_active_segment(&loc).unwrap();
        assert_eq!(&read_back[..], data);
    }

    #[test]
    fn multiple_blobs() {
        let (_dir, mut writer) = temp_segment(1024 * 1024);

        let loc1 = writer.append(1, b"first").unwrap();
        let loc2 = writer.append(2, b"second").unwrap();
        writer.flush().unwrap();

        assert_eq!(&read_active_segment(&loc1).unwrap()[..], b"first");
        assert_eq!(&read_active_segment(&loc2).unwrap()[..], b"second");
    }

    #[test]
    fn would_exceed_check() {
        let (_dir, writer) = temp_segment(100);
        // magic(8) + header(16) + data + crc(4) must fit in 100
        // Available: 100 - 8 = 92 for records
        // One record overhead: 16 + 4 = 20, so max data = 72
        assert!(!writer.would_exceed(72));
        assert!(writer.would_exceed(73));
    }

    #[test]
    fn segment_rotation() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(SegmentCache::new());
        let container = Container::new(dir.path().join("container"), 256).unwrap();

        // Write a small blob
        {
            let mut w = container.writer.lock();
            w.append(1, b"test data").unwrap();
            w.flush().unwrap();
        }

        // Rotate
        let old_path = container.rotate(&cache).unwrap();
        assert!(cache.sealed_paths().contains(&old_path));

        // Write to new segment
        {
            let mut w = container.writer.lock();
            let loc = w.append(2, b"new segment data").unwrap();
            w.flush().unwrap();
            let data = read_active_segment(&loc).unwrap();
            assert_eq!(&data[..], b"new segment data");
        }
    }

    #[test]
    fn sealed_segment_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sealed.dat");
        let mut writer = SegmentWriter::create(path.clone(), 1024 * 1024).unwrap();

        let loc = writer.append(10, b"sealed data").unwrap();
        writer.flush().unwrap();
        drop(writer);

        let cache = SegmentCache::new();
        cache.seal_segment(&path).unwrap();

        let data = cache.read_sealed(&loc).unwrap();
        assert_eq!(&data[..], b"sealed data");
    }

    #[test]
    fn crc_integrity() {
        let (_dir, mut writer) = temp_segment(1024 * 1024);
        let data = b"integrity check data";
        let loc = writer.append(99, data).unwrap();
        writer.flush().unwrap();

        // Corrupt the data file
        let file = OpenOptions::new()
            .write(true)
            .open(&loc.segment_path)
            .unwrap();
        file.write_at(b"X", loc.data_offset).unwrap();

        // Read should detect corruption
        let result = read_active_segment(&loc);
        assert!(result.is_err());
        match result.unwrap_err() {
            RuniFiError::ContentCorrupted { .. } => {}
            other => panic!("expected ContentCorrupted, got: {other}"),
        }
    }
}
