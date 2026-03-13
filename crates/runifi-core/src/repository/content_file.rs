use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use runifi_plugin_api::ContentClaim;
use tokio_util::sync::CancellationToken;

use super::content_repo::ContentRepository;
use super::segment::{BlobLocation, Container, SegmentCache, read_active_segment};
use crate::error::{Result, RuniFiError};
use crate::id::IdGenerator;

/// Default inline threshold: content <= this size stays in memory.
const DEFAULT_INLINE_THRESHOLD: u64 = 64 * 1024; // 64 KB

/// Default memory eviction threshold.
const DEFAULT_MEMORY_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB

/// Default max segment file size.
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 128 * 1024 * 1024; // 128 MB

/// Default cleanup interval in seconds.
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 30;

/// Configuration for the file-based content repository.
#[derive(Debug, Clone)]
pub struct FileContentRepoConfig {
    pub containers: Vec<PathBuf>,
    pub max_segment_size: u64,
    pub memory_threshold: u64,
    pub inline_threshold: u64,
    pub cleanup_interval_secs: u64,
}

impl Default for FileContentRepoConfig {
    fn default() -> Self {
        Self {
            containers: vec![PathBuf::from("./content-repo")],
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            memory_threshold: DEFAULT_MEMORY_THRESHOLD,
            inline_threshold: DEFAULT_INLINE_THRESHOLD,
            cleanup_interval_secs: DEFAULT_CLEANUP_INTERVAL_SECS,
        }
    }
}

/// Storage tier for a resource.
#[derive(Debug)]
pub(crate) enum StorageTier {
    /// Content stored inline in memory.
    Memory(Bytes),
    /// Content stored on disk in a segment file.
    Disk(BlobLocation),
}

/// Metadata + storage for a single content resource.
pub(crate) struct ResourceEntry {
    pub(crate) tier: RwLock<StorageTier>,
    pub(crate) ref_count: AtomicU64,
    pub(crate) last_access_nanos: AtomicU64,
}

/// Hybrid memory/disk content repository.
///
/// Small content (<= inline_threshold) is kept in memory for fast access.
/// Large content is appended to segment files on disk with CRC32 integrity.
/// Background cleanup handles GC, memory eviction, and dead segment deletion.
pub struct FileContentRepository {
    index: DashMap<u64, ResourceEntry>,
    id_gen: IdGenerator,
    containers: Vec<Container>,
    next_container: AtomicU64,
    segment_cache: Arc<SegmentCache>,
    pub(crate) memory_bytes: AtomicU64,
    config: FileContentRepoConfig,
    cleanup_cancel: CancellationToken,
}

impl FileContentRepository {
    /// Create a new file-based content repository.
    pub fn new(config: FileContentRepoConfig) -> Result<Self> {
        let segment_cache = Arc::new(SegmentCache::new());

        let mut containers = Vec::with_capacity(config.containers.len());
        for dir in &config.containers {
            containers.push(Container::new(dir.clone(), config.max_segment_size)?);
        }

        if containers.is_empty() {
            return Err(RuniFiError::Config(
                "at least one content repository container is required".to_string(),
            ));
        }

        Ok(Self {
            index: DashMap::new(),
            id_gen: IdGenerator::new(),
            containers,
            next_container: AtomicU64::new(0),
            segment_cache,
            memory_bytes: AtomicU64::new(0),
            config,
            cleanup_cancel: CancellationToken::new(),
        })
    }

    /// Get the cancellation token for the cleanup task.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cleanup_cancel.clone()
    }

    /// Get a reference to the segment cache (needed by cleanup).
    pub fn segment_cache(&self) -> &Arc<SegmentCache> {
        &self.segment_cache
    }

    /// Get current memory usage in bytes.
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Get the config.
    pub fn config(&self) -> &FileContentRepoConfig {
        &self.config
    }

    /// Get access to the resource index (needed by cleanup for eviction).
    pub(crate) fn index(&self) -> &DashMap<u64, ResourceEntry> {
        &self.index
    }

    /// Select the next container in round-robin fashion.
    fn next_container(&self) -> &Container {
        let idx = self.next_container.fetch_add(1, Ordering::Relaxed) as usize;
        &self.containers[idx % self.containers.len()]
    }

    fn now_nanos() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    /// Write data to disk via a container's segment writer.
    fn write_to_disk(&self, resource_id: u64, data: &[u8]) -> Result<BlobLocation> {
        let container = self.next_container();

        let mut writer = container.writer.lock();

        // Check if we need to rotate
        if writer.would_exceed(data.len() as u64) {
            writer.flush()?;
            let old_path = writer.path().to_path_buf();

            // Seal the old segment
            self.segment_cache.seal_segment(&old_path)?;

            // Create new segment
            let next_id = container.segment_counter.fetch_add(1, Ordering::Relaxed) + 1;
            let new_path = container.dir.join(format!("segment-{next_id:010}.dat"));
            *writer = super::segment::SegmentWriter::create(new_path, container.max_segment_size)?;
        }

        let location = writer.append(resource_id, data)?;
        // Flush so pread can see the data on active (non-sealed) segments.
        writer.flush()?;
        self.segment_cache.increment_live(writer.path());

        Ok(location)
    }

    /// Evict a memory-tier resource to disk.
    /// Returns the freed memory size, or 0 if eviction wasn't possible.
    pub fn evict_to_disk(&self, resource_id: u64) -> Result<u64> {
        let entry = match self.index.get(&resource_id) {
            Some(e) => e,
            None => return Ok(0),
        };

        let freed = {
            let tier = entry.tier.read();
            match &*tier {
                StorageTier::Memory(data) => {
                    let data_clone = data.clone();
                    let length = data_clone.len() as u64;
                    drop(tier);

                    let location = self.write_to_disk(resource_id, &data_clone)?;
                    let mut tier = entry.tier.write();
                    // Double-check it's still in memory (avoid double-evict)
                    if matches!(&*tier, StorageTier::Memory(_)) {
                        *tier = StorageTier::Disk(location);
                        self.memory_bytes.fetch_sub(length, Ordering::Relaxed);
                        length
                    } else {
                        0
                    }
                }
                StorageTier::Disk(_) => 0,
            }
        };

        Ok(freed)
    }
}

impl ContentRepository for FileContentRepository {
    fn create(&self, data: Bytes) -> Result<ContentClaim> {
        let resource_id = self.id_gen.next_id();
        let length = data.len() as u64;

        let tier = if length <= self.config.inline_threshold {
            self.memory_bytes.fetch_add(length, Ordering::Relaxed);
            StorageTier::Memory(data)
        } else {
            let location = self.write_to_disk(resource_id, &data)?;
            StorageTier::Disk(location)
        };

        self.index.insert(
            resource_id,
            ResourceEntry {
                tier: RwLock::new(tier),
                ref_count: AtomicU64::new(1),
                last_access_nanos: AtomicU64::new(Self::now_nanos()),
            },
        );

        Ok(ContentClaim {
            resource_id,
            offset: 0,
            length,
        })
    }

    fn read(&self, claim: &ContentClaim) -> Result<Bytes> {
        let entry = self
            .index
            .get(&claim.resource_id)
            .ok_or(RuniFiError::ContentNotFound(claim.resource_id))?;

        entry
            .last_access_nanos
            .store(Self::now_nanos(), Ordering::Relaxed);

        let tier = entry.tier.read();
        let full_data = match &*tier {
            StorageTier::Memory(data) => data.clone(),
            StorageTier::Disk(location) => {
                // Try sealed cache first, fall back to pread
                self.segment_cache
                    .read_sealed(location)
                    .or_else(|_| read_active_segment(location))?
            }
        };

        // Apply claim offset + length
        let start = claim.offset as usize;
        let end = start + claim.length as usize;
        if end > full_data.len() {
            return Err(RuniFiError::ContentNotFound(claim.resource_id));
        }

        Ok(full_data.slice(start..end))
    }

    fn increment_ref(&self, resource_id: u64) -> Result<()> {
        let entry = self
            .index
            .get(&resource_id)
            .ok_or(RuniFiError::ContentNotFound(resource_id))?;
        entry.ref_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn decrement_ref(&self, resource_id: u64) -> Result<()> {
        let should_remove = {
            let entry = self
                .index
                .get(&resource_id)
                .ok_or(RuniFiError::ContentNotFound(resource_id))?;
            entry.ref_count.fetch_sub(1, Ordering::AcqRel) == 1
        };

        if should_remove && let Some((_, entry)) = self.index.remove(&resource_id) {
            let tier = entry.tier.into_inner();
            match tier {
                StorageTier::Memory(data) => {
                    self.memory_bytes
                        .fetch_sub(data.len() as u64, Ordering::Relaxed);
                }
                StorageTier::Disk(location) => {
                    self.segment_cache.decrement_live(&location.segment_path);
                }
            }
        }

        Ok(())
    }

    fn shutdown(&self) {
        self.cleanup_cancel.cancel();

        // Flush all segment writers
        for container in &self.containers {
            let mut writer = container.writer.lock();
            let _ = writer.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_repo(inline_threshold: u64) -> (TempDir, FileContentRepository) {
        let dir = TempDir::new().unwrap();
        let config = FileContentRepoConfig {
            containers: vec![dir.path().join("c1")],
            max_segment_size: 1024 * 1024,
            memory_threshold: 1024 * 1024,
            inline_threshold,
            cleanup_interval_secs: 60,
        };
        let repo = FileContentRepository::new(config).unwrap();
        (dir, repo)
    }

    #[test]
    fn small_content_stays_in_memory() {
        let (_dir, repo) = test_repo(1024);
        let data = Bytes::from(vec![0xABu8; 512]);
        let claim = repo.create(data.clone()).unwrap();

        assert_eq!(repo.memory_bytes(), 512);
        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn large_content_goes_to_disk() {
        let (_dir, repo) = test_repo(64);
        let data = Bytes::from(vec![0xCDu8; 256]);
        let claim = repo.create(data.clone()).unwrap();

        // Should not count against memory
        assert_eq!(repo.memory_bytes(), 0);

        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn read_with_offset_and_length() {
        let (_dir, repo) = test_repo(1024);
        let data = Bytes::from_static(b"abcdefghij");
        let claim = repo.create(data).unwrap();

        let partial = ContentClaim {
            resource_id: claim.resource_id,
            offset: 3,
            length: 4,
        };
        let read_back = repo.read(&partial).unwrap();
        assert_eq!(&read_back[..], b"defg");
    }

    #[test]
    fn ref_counting_frees_memory() {
        let (_dir, repo) = test_repo(1024);
        let data = Bytes::from(vec![0u8; 100]);
        let claim = repo.create(data).unwrap();

        assert_eq!(repo.memory_bytes(), 100);

        repo.increment_ref(claim.resource_id).unwrap();
        repo.decrement_ref(claim.resource_id).unwrap();
        // Still alive (ref=1), memory still used
        assert_eq!(repo.memory_bytes(), 100);

        repo.decrement_ref(claim.resource_id).unwrap();
        // Now freed
        assert_eq!(repo.memory_bytes(), 0);
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn ref_counting_frees_disk() {
        let (_dir, repo) = test_repo(32);
        let data = Bytes::from(vec![0xFFu8; 128]);
        let claim = repo.create(data).unwrap();

        // Should be on disk
        assert_eq!(repo.memory_bytes(), 0);
        assert!(repo.read(&claim).is_ok());

        repo.decrement_ref(claim.resource_id).unwrap();
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn concurrent_creates() {
        let (_dir, repo) = test_repo(1024);
        let repo = Arc::new(repo);
        let mut handles = Vec::new();

        for i in 0..8 {
            let repo = repo.clone();
            handles.push(std::thread::spawn(move || {
                let data = Bytes::from(vec![i as u8; 64]);
                let claim = repo.create(data.clone()).unwrap();
                let read_back = repo.read(&claim).unwrap();
                assert_eq!(read_back, data);
                claim
            }));
        }

        let claims: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All unique resource IDs
        let mut ids: Vec<_> = claims.iter().map(|c| c.resource_id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 8);
    }

    #[test]
    fn read_nonexistent_fails() {
        let (_dir, repo) = test_repo(1024);
        let claim = ContentClaim {
            resource_id: 999,
            offset: 0,
            length: 10,
        };
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn evict_to_disk() {
        let (_dir, repo) = test_repo(1024);
        let data = Bytes::from(vec![0xBBu8; 200]);
        let claim = repo.create(data.clone()).unwrap();

        assert_eq!(repo.memory_bytes(), 200);

        let freed = repo.evict_to_disk(claim.resource_id).unwrap();
        assert_eq!(freed, 200);
        assert_eq!(repo.memory_bytes(), 0);

        // Should still be readable from disk
        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn shutdown_flushes() {
        let (_dir, repo) = test_repo(32);
        let data = Bytes::from(vec![0xEEu8; 100]);
        let claim = repo.create(data.clone()).unwrap();

        repo.shutdown();

        // Data should still be readable after shutdown
        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }
}
