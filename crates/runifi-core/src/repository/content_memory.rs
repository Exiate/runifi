use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use runifi_plugin_api::ContentClaim;

use super::content_repo::ContentRepository;
use crate::error::{Result, RuniFiError};
use crate::id::IdGenerator;

/// In-memory content repository using DashMap for concurrent access.
///
/// Content is stored as `Bytes` (zero-copy slicing) with atomic reference counting.
/// When a ref count drops to zero, the entry is removed from the map.
pub struct InMemoryContentRepository {
    store: DashMap<u64, (Bytes, AtomicU64)>,
    id_gen: IdGenerator,
}

impl InMemoryContentRepository {
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
            id_gen: IdGenerator::new(),
        }
    }
}

impl Default for InMemoryContentRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentRepository for InMemoryContentRepository {
    fn create(&self, data: Bytes) -> Result<ContentClaim> {
        let resource_id = self.id_gen.next_id();
        let length = data.len() as u64;
        self.store.insert(resource_id, (data, AtomicU64::new(1)));
        Ok(ContentClaim {
            resource_id,
            offset: 0,
            length,
        })
    }

    fn read(&self, claim: &ContentClaim) -> Result<Bytes> {
        let entry = self
            .store
            .get(&claim.resource_id)
            .ok_or(RuniFiError::ContentNotFound(claim.resource_id))?;
        let (data, _) = entry.value();
        let start = claim.offset as usize;
        let end = start + claim.length as usize;
        if end > data.len() {
            return Err(RuniFiError::ContentNotFound(claim.resource_id));
        }
        Ok(data.slice(start..end))
    }

    fn increment_ref(&self, resource_id: u64) -> Result<()> {
        let entry = self
            .store
            .get(&resource_id)
            .ok_or(RuniFiError::ContentNotFound(resource_id))?;
        entry.value().1.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn decrement_ref(&self, resource_id: u64) -> Result<()> {
        // Check current ref count first.
        let should_remove = {
            let entry = self
                .store
                .get(&resource_id)
                .ok_or(RuniFiError::ContentNotFound(resource_id))?;
            let prev = entry.value().1.fetch_sub(1, Ordering::AcqRel);
            prev == 1
        };
        if should_remove && let Some((_, (data, _))) = self.store.remove(&resource_id) {
            // Secure deletion: overwrite the buffer with zeros before dropping.
            // Convert Bytes -> BytesMut (always succeeds since we hold the sole
            // reference after removal from DashMap), then zero via write_volatile
            // to prevent the compiler from eliding the zeroing as a dead store.
            let mut mutable = BytesMut::from(data);
            for byte in mutable.iter_mut() {
                // SAFETY: write_volatile prevents optimization of the zero write.
                unsafe { std::ptr::write_volatile(byte, 0) };
            }
            drop(mutable);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read() {
        let repo = InMemoryContentRepository::new();
        let data = Bytes::from_static(b"hello world");
        let claim = repo.create(data.clone()).unwrap();
        assert_eq!(claim.offset, 0);
        assert_eq!(claim.length, 11);

        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn read_with_offset() {
        let repo = InMemoryContentRepository::new();
        let data = Bytes::from_static(b"hello world");
        let claim = repo.create(data).unwrap();

        let partial = ContentClaim {
            resource_id: claim.resource_id,
            offset: 6,
            length: 5,
        };
        let read_back = repo.read(&partial).unwrap();
        assert_eq!(read_back, Bytes::from_static(b"world"));
    }

    #[test]
    fn ref_counting_gc() {
        let repo = InMemoryContentRepository::new();
        let claim = repo.create(Bytes::from_static(b"data")).unwrap();

        repo.increment_ref(claim.resource_id).unwrap();
        // ref count = 2
        repo.decrement_ref(claim.resource_id).unwrap();
        // ref count = 1, still alive
        assert!(repo.read(&claim).is_ok());

        repo.decrement_ref(claim.resource_id).unwrap();
        // ref count = 0, should be removed
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn read_missing_resource_fails() {
        let repo = InMemoryContentRepository::new();
        let claim = ContentClaim {
            resource_id: 999,
            offset: 0,
            length: 10,
        };
        assert!(repo.read(&claim).is_err());
    }

    #[test]
    fn create_empty_content() {
        let repo = InMemoryContentRepository::new();
        let claim = repo.create(Bytes::new()).unwrap();
        assert_eq!(claim.length, 0);
        assert_eq!(claim.offset, 0);
        let data = repo.read(&claim).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn multiple_creates_produce_unique_ids() {
        let repo = InMemoryContentRepository::new();
        let c1 = repo.create(Bytes::from_static(b"a")).unwrap();
        let c2 = repo.create(Bytes::from_static(b"b")).unwrap();
        let c3 = repo.create(Bytes::from_static(b"c")).unwrap();
        assert_ne!(c1.resource_id, c2.resource_id);
        assert_ne!(c2.resource_id, c3.resource_id);
    }

    #[test]
    fn zero_copy_slicing() {
        let repo = InMemoryContentRepository::new();
        let data = Bytes::from_static(b"hello world");
        let claim = repo.create(data).unwrap();

        // Read the full content.
        let full = repo.read(&claim).unwrap();
        assert_eq!(full, Bytes::from_static(b"hello world"));

        // Read a slice.
        let slice_claim = ContentClaim {
            resource_id: claim.resource_id,
            offset: 6,
            length: 5,
        };
        let slice = repo.read(&slice_claim).unwrap();
        assert_eq!(slice, Bytes::from_static(b"world"));
    }

    #[test]
    fn out_of_bounds_read_fails() {
        let repo = InMemoryContentRepository::new();
        let claim = repo.create(Bytes::from_static(b"short")).unwrap();
        let bad_claim = ContentClaim {
            resource_id: claim.resource_id,
            offset: 0,
            length: 100, // Way beyond actual size.
        };
        assert!(repo.read(&bad_claim).is_err());
    }

    #[test]
    fn increment_ref_on_missing_resource_fails() {
        let repo = InMemoryContentRepository::new();
        assert!(repo.increment_ref(9999).is_err());
    }

    #[test]
    fn decrement_ref_on_missing_resource_fails() {
        let repo = InMemoryContentRepository::new();
        assert!(repo.decrement_ref(9999).is_err());
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let repo = Arc::new(InMemoryContentRepository::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let repo = repo.clone();
            handles.push(thread::spawn(move || {
                let data = Bytes::from(format!("content-{}", i));
                let claim = repo.create(data.clone()).unwrap();
                let read_back = repo.read(&claim).unwrap();
                assert_eq!(read_back, data);
                claim
            }));
        }

        let claims: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All claims should have unique IDs.
        let mut ids: Vec<u64> = claims.iter().map(|c| c.resource_id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 10);
    }

    #[test]
    fn large_content() {
        let repo = InMemoryContentRepository::new();
        let data = Bytes::from(vec![0xABu8; 1024 * 1024]); // 1 MB
        let claim = repo.create(data.clone()).unwrap();
        assert_eq!(claim.length, 1024 * 1024);
        let read_back = repo.read(&claim).unwrap();
        assert_eq!(read_back.len(), 1024 * 1024);
        assert_eq!(read_back, data);
    }
}
