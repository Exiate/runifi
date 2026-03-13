use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::content_file::FileContentRepository;

/// Spawn the background cleanup task for a `FileContentRepository`.
///
/// Runs on a configurable interval performing:
/// 1. Garbage collection — remove ref_count=0 entries (safety net)
/// 2. Memory eviction — spill oldest-accessed memory entries to disk when over threshold
/// 3. Dead segment deletion — remove segment files with zero live blobs
pub fn spawn_cleanup_task(repo: Arc<FileContentRepository>) -> tokio::task::JoinHandle<()> {
    let cancel = repo.cancel_token();
    let interval_secs = repo.config().cleanup_interval_secs;

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await; // First tick is immediate, skip it.

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("cleanup task cancelled");
                    break;
                }
                _ = interval.tick() => {
                    run_cleanup_cycle(&repo);
                }
            }
        }
    })
}

fn run_cleanup_cycle(repo: &FileContentRepository) {
    let gc_count = garbage_collect(repo);
    let evicted = evict_memory(repo);
    let deleted = delete_dead_segments(repo);

    if gc_count > 0 || evicted > 0 || deleted > 0 {
        tracing::debug!(
            gc_removed = gc_count,
            memory_evicted = evicted,
            segments_deleted = deleted,
            "cleanup cycle complete"
        );
    }
}

/// Safety net: scan for entries with ref_count == 0 and remove them.
fn garbage_collect(repo: &FileContentRepository) -> u64 {
    let mut removed = 0u64;
    let mut dead_ids = Vec::new();

    for entry in repo.index().iter() {
        if entry.value().ref_count.load(Ordering::Relaxed) == 0 {
            dead_ids.push(*entry.key());
        }
    }

    for id in dead_ids {
        // Use decrement_ref logic won't work since ref is already 0.
        // Directly remove from index.
        if let Some((_, entry)) = repo.index().remove(&id) {
            let tier = entry.tier.into_inner();
            match tier {
                super::content_file::StorageTier::Memory(data) => {
                    repo.memory_bytes
                        .fetch_sub(data.len() as u64, Ordering::Relaxed);
                }
                super::content_file::StorageTier::Disk(location) => {
                    repo.segment_cache().decrement_live(&location.segment_path);
                }
            }
            removed += 1;
        }
    }

    removed
}

/// Evict oldest-accessed memory entries to disk until below 80% of threshold.
fn evict_memory(repo: &FileContentRepository) -> u64 {
    let threshold = repo.config().memory_threshold;
    let current = repo.memory_bytes();
    if current <= threshold {
        return 0;
    }

    let target = threshold * 80 / 100;
    let mut evicted = 0u64;

    // Collect memory-tier entries sorted by last access time (oldest first)
    let mut candidates: Vec<(u64, u64)> = Vec::new(); // (resource_id, last_access_nanos)
    for entry in repo.index().iter() {
        let tier = entry.value().tier.read();
        if matches!(&*tier, super::content_file::StorageTier::Memory(_)) {
            candidates.push((
                *entry.key(),
                entry.value().last_access_nanos.load(Ordering::Relaxed),
            ));
        }
    }
    candidates.sort_by_key(|&(_, ts)| ts);

    for (resource_id, _) in candidates {
        if repo.memory_bytes() <= target {
            break;
        }
        match repo.evict_to_disk(resource_id) {
            Ok(freed) => evicted += freed,
            Err(e) => {
                tracing::warn!(resource_id, error = %e, "failed to evict to disk");
            }
        }
    }

    evicted
}

/// Delete sealed segments that have zero live blobs.
fn delete_dead_segments(repo: &FileContentRepository) -> u64 {
    let cache = repo.segment_cache();
    let mut deleted = 0u64;

    let sealed_paths = cache.sealed_paths();
    for path in sealed_paths {
        if cache.live_count(&path) == 0 {
            match cache.remove_segment(&path) {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "failed to delete dead segment");
                }
            }
        }
    }

    deleted
}
