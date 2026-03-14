//! File-backed provenance repository with segment-based append-only storage.
//!
//! Events are written to segment files. When a segment exceeds the configured
//! max size, it is finalized and a new segment is started. An in-memory index
//! provides O(1) event lookup by ID and efficient filtered search.

use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::error::{Result, RuniFiError};
use crate::repository::provenance_format::{
    self, TAG_PROVENANCE_EVENT, decode_provenance_event, encode_provenance_event,
};
use crate::repository::provenance_index::{self, SegmentIndex};
use crate::repository::provenance_repo::{
    ProvenanceConfig, ProvenanceEvent, ProvenanceQuery, ProvenanceRepository,
    ProvenanceSearchResult, ProvenanceStats,
};
use crate::repository::wal_format;

/// Configuration for the file-backed provenance repository.
#[derive(Debug, Clone)]
pub struct FileProvenanceConfig {
    /// Directory where segment files are stored.
    pub directory: PathBuf,
    /// Maximum size of a single segment file in bytes (default 100MB).
    pub max_segment_size: u64,
    /// Retention period in days (default 1).
    pub retention_days: u32,
    /// Maximum total size of all segments in bytes (default 1GB).
    pub max_total_size: u64,
}

impl Default for FileProvenanceConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("data/provenance"),
            max_segment_size: 100 * 1024 * 1024,
            retention_days: 1,
            max_total_size: 1024 * 1024 * 1024,
        }
    }
}

/// Metadata about a finalized segment file.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SegmentInfo {
    path: PathBuf,
    min_event_id: u64,
    max_event_id: u64,
    min_timestamp: u64,
    max_timestamp: u64,
    event_count: u64,
    file_size: u64,
    created_at_nanos: u64,
}

/// Active segment writer for the current segment.
struct ActiveSegmentWriter {
    path: PathBuf,
    writer: BufWriter<fs::File>,
    bytes_written: u64,
    event_count: u64,
    min_event_id: u64,
    max_event_id: u64,
    min_timestamp: u64,
    max_timestamp: u64,
    created_at_nanos: u64,
    /// Per-segment index built incrementally.
    index: SegmentIndex,
}

impl ActiveSegmentWriter {
    fn new(path: PathBuf, segment_id: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| RuniFiError::ProvenanceError {
                path: parent.display().to_string(),
                reason: format!("failed to create directory: {e}"),
            })?;
        }

        let file = fs::File::create(&path).map_err(|e| RuniFiError::ProvenanceError {
            path: path.display().to_string(),
            reason: format!("failed to create segment: {e}"),
        })?;
        let mut writer = BufWriter::new(file);

        provenance_format::write_provenance_header(&mut writer).map_err(|e| {
            RuniFiError::ProvenanceError {
                path: path.display().to_string(),
                reason: format!("failed to write header: {e}"),
            }
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Ok(Self {
            path,
            writer,
            bytes_written: 8, // header size
            event_count: 0,
            min_event_id: u64::MAX,
            max_event_id: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            created_at_nanos: now,
            index: SegmentIndex::new(segment_id),
        })
    }

    fn write_event(&mut self, event: &ProvenanceEvent) -> Result<()> {
        let file_offset = self.bytes_written;
        let payload = encode_provenance_event(event);
        wal_format::write_record(&mut self.writer, TAG_PROVENANCE_EVENT, &payload).map_err(
            |e| RuniFiError::ProvenanceError {
                path: self.path.display().to_string(),
                reason: format!("failed to write event: {e}"),
            },
        )?;

        // Update metadata.
        let record_size = 1 + 4 + payload.len() as u64 + 4; // tag + len + payload + crc
        self.bytes_written += record_size;
        self.event_count += 1;
        self.min_event_id = self.min_event_id.min(event.event_id);
        self.max_event_id = self.max_event_id.max(event.event_id);
        self.min_timestamp = self.min_timestamp.min(event.timestamp_nanos);
        self.max_timestamp = self.max_timestamp.max(event.timestamp_nanos);

        // Update per-segment index.
        self.index.add_event(event, file_offset);

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| RuniFiError::ProvenanceError {
                path: self.path.display().to_string(),
                reason: format!("flush failed: {e}"),
            })
    }

    fn to_segment_info(&self) -> SegmentInfo {
        SegmentInfo {
            path: self.path.clone(),
            min_event_id: self.min_event_id,
            max_event_id: self.max_event_id,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            event_count: self.event_count,
            file_size: self.bytes_written,
            created_at_nanos: self.created_at_nanos,
        }
    }
}

/// File-backed provenance repository with in-memory index.
pub struct FileProvenanceRepository {
    config: FileProvenanceConfig,
    /// Finalized segments (read-only).
    segments: RwLock<Vec<SegmentInfo>>,
    /// Active segment (append-only).
    active_segment: Mutex<Option<ActiveSegmentWriter>>,
    /// In-memory index: event_id → ProvenanceEvent.
    events_by_id: DashMap<u64, ProvenanceEvent>,
    /// Index: flowfile_id → vec of event_ids.
    events_by_flowfile: DashMap<u64, Vec<u64>>,
    /// Index: lineage_start_id → vec of event_ids.
    events_by_lineage: DashMap<u64, Vec<u64>>,
    /// Ordered list of event IDs for time-based queries.
    ordered_ids: RwLock<Vec<u64>>,
    /// Monotonic event ID generator.
    next_event_id: AtomicU64,
    /// Retention config for in-memory pruning compatibility.
    #[allow(dead_code)]
    retention_config: ProvenanceConfig,
}

impl FileProvenanceRepository {
    /// Create a new file-backed provenance repository.
    pub fn new(config: FileProvenanceConfig) -> Result<Self> {
        fs::create_dir_all(&config.directory).map_err(|e| RuniFiError::ProvenanceError {
            path: config.directory.display().to_string(),
            reason: format!("failed to create provenance directory: {e}"),
        })?;

        let retention_nanos = config.retention_days as u64 * 24 * 60 * 60 * 1_000_000_000;

        let repo = Self {
            config,
            segments: RwLock::new(Vec::new()),
            active_segment: Mutex::new(None),
            events_by_id: DashMap::new(),
            events_by_flowfile: DashMap::new(),
            events_by_lineage: DashMap::new(),
            ordered_ids: RwLock::new(Vec::new()),
            next_event_id: AtomicU64::new(1),
            retention_config: ProvenanceConfig {
                max_events: usize::MAX,
                max_age_nanos: retention_nanos,
            },
        };

        // Recover existing segments.
        repo.recover()?;

        Ok(repo)
    }

    /// Scan the provenance directory for existing segment files and rebuild the index.
    fn recover(&self) -> Result<()> {
        let mut segment_paths: Vec<PathBuf> = Vec::new();

        let entries =
            fs::read_dir(&self.config.directory).map_err(|e| RuniFiError::ProvenanceError {
                path: self.config.directory.display().to_string(),
                reason: format!("failed to read directory: {e}"),
            })?;

        for entry in entries {
            let entry = entry.map_err(|e| RuniFiError::ProvenanceError {
                path: self.config.directory.display().to_string(),
                reason: format!("failed to read entry: {e}"),
            })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("seg") {
                segment_paths.push(path);
            }
        }

        // Sort by filename (which includes a sequence number).
        segment_paths.sort();

        let mut max_event_id = 0u64;
        let mut segments = Vec::new();

        for path in &segment_paths {
            match self.load_segment(path) {
                Ok(info) => {
                    if info.max_event_id > max_event_id {
                        max_event_id = info.max_event_id;
                    }
                    segments.push(info);
                }
                Err(e) => {
                    warn!(
                        "Skipping corrupted provenance segment {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }

        if !segments.is_empty() {
            info!(
                "Recovered {} provenance segments with {} total events",
                segments.len(),
                self.events_by_id.len()
            );
        }

        self.next_event_id
            .store(max_event_id + 1, Ordering::Relaxed);
        *self.segments.write() = segments;

        Ok(())
    }

    /// Load a segment file and index all events within it.
    fn load_segment(&self, path: &Path) -> Result<SegmentInfo> {
        let file = fs::File::open(path).map_err(|e| RuniFiError::ProvenanceError {
            path: path.display().to_string(),
            reason: format!("failed to open: {e}"),
        })?;
        let mut reader = BufReader::new(file);

        if !provenance_format::read_provenance_header(&mut reader)? {
            return Err(RuniFiError::ProvenanceError {
                path: path.display().to_string(),
                reason: "invalid or missing header".into(),
            });
        }

        let mut min_eid = u64::MAX;
        let mut max_eid = 0u64;
        let mut min_ts = u64::MAX;
        let mut max_ts = 0u64;
        let mut count = 0u64;

        while let Some((tag, payload)) = wal_format::read_record(&mut reader)? {
            if tag != TAG_PROVENANCE_EVENT {
                warn!(
                    "Unknown tag {tag:#04x} in provenance segment {}",
                    path.display()
                );
                continue;
            }

            let event = decode_provenance_event(&payload)?;
            min_eid = min_eid.min(event.event_id);
            max_eid = max_eid.max(event.event_id);
            min_ts = min_ts.min(event.timestamp_nanos);
            max_ts = max_ts.max(event.timestamp_nanos);
            count += 1;

            self.index_event(&event);
        }

        let file_size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

        let created_at = fs::metadata(path)
            .and_then(|m| m.created())
            .or_else(|_| fs::metadata(path).and_then(|m| m.modified()))
            .map(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64
            })
            .unwrap_or(min_ts);

        Ok(SegmentInfo {
            path: path.to_path_buf(),
            min_event_id: min_eid,
            max_event_id: max_eid,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            event_count: count,
            file_size,
            created_at_nanos: created_at,
        })
    }

    /// Add an event to all in-memory indexes.
    fn index_event(&self, event: &ProvenanceEvent) {
        let eid = event.event_id;
        self.events_by_id.insert(eid, event.clone());
        self.events_by_flowfile
            .entry(event.flowfile_id)
            .or_default()
            .push(eid);
        self.events_by_lineage
            .entry(event.lineage_start_id)
            .or_default()
            .push(eid);
        self.ordered_ids.write().push(eid);
    }

    /// Ensure there's an active segment writer, creating one if needed.
    fn ensure_active_segment(&self) -> Result<()> {
        let mut active = self.active_segment.lock();
        if active.is_none() {
            let seq = self.segments.read().len();
            let filename = format!("provenance-{seq:06}.seg");
            let path = self.config.directory.join(filename);
            *active = Some(ActiveSegmentWriter::new(path, seq as u64)?);
        }
        Ok(())
    }

    /// Rotate the active segment if it exceeds the max size.
    fn maybe_rotate(&self) -> Result<()> {
        let should_rotate = {
            let active = self.active_segment.lock();
            active
                .as_ref()
                .is_some_and(|w| w.bytes_written >= self.config.max_segment_size)
        };

        if should_rotate {
            let mut active = self.active_segment.lock();
            if let Some(mut writer) = active.take() {
                writer.flush()?;
                let info = writer.to_segment_info();

                // Write the index file for this finalized segment.
                let idx_path = info.path.with_extension("idx");
                if let Err(e) = provenance_index::write_index(&idx_path, &writer.index) {
                    warn!(
                        "Failed to write index for segment {}: {e}",
                        info.path.display()
                    );
                }

                debug!(
                    "Finalized provenance segment {} ({} events, {} bytes)",
                    info.path.display(),
                    info.event_count,
                    info.file_size
                );
                self.segments.write().push(info);
            }
            // Next write will create a new segment.
        }

        Ok(())
    }

    /// Record a single event to disk and index.
    fn record_event(&self, mut event: ProvenanceEvent) {
        if event.event_id == 0 {
            event.event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        }

        if let Err(e) = self.ensure_active_segment() {
            error!("Failed to create provenance segment: {e}");
            return;
        }

        {
            let mut active = self.active_segment.lock();
            if let Some(writer) = active.as_mut()
                && let Err(e) = writer.write_event(&event)
            {
                error!("Failed to write provenance event: {e}");
                return;
            }
        }

        self.index_event(&event);

        if let Err(e) = self.maybe_rotate() {
            error!("Failed to rotate provenance segment: {e}");
        }
    }

    /// Flush the active segment to disk.
    pub fn flush(&self) -> Result<()> {
        let mut active = self.active_segment.lock();
        if let Some(writer) = active.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get total size of all segment files.
    pub fn total_size_bytes(&self) -> u64 {
        let segments = self.segments.read();
        let finalized: u64 = segments.iter().map(|s| s.file_size).sum();
        let active = self.active_segment.lock();
        let active_size = active.as_ref().map_or(0, |w| w.bytes_written);
        finalized + active_size
    }

    /// Prune old segments by time and size constraints.
    pub fn prune_segments(&self) {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let retention_nanos = self.config.retention_days as u64 * 24 * 60 * 60 * 1_000_000_000;
        let cutoff = now_nanos.saturating_sub(retention_nanos);

        let mut segments = self.segments.write();
        let mut to_remove: Vec<usize> = Vec::new();

        // Remove segments older than retention period.
        for (i, seg) in segments.iter().enumerate() {
            if seg.max_timestamp < cutoff {
                to_remove.push(i);
            }
        }

        // Also remove oldest segments if total size exceeds limit.
        let mut total_size: u64 = segments.iter().map(|s| s.file_size).sum();
        for (i, seg) in segments.iter().enumerate() {
            if total_size > self.config.max_total_size && !to_remove.contains(&i) {
                to_remove.push(i);
                total_size -= seg.file_size;
            }
        }

        to_remove.sort_unstable();
        to_remove.dedup();

        // Remove events from index and delete files.
        for &idx in to_remove.iter().rev() {
            let seg = &segments[idx];
            self.remove_segment_from_index(seg);

            if let Err(e) = fs::remove_file(&seg.path) {
                warn!(
                    "Failed to delete provenance segment {}: {e}",
                    seg.path.display()
                );
            } else {
                debug!("Pruned provenance segment {}", seg.path.display());
            }

            // Also remove the index file if it exists.
            let idx_path = seg.path.with_extension("idx");
            if idx_path.exists() {
                let _ = fs::remove_file(&idx_path);
            }

            segments.remove(idx);
        }
    }

    /// Remove all events in a segment from the in-memory index.
    fn remove_segment_from_index(&self, seg: &SegmentInfo) {
        let mut ordered = self.ordered_ids.write();
        let mut to_remove_ids = Vec::new();

        // Find all event IDs in this segment's range.
        for &eid in ordered.iter() {
            if eid >= seg.min_event_id && eid <= seg.max_event_id {
                to_remove_ids.push(eid);
            }
        }

        for &eid in &to_remove_ids {
            if let Some((_, event)) = self.events_by_id.remove(&eid) {
                if let Some(mut ids) = self.events_by_flowfile.get_mut(&event.flowfile_id) {
                    ids.retain(|&id| id != eid);
                }
                if let Some(mut ids) = self.events_by_lineage.get_mut(&event.lineage_start_id) {
                    ids.retain(|&id| id != eid);
                }
            }
        }

        ordered.retain(|id| !to_remove_ids.contains(id));
    }
}

impl ProvenanceRepository for FileProvenanceRepository {
    fn record(&self, event: ProvenanceEvent) {
        self.record_event(event);
    }

    fn record_batch(&self, events: Vec<ProvenanceEvent>) {
        for event in events {
            self.record_event(event);
        }
        // Flush after batch for durability.
        if let Err(e) = self.flush() {
            error!("Failed to flush provenance after batch: {e}");
        }
    }

    fn search(&self, query: &ProvenanceQuery) -> ProvenanceSearchResult {
        let ordered = self.ordered_ids.read();
        let mut matching: Vec<ProvenanceEvent> = Vec::new();

        for &eid in ordered.iter().rev() {
            if let Some(event) = self.events_by_id.get(&eid) {
                let event = event.value();

                if let Some(ffid) = query.flowfile_id
                    && event.flowfile_id != ffid
                {
                    continue;
                }
                if let Some(ref proc_name) = query.processor_name
                    && event.processor_name != *proc_name
                {
                    continue;
                }
                if let Some(et) = query.event_type
                    && event.event_type != et
                {
                    continue;
                }
                if let Some(start) = query.start_time_nanos
                    && event.timestamp_nanos < start
                {
                    continue;
                }
                if let Some(end) = query.end_time_nanos
                    && event.timestamp_nanos > end
                {
                    continue;
                }
                if let Some(ref pt) = query.processor_type
                    && event.processor_type != *pt
                {
                    continue;
                }
                if let Some(min) = query.min_size
                    && event.content_size < min
                {
                    continue;
                }
                if let Some(max) = query.max_size
                    && event.content_size > max
                {
                    continue;
                }

                matching.push(event.clone());
            }
        }

        let total_count = matching.len();
        let max_results = if query.max_results == 0 {
            100
        } else {
            query.max_results
        };
        let events: Vec<ProvenanceEvent> = matching
            .into_iter()
            .skip(query.offset)
            .take(max_results)
            .collect();

        ProvenanceSearchResult {
            events,
            total_count,
        }
    }

    fn get_lineage(&self, flowfile_id: u64) -> Vec<ProvenanceEvent> {
        let lineage_start_id = if let Some(ids) = self.events_by_flowfile.get(&flowfile_id) {
            ids.iter()
                .filter_map(|&eid| self.events_by_id.get(&eid))
                .map(|e| e.lineage_start_id)
                .next()
                .unwrap_or(flowfile_id)
        } else {
            flowfile_id
        };

        let mut events: Vec<ProvenanceEvent> =
            if let Some(ids) = self.events_by_lineage.get(&lineage_start_id) {
                ids.iter()
                    .filter_map(|&eid| self.events_by_id.get(&eid).map(|e| e.value().clone()))
                    .collect()
            } else {
                Vec::new()
            };

        events.sort_by_key(|e| e.timestamp_nanos);
        events
    }

    fn get_event(&self, event_id: u64) -> Option<ProvenanceEvent> {
        self.events_by_id.get(&event_id).map(|e| e.value().clone())
    }

    fn event_count(&self) -> usize {
        self.events_by_id.len()
    }

    fn prune(&self) {
        self.prune_segments();
    }

    fn stats(&self) -> ProvenanceStats {
        let ordered = self.ordered_ids.read();
        let oldest = ordered
            .first()
            .and_then(|id| self.events_by_id.get(id))
            .map(|e| e.timestamp_nanos);
        let newest = ordered
            .last()
            .and_then(|id| self.events_by_id.get(id))
            .map(|e| e.timestamp_nanos);
        ProvenanceStats {
            event_count: self.events_by_id.len(),
            oldest_timestamp_nanos: oldest,
            newest_timestamp_nanos: newest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::provenance_repo::ProvenanceEventType;

    fn now_nanos() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn make_event(
        flowfile_id: u64,
        event_type: ProvenanceEventType,
        processor: &str,
        lineage_id: u64,
    ) -> ProvenanceEvent {
        ProvenanceEvent {
            event_id: 0,
            flowfile_id,
            event_type,
            processor_name: processor.to_string(),
            processor_type: "TestProcessor".to_string(),
            timestamp_nanos: now_nanos(),
            attributes: vec![("filename".to_string(), "test.txt".to_string())],
            content_size: 1024,
            lineage_start_id: lineage_id,
            relationship: None,
            source_flowfile_id: None,
            details: String::new(),
            parent_flowfile_ids: Vec::new(),
            child_flowfile_ids: Vec::new(),
            transit_uri: None,
            content_claim_id: None,
            previous_attributes: Vec::new(),
        }
    }

    #[test]
    fn record_and_retrieve() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Send, "gen", 2));

        assert_eq!(repo.event_count(), 2);

        let event = repo.get_event(1).unwrap();
        assert_eq!(event.flowfile_id, 1);
        assert_eq!(event.event_type, ProvenanceEventType::Create);
    }

    #[test]
    fn search_with_filters() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Send, "gen", 2));
        repo.record(make_event(3, ProvenanceEventType::Create, "get-file", 3));

        let result = repo.search(&ProvenanceQuery {
            event_type: Some(ProvenanceEventType::Create),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 2);

        let result = repo.search(&ProvenanceQuery {
            processor_name: Some("gen".to_string()),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 2);
    }

    #[test]
    fn lineage_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(1, ProvenanceEventType::Send, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Clone, "route", 1));

        let lineage = repo.get_lineage(1);
        assert_eq!(lineage.len(), 3);

        let lineage2 = repo.get_lineage(2);
        assert_eq!(lineage2.len(), 3);
    }

    #[test]
    fn persistence_across_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            max_segment_size: 1024, // Small to force segment finalization.
            ..Default::default()
        };

        // Write events.
        {
            let repo = FileProvenanceRepository::new(config.clone()).unwrap();
            for i in 1..=20 {
                repo.record(make_event(i, ProvenanceEventType::Create, "gen", i));
            }
            repo.flush().unwrap();
        }

        // Restart and verify.
        {
            let repo = FileProvenanceRepository::new(config).unwrap();
            assert!(repo.event_count() >= 20);

            let event = repo.get_event(1).unwrap();
            assert_eq!(event.flowfile_id, 1);
        }
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            max_segment_size: 512, // Very small to trigger rotation.
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        for i in 1..=50 {
            repo.record(make_event(i, ProvenanceEventType::Create, "gen", i));
        }

        let segments = repo.segments.read();
        assert!(segments.len() >= 1, "Expected at least 1 finalized segment");
    }

    #[test]
    fn batch_record() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        let events = vec![
            make_event(1, ProvenanceEventType::Create, "gen", 1),
            make_event(2, ProvenanceEventType::Send, "gen", 2),
            make_event(3, ProvenanceEventType::Receive, "log", 3),
        ];
        repo.record_batch(events);
        assert_eq!(repo.event_count(), 3);
    }

    #[test]
    fn stats_report() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        repo.record(make_event(1, ProvenanceEventType::Create, "gen", 1));
        repo.record(make_event(2, ProvenanceEventType::Send, "gen", 2));

        let stats = repo.stats();
        assert_eq!(stats.event_count, 2);
        assert!(stats.oldest_timestamp_nanos.is_some());
        assert!(stats.newest_timestamp_nanos.is_some());
    }

    #[test]
    fn search_by_size_range() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = FileProvenanceConfig {
            directory: dir.path().to_path_buf(),
            ..Default::default()
        };
        let repo = FileProvenanceRepository::new(config).unwrap();

        let mut small = make_event(1, ProvenanceEventType::Create, "gen", 1);
        small.content_size = 100;
        let mut large = make_event(2, ProvenanceEventType::Create, "gen", 2);
        large.content_size = 10000;

        repo.record(small);
        repo.record(large);

        let result = repo.search(&ProvenanceQuery {
            min_size: Some(500),
            max_results: 10,
            ..Default::default()
        });
        assert_eq!(result.total_count, 1);
        assert_eq!(result.events[0].content_size, 10000);
    }
}
