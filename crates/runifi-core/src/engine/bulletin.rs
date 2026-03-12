use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::Serialize;

/// Severity level for a bulletin entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BulletinSeverity {
    Warn,
    Error,
}

impl BulletinSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// A single bulletin entry — a captured processor warning or error.
#[derive(Debug, Clone, Serialize)]
pub struct Bulletin {
    /// Monotonic bulletin ID (for ordering / deduplication).
    pub id: u64,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Severity level.
    pub severity: BulletinSeverity,
    /// Name of the processor that generated this bulletin.
    pub processor_name: String,
    /// The bulletin message.
    pub message: String,
}

/// A bounded ring buffer for bulletins.
///
/// Stores the most recent `capacity` entries, evicting the oldest when full.
struct BulletinRing {
    entries: Vec<Option<Bulletin>>,
    capacity: usize,
    write_pos: usize,
    count: usize,
}

impl BulletinRing {
    fn new(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        entries.resize_with(capacity, || None);
        Self {
            entries,
            capacity,
            write_pos: 0,
            count: 0,
        }
    }

    fn push(&mut self, bulletin: Bulletin) {
        self.entries[self.write_pos] = Some(bulletin);
        self.write_pos = (self.write_pos + 1) % self.capacity;
        if self.count < self.capacity {
            self.count += 1;
        }
    }

    /// Return all stored bulletins in chronological order (oldest first).
    fn iter(&self) -> Vec<&Bulletin> {
        if self.count == 0 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(self.count);
        // If the ring is full, start reading from write_pos (oldest entry).
        // If not full, start from index 0.
        let start = if self.count == self.capacity {
            self.write_pos
        } else {
            0
        };
        for i in 0..self.count {
            let idx = (start + i) % self.capacity;
            if let Some(b) = &self.entries[idx] {
                result.push(b);
            }
        }
        result
    }

    /// Return the most recent bulletin, if any.
    fn latest(&self) -> Option<&Bulletin> {
        if self.count == 0 {
            return None;
        }
        let idx = if self.write_pos == 0 {
            self.capacity - 1
        } else {
            self.write_pos - 1
        };
        self.entries[idx].as_ref()
    }
}

/// Thread-safe bulletin board that collects per-processor warnings and errors.
///
/// Shared via `Arc` between the engine (writers) and the API (readers).
pub struct BulletinBoard {
    /// Per-processor ring buffers.
    rings: RwLock<HashMap<String, BulletinRing>>,
    /// Maximum bulletins per processor.
    capacity_per_processor: usize,
    /// Monotonic ID counter.
    next_id: std::sync::atomic::AtomicU64,
}

impl BulletinBoard {
    /// Create a new bulletin board with the given per-processor capacity.
    pub fn new(capacity_per_processor: usize) -> Self {
        Self {
            rings: RwLock::new(HashMap::new()),
            capacity_per_processor,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Record a bulletin for a processor.
    pub fn add(
        &self,
        processor_name: &str,
        severity: BulletinSeverity,
        message: String,
    ) {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let bulletin = Bulletin {
            id,
            timestamp_ms,
            severity,
            processor_name: processor_name.to_string(),
            message,
        };

        let mut rings = self.rings.write();
        let ring = rings
            .entry(processor_name.to_string())
            .or_insert_with(|| BulletinRing::new(self.capacity_per_processor));
        ring.push(bulletin);
    }

    /// Get all bulletins across all processors (newest last), optionally filtered.
    pub fn get_all(
        &self,
        filter_processor: Option<&str>,
        filter_severity: Option<BulletinSeverity>,
    ) -> Vec<Bulletin> {
        let rings = self.rings.read();
        let mut all: Vec<Bulletin> = Vec::new();

        for (name, ring) in rings.iter() {
            if let Some(proc_filter) = filter_processor
                && name != proc_filter
            {
                continue;
            }
            for b in ring.iter() {
                if let Some(sev) = filter_severity
                    && b.severity != sev
                {
                    continue;
                }
                all.push(b.clone());
            }
        }

        // Sort by id (chronological order).
        all.sort_by_key(|b| b.id);
        all
    }

    /// Get bulletins for a specific processor.
    pub fn get_for_processor(
        &self,
        processor_name: &str,
        filter_severity: Option<BulletinSeverity>,
    ) -> Vec<Bulletin> {
        self.get_all(Some(processor_name), filter_severity)
    }

    /// Get the latest bulletin for each processor (for SSE summary).
    pub fn latest_per_processor(&self) -> HashMap<String, Bulletin> {
        let rings = self.rings.read();
        let mut result = HashMap::new();
        for (name, ring) in rings.iter() {
            if let Some(b) = ring.latest() {
                result.insert(name.clone(), b.clone());
            }
        }
        result
    }

    /// Create a shared handle to this bulletin board.
    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

impl Default for BulletinBoard {
    fn default() -> Self {
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulletin_board_basic_operations() {
        let board = BulletinBoard::new(3);

        board.add("proc1", BulletinSeverity::Warn, "warning 1".into());
        board.add("proc1", BulletinSeverity::Error, "error 1".into());
        board.add("proc2", BulletinSeverity::Warn, "warning 2".into());

        let all = board.get_all(None, None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].message, "warning 1");
        assert_eq!(all[1].message, "error 1");
        assert_eq!(all[2].message, "warning 2");
    }

    #[test]
    fn bulletin_board_filter_by_processor() {
        let board = BulletinBoard::new(10);

        board.add("proc1", BulletinSeverity::Warn, "w1".into());
        board.add("proc2", BulletinSeverity::Error, "e2".into());
        board.add("proc1", BulletinSeverity::Error, "e1".into());

        let filtered = board.get_for_processor("proc1", None);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|b| b.processor_name == "proc1"));
    }

    #[test]
    fn bulletin_board_filter_by_severity() {
        let board = BulletinBoard::new(10);

        board.add("proc1", BulletinSeverity::Warn, "w1".into());
        board.add("proc1", BulletinSeverity::Error, "e1".into());
        board.add("proc1", BulletinSeverity::Warn, "w2".into());

        let warnings = board.get_all(None, Some(BulletinSeverity::Warn));
        assert_eq!(warnings.len(), 2);

        let errors = board.get_all(None, Some(BulletinSeverity::Error));
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn bulletin_ring_evicts_oldest() {
        let board = BulletinBoard::new(2);

        board.add("proc1", BulletinSeverity::Warn, "first".into());
        board.add("proc1", BulletinSeverity::Warn, "second".into());
        board.add("proc1", BulletinSeverity::Warn, "third".into());

        let all = board.get_for_processor("proc1", None);
        assert_eq!(all.len(), 2);
        // "first" was evicted; we should have "second" and "third"
        assert_eq!(all[0].message, "second");
        assert_eq!(all[1].message, "third");
    }

    #[test]
    fn latest_per_processor() {
        let board = BulletinBoard::new(10);

        board.add("proc1", BulletinSeverity::Warn, "old".into());
        board.add("proc1", BulletinSeverity::Error, "latest-p1".into());
        board.add("proc2", BulletinSeverity::Warn, "latest-p2".into());

        let latest = board.latest_per_processor();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest["proc1"].message, "latest-p1");
        assert_eq!(latest["proc2"].message, "latest-p2");
    }
}
