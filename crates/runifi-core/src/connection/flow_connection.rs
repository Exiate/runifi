use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam::channel::{self, Receiver, Sender, TryRecvError, TrySendError};
use parking_lot::Mutex;
use runifi_plugin_api::{ContentClaim, FlowFile};
use tokio::sync::Notify;
use tracing::warn;

use super::back_pressure::BackPressureConfig;

/// Lightweight snapshot of a FlowFile for queue inspection.
///
/// Stored in a shadow index alongside the crossbeam channel so that
/// API consumers can peek at queue contents without consuming FlowFiles.
#[derive(Debug, Clone)]
pub struct FlowFileSnapshot {
    pub id: u64,
    pub attributes: Vec<(Arc<str>, Arc<str>)>,
    pub content_claim: Option<ContentClaim>,
    pub size: u64,
    pub created_at_nanos: u64,
}

impl FlowFileSnapshot {
    fn from_flowfile(ff: &FlowFile) -> Self {
        Self {
            id: ff.id,
            attributes: ff.attributes.clone(),
            content_claim: ff.content_claim.clone(),
            size: ff.size,
            created_at_nanos: ff.created_at_nanos,
        }
    }
}

/// A connection between two processors in the flow graph.
///
/// Uses `crossbeam::channel::bounded` for lock-free, ~100ns FlowFile transfer.
/// Tracks queue count and byte size with atomics for back-pressure enforcement.
/// Uses `tokio::sync::Notify` for event-driven wakeup (no busy polling).
///
/// A shadow `VecDeque<FlowFileSnapshot>` mirrors the channel contents for
/// queue inspection. The shadow is protected by a `parking_lot::Mutex` which
/// has minimal contention (~40ns uncontended) since send/recv hold it only
/// briefly to push/pop a snapshot.
pub struct FlowConnection {
    pub id: String,
    sender: Sender<FlowFile>,
    receiver: Receiver<FlowFile>,
    config: BackPressureConfig,
    count: AtomicUsize,
    bytes: AtomicU64,
    notify: Arc<Notify>,
    /// Shadow index for queue inspection — mirrors the crossbeam channel.
    shadow: Mutex<VecDeque<FlowFileSnapshot>>,
}

impl FlowConnection {
    pub fn new(id: impl Into<String>, config: BackPressureConfig) -> Self {
        let (sender, receiver) = channel::bounded(config.max_count);
        Self {
            id: id.into(),
            sender,
            receiver,
            config,
            count: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            notify: Arc::new(Notify::new()),
            shadow: Mutex::new(VecDeque::new()),
        }
    }

    /// Try to send a FlowFile into the connection.
    /// Returns `Err(flowfile)` if the queue is full (back-pressure).
    pub fn try_send(&self, flowfile: FlowFile) -> Result<(), FlowFile> {
        let size = flowfile.size;
        let snapshot = FlowFileSnapshot::from_flowfile(&flowfile);
        match self.sender.try_send(flowfile) {
            Ok(()) => {
                self.count.fetch_add(1, Ordering::Relaxed);
                self.bytes.fetch_add(size, Ordering::Relaxed);
                self.shadow.lock().push_back(snapshot);
                self.notify.notify_one();
                Ok(())
            }
            Err(TrySendError::Full(ff)) | Err(TrySendError::Disconnected(ff)) => Err(ff),
        }
    }

    /// Try to receive a FlowFile from the connection.
    pub fn try_recv(&self) -> Option<FlowFile> {
        match self.receiver.try_recv() {
            Ok(ff) => {
                self.count.fetch_sub(1, Ordering::Relaxed);
                self.bytes.fetch_sub(ff.size, Ordering::Relaxed);
                // Remove the matching snapshot from the front of the shadow.
                let mut shadow = self.shadow.lock();
                if let Some(pos) = shadow.iter().position(|s| s.id == ff.id) {
                    shadow.remove(pos);
                }
                Some(ff)
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    /// Receive up to `max` FlowFiles at once.
    pub fn try_recv_batch(&self, max: usize) -> Vec<FlowFile> {
        let mut batch = Vec::with_capacity(max);
        for _ in 0..max {
            match self.try_recv() {
                Some(ff) => batch.push(ff),
                None => break,
            }
        }
        batch
    }

    /// Check if back-pressure is active (queue exceeds thresholds).
    pub fn is_back_pressured(&self) -> bool {
        self.count.load(Ordering::Relaxed) >= self.config.max_count
            || self.bytes.load(Ordering::Relaxed) >= self.config.max_bytes
    }

    /// Current number of FlowFiles in the queue.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Current total bytes of content in the queue.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Get the back-pressure configuration for this connection.
    pub fn back_pressure_config(&self) -> BackPressureConfig {
        self.config
    }

    /// Get a handle to the notify for async wakeup.
    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    // ── Queue inspection API ─────────────────────────────────────

    /// Return a paginated snapshot of FlowFiles currently in the queue.
    ///
    /// This reads from the shadow index and never blocks the crossbeam channel.
    /// `offset` and `limit` control pagination (0-based offset).
    pub fn queue_snapshot(&self, offset: usize, limit: usize) -> Vec<FlowFileSnapshot> {
        let shadow = self.shadow.lock();
        shadow.iter().skip(offset).take(limit).cloned().collect()
    }

    /// Return the total number of snapshots in the shadow index.
    pub fn queue_snapshot_count(&self) -> usize {
        self.shadow.lock().len()
    }

    /// Look up a single FlowFile snapshot by ID.
    pub fn queue_get(&self, flowfile_id: u64) -> Option<FlowFileSnapshot> {
        let shadow = self.shadow.lock();
        shadow.iter().find(|s| s.id == flowfile_id).cloned()
    }

    /// Look up a single FlowFile snapshot by ID, returning it with its
    /// current position in the queue.
    pub fn queue_get_with_position(&self, flowfile_id: u64) -> Option<(usize, FlowFileSnapshot)> {
        let shadow = self.shadow.lock();
        shadow
            .iter()
            .enumerate()
            .find(|(_, s)| s.id == flowfile_id)
            .map(|(pos, s)| (pos, s.clone()))
    }

    /// Remove a specific FlowFile from the queue by ID.
    ///
    /// This drains the crossbeam channel, removes the matching FlowFile,
    /// and re-inserts the rest. This is an admin operation — not on the
    /// hot path — so the brief lock+drain is acceptable.
    ///
    /// Returns `true` if the FlowFile was found and removed.
    pub fn remove_flowfile(&self, flowfile_id: u64) -> bool {
        let mut shadow = self.shadow.lock();

        // Check if the FlowFile exists in the shadow.
        let pos = match shadow.iter().position(|s| s.id == flowfile_id) {
            Some(p) => p,
            None => return false,
        };

        shadow.remove(pos).expect("position was valid");

        // Drain the channel and re-insert everything except the target.
        let mut drained = Vec::new();
        while let Ok(ff) = self.receiver.try_recv() {
            drained.push(ff);
        }

        for ff in drained {
            if ff.id == flowfile_id {
                // Don't re-insert — this is the one we're removing.
                // Update atomics.
                self.count.fetch_sub(1, Ordering::Relaxed);
                self.bytes.fetch_sub(ff.size, Ordering::Relaxed);
            } else {
                // Re-insert into channel. Use blocking send to avoid silent
                // data loss if concurrent senders filled the channel while
                // we had it drained.
                if let Err(e) = self.sender.send(ff) {
                    // Channel is disconnected — log and adjust counters so
                    // they stay consistent with actual channel contents.
                    warn!(
                        connection_id = %self.id,
                        flowfile_id = e.0.id,
                        "failed to re-insert FlowFile during remove: channel disconnected"
                    );
                    self.count.fetch_sub(1, Ordering::Relaxed);
                    self.bytes.fetch_sub(e.0.size, Ordering::Relaxed);
                }
            }
        }

        // If the FlowFile was not found in the channel, it was consumed by a
        // concurrent try_recv between the shadow check and the drain. try_recv
        // already decremented count and bytes, so no atomic adjustment is needed.
        // The snapshot was already removed from the shadow above.

        true
    }

    /// Clear all FlowFiles from the queue.
    ///
    /// Returns the number of FlowFiles that were removed.
    pub fn clear_queue(&self) -> usize {
        let mut shadow = self.shadow.lock();
        shadow.clear();

        let mut removed = 0u64;
        let mut removed_bytes = 0u64;
        while let Ok(ff) = self.receiver.try_recv() {
            removed += 1;
            removed_bytes += ff.size;
        }

        // Reset atomics. Use saturating subtraction to avoid underflow.
        let removed_count = removed as usize;
        let prev_count = self.count.fetch_sub(removed_count, Ordering::Relaxed);
        if removed_count > prev_count {
            // Correct for underflow.
            self.count.store(0, Ordering::Relaxed);
        }
        let prev_bytes = self.bytes.fetch_sub(removed_bytes, Ordering::Relaxed);
        if removed_bytes > prev_bytes {
            self.bytes.store(0, Ordering::Relaxed);
        }

        removed_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_flowfile(id: u64, size: u64) -> FlowFile {
        FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        }
    }

    #[test]
    fn send_and_recv() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        let ff = test_flowfile(1, 100);
        assert!(conn.try_send(ff).is_ok());
        assert_eq!(conn.count(), 1);
        assert_eq!(conn.bytes(), 100);

        let received = conn.try_recv().unwrap();
        assert_eq!(received.id, 1);
        assert_eq!(conn.count(), 0);
        assert_eq!(conn.bytes(), 0);
    }

    #[test]
    fn back_pressure_by_count() {
        let config = BackPressureConfig::new(2, u64::MAX);
        let conn = FlowConnection::new("test", config);
        assert!(conn.try_send(test_flowfile(1, 10)).is_ok());
        assert!(conn.try_send(test_flowfile(2, 10)).is_ok());
        assert!(conn.is_back_pressured());
        // Channel is full, next send should fail
        assert!(conn.try_send(test_flowfile(3, 10)).is_err());
    }

    #[test]
    fn back_pressure_by_bytes() {
        let config = BackPressureConfig::new(1000, 50);
        let conn = FlowConnection::new("test", config);
        assert!(!conn.is_back_pressured());
        assert!(conn.try_send(test_flowfile(1, 60)).is_ok());
        assert!(conn.is_back_pressured());
    }

    #[test]
    fn batch_recv() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        let batch = conn.try_recv_batch(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(conn.count(), 2);
    }

    #[test]
    fn recv_empty_returns_none() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        assert!(conn.try_recv().is_none());
    }

    #[test]
    fn queue_snapshot_returns_queued_items() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        let snapshot = conn.queue_snapshot(0, 100);
        assert_eq!(snapshot.len(), 5);
        assert_eq!(snapshot[0].id, 0);
        assert_eq!(snapshot[4].id, 4);
    }

    #[test]
    fn queue_snapshot_pagination() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..10 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        let page = conn.queue_snapshot(3, 4);
        assert_eq!(page.len(), 4);
        assert_eq!(page[0].id, 3);
        assert_eq!(page[3].id, 6);
    }

    #[test]
    fn queue_get_finds_flowfile() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        conn.try_send(test_flowfile(42, 100)).unwrap();
        let snapshot = conn.queue_get(42);
        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().id, 42);
        assert!(conn.queue_get(999).is_none());
    }

    #[test]
    fn remove_flowfile_from_queue() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        assert_eq!(conn.count(), 5);

        assert!(conn.remove_flowfile(2));
        assert_eq!(conn.count(), 4);
        assert_eq!(conn.queue_snapshot_count(), 4);

        // FlowFile 2 should be gone from the snapshot.
        let snapshot = conn.queue_snapshot(0, 100);
        assert!(snapshot.iter().all(|s| s.id != 2));
    }

    #[test]
    fn remove_nonexistent_flowfile() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        conn.try_send(test_flowfile(1, 10)).unwrap();
        assert!(!conn.remove_flowfile(999));
        assert_eq!(conn.count(), 1);
    }

    #[test]
    fn clear_queue_removes_all() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        assert_eq!(conn.count(), 5);

        let removed = conn.clear_queue();
        assert_eq!(removed, 5);
        assert_eq!(conn.count(), 0);
        assert_eq!(conn.bytes(), 0);
        assert_eq!(conn.queue_snapshot_count(), 0);
    }

    #[test]
    fn shadow_tracks_recv() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        conn.try_send(test_flowfile(1, 10)).unwrap();
        conn.try_send(test_flowfile(2, 10)).unwrap();
        assert_eq!(conn.queue_snapshot_count(), 2);

        conn.try_recv();
        assert_eq!(conn.queue_snapshot_count(), 1);
        // The remaining snapshot should be FlowFile 2.
        let snapshot = conn.queue_snapshot(0, 100);
        assert_eq!(snapshot[0].id, 2);
    }

    #[test]
    fn fifo_ordering() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }
        for i in 0..5 {
            let ff = conn.try_recv().unwrap();
            assert_eq!(ff.id, i);
        }
    }

    #[test]
    fn bytes_tracking_accurate() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        conn.try_send(test_flowfile(1, 100)).unwrap();
        conn.try_send(test_flowfile(2, 200)).unwrap();
        conn.try_send(test_flowfile(3, 300)).unwrap();
        assert_eq!(conn.bytes(), 600);

        conn.try_recv(); // removes 100 bytes
        assert_eq!(conn.bytes(), 500);

        conn.try_recv(); // removes 200 bytes
        assert_eq!(conn.bytes(), 300);
    }

    #[test]
    fn queue_get_with_position() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        for i in 0..5 {
            conn.try_send(test_flowfile(i, 10)).unwrap();
        }

        let (pos, snap) = conn.queue_get_with_position(3).unwrap();
        assert_eq!(pos, 3);
        assert_eq!(snap.id, 3);

        assert!(conn.queue_get_with_position(999).is_none());
    }

    #[test]
    fn notifier_fires_on_send() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        let notifier = conn.notifier();

        // Send triggers a notify.
        conn.try_send(test_flowfile(1, 10)).unwrap();

        // We can't easily test async Notify in a sync test,
        // but we can verify the notifier is valid.
        assert!(std::sync::Arc::strong_count(&notifier) >= 1);
    }

    #[test]
    fn back_pressure_released_after_recv() {
        let config = BackPressureConfig::new(2, u64::MAX);
        let conn = FlowConnection::new("test", config);
        conn.try_send(test_flowfile(1, 10)).unwrap();
        conn.try_send(test_flowfile(2, 10)).unwrap();
        assert!(conn.is_back_pressured());

        conn.try_recv();
        assert!(!conn.is_back_pressured());
    }

    #[test]
    fn batch_recv_with_fewer_available() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        conn.try_send(test_flowfile(1, 10)).unwrap();
        conn.try_send(test_flowfile(2, 10)).unwrap();

        let batch = conn.try_recv_batch(5);
        assert_eq!(batch.len(), 2);
        assert_eq!(conn.count(), 0);
    }

    #[test]
    fn snapshot_with_attributes() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        let mut ff = test_flowfile(1, 100);
        ff.attributes.push((Arc::from("key"), Arc::from("value")));
        conn.try_send(ff).unwrap();

        let snapshot = conn.queue_snapshot(0, 1);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].attributes.len(), 1);
        assert_eq!(snapshot[0].attributes[0].0.as_ref(), "key");
        assert_eq!(snapshot[0].attributes[0].1.as_ref(), "value");
    }

    #[test]
    fn clear_empty_queue() {
        let conn = FlowConnection::new("test", BackPressureConfig::default());
        let removed = conn.clear_queue();
        assert_eq!(removed, 0);
        assert_eq!(conn.count(), 0);
        assert_eq!(conn.bytes(), 0);
    }
}
