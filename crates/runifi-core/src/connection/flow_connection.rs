use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam::channel::{self, Receiver, Sender, TryRecvError, TrySendError};
use runifi_plugin_api::FlowFile;
use tokio::sync::Notify;

use super::back_pressure::BackPressureConfig;

/// A connection between two processors in the flow graph.
///
/// Uses `crossbeam::channel::bounded` for lock-free, ~100ns FlowFile transfer.
/// Tracks queue count and byte size with atomics for back-pressure enforcement.
/// Uses `tokio::sync::Notify` for event-driven wakeup (no busy polling).
pub struct FlowConnection {
    pub id: String,
    sender: Sender<FlowFile>,
    receiver: Receiver<FlowFile>,
    config: BackPressureConfig,
    count: AtomicUsize,
    bytes: AtomicU64,
    notify: Arc<Notify>,
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
        }
    }

    /// Try to send a FlowFile into the connection.
    /// Returns `Err(flowfile)` if the queue is full (back-pressure).
    pub fn try_send(&self, flowfile: FlowFile) -> Result<(), FlowFile> {
        let size = flowfile.size;
        match self.sender.try_send(flowfile) {
            Ok(()) => {
                self.count.fetch_add(1, Ordering::Relaxed);
                self.bytes.fetch_add(size, Ordering::Relaxed);
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

    /// Get a handle to the notify for async wakeup.
    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
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
}
