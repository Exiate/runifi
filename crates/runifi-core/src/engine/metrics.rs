use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

/// Number of one-second buckets in the rolling window (5 minutes).
const WINDOW_SECS: usize = 300;

/// A single one-second bucket of delta counts.
#[derive(Debug, Clone, Copy, Default)]
struct Bucket {
    flowfiles_in: u64,
    flowfiles_out: u64,
    bytes_in: u64,
    bytes_out: u64,
}

/// Ring buffer of `WINDOW_SECS` one-second buckets for rolling metric aggregation.
///
/// Completely stack-allocated — no heap allocations per tick.
struct RollingWindow {
    buckets: [Bucket; WINDOW_SECS],
    current_index: usize,
    last_tick_secs: u64,
    /// Previous atomic values for computing deltas.
    prev_flowfiles_in: u64,
    prev_flowfiles_out: u64,
    prev_bytes_in: u64,
    prev_bytes_out: u64,
}

impl RollingWindow {
    fn new() -> Self {
        Self {
            buckets: [Bucket::default(); WINDOW_SECS],
            current_index: 0,
            last_tick_secs: 0,
            prev_flowfiles_in: 0,
            prev_flowfiles_out: 0,
            prev_bytes_in: 0,
            prev_bytes_out: 0,
        }
    }

    /// Record a tick: snapshot current atomic values, compute deltas, store in ring buffer.
    ///
    /// `now_secs` is the current epoch time in seconds. If multiple seconds have elapsed
    /// since the last tick, the missed buckets are zeroed out.
    fn record(
        &mut self,
        now_secs: u64,
        flowfiles_in: u64,
        flowfiles_out: u64,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        if self.last_tick_secs == 0 {
            // First tick — initialize previous values, no delta to record.
            self.prev_flowfiles_in = flowfiles_in;
            self.prev_flowfiles_out = flowfiles_out;
            self.prev_bytes_in = bytes_in;
            self.prev_bytes_out = bytes_out;
            self.last_tick_secs = now_secs;
            return;
        }

        let elapsed = now_secs.saturating_sub(self.last_tick_secs);
        if elapsed == 0 {
            // Same second — merge delta into the current bucket.
            let delta = Bucket {
                flowfiles_in: flowfiles_in.saturating_sub(self.prev_flowfiles_in),
                flowfiles_out: flowfiles_out.saturating_sub(self.prev_flowfiles_out),
                bytes_in: bytes_in.saturating_sub(self.prev_bytes_in),
                bytes_out: bytes_out.saturating_sub(self.prev_bytes_out),
            };
            let b = &mut self.buckets[self.current_index];
            b.flowfiles_in += delta.flowfiles_in;
            b.flowfiles_out += delta.flowfiles_out;
            b.bytes_in += delta.bytes_in;
            b.bytes_out += delta.bytes_out;
        } else {
            // Advance the ring buffer. Zero out any skipped buckets.
            let gaps = elapsed.min(WINDOW_SECS as u64) as usize;
            let delta = Bucket {
                flowfiles_in: flowfiles_in.saturating_sub(self.prev_flowfiles_in),
                flowfiles_out: flowfiles_out.saturating_sub(self.prev_flowfiles_out),
                bytes_in: bytes_in.saturating_sub(self.prev_bytes_in),
                bytes_out: bytes_out.saturating_sub(self.prev_bytes_out),
            };

            // Zero out the gap buckets (if we missed ticks).
            for i in 1..gaps {
                let idx = (self.current_index + i) % WINDOW_SECS;
                self.buckets[idx] = Bucket::default();
            }

            // Write the new delta into the latest bucket.
            let new_index = (self.current_index + gaps) % WINDOW_SECS;
            self.buckets[new_index] = delta;
            self.current_index = new_index;
        }

        self.prev_flowfiles_in = flowfiles_in;
        self.prev_flowfiles_out = flowfiles_out;
        self.prev_bytes_in = bytes_in;
        self.prev_bytes_out = bytes_out;
        self.last_tick_secs = now_secs;
    }

    /// Sum all buckets in the window and compute per-second rates.
    fn snapshot(&self) -> RollingSnapshot {
        let mut total = Bucket::default();
        for b in &self.buckets {
            total.flowfiles_in += b.flowfiles_in;
            total.flowfiles_out += b.flowfiles_out;
            total.bytes_in += b.bytes_in;
            total.bytes_out += b.bytes_out;
        }
        let secs = WINDOW_SECS as f64;
        RollingSnapshot {
            flowfiles_in_5m: total.flowfiles_in,
            flowfiles_out_5m: total.flowfiles_out,
            bytes_in_5m: total.bytes_in,
            bytes_out_5m: total.bytes_out,
            flowfiles_in_rate: total.flowfiles_in as f64 / secs,
            flowfiles_out_rate: total.flowfiles_out as f64 / secs,
            bytes_in_rate: total.bytes_in as f64 / secs,
            bytes_out_rate: total.bytes_out as f64 / secs,
        }
    }
}

/// Rolling 5-minute aggregated snapshot.
#[derive(Debug, Clone, Copy)]
pub struct RollingSnapshot {
    /// Total FlowFiles received in the last 5 minutes.
    pub flowfiles_in_5m: u64,
    /// Total FlowFiles sent in the last 5 minutes.
    pub flowfiles_out_5m: u64,
    /// Total bytes received in the last 5 minutes.
    pub bytes_in_5m: u64,
    /// Total bytes sent in the last 5 minutes.
    pub bytes_out_5m: u64,
    /// FlowFiles received per second (5-minute average).
    pub flowfiles_in_rate: f64,
    /// FlowFiles sent per second (5-minute average).
    pub flowfiles_out_rate: f64,
    /// Bytes received per second (5-minute average).
    pub bytes_in_rate: f64,
    /// Bytes sent per second (5-minute average).
    pub bytes_out_rate: f64,
}

/// Shared processor metrics — atomics for zero-contention reads from the API.
///
/// The processor loop writes to these atomics; the API reads them.
/// All operations use `Relaxed` ordering (no cross-field consistency needed).
///
/// The `rolling` field provides a 5-minute sliding window of throughput data,
/// updated once per second by a dedicated tick task in the engine.
pub struct ProcessorMetrics {
    pub total_invocations: AtomicU64,
    pub total_failures: AtomicU64,
    pub consecutive_failures: AtomicU64,
    pub circuit_open: AtomicBool,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub flowfiles_in: AtomicU64,
    pub flowfiles_out: AtomicU64,
    pub last_trigger_nanos: AtomicU64,
    pub active: AtomicBool,
    /// API sets this flag to request a circuit reset; processor loop checks and clears it.
    pub reset_requested: AtomicBool,
    /// When false, the processor's run loop breaks cleanly (per-processor stop).
    pub enabled: AtomicBool,
    /// When true, the processor skips invocation and sleeps (per-processor pause).
    pub paused: AtomicBool,
    /// 5-minute rolling window — guarded by a lightweight mutex (only held during tick).
    rolling: Mutex<RollingWindow>,
}

impl ProcessorMetrics {
    pub fn new() -> Self {
        Self {
            total_invocations: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            circuit_open: AtomicBool::new(false),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            flowfiles_in: AtomicU64::new(0),
            flowfiles_out: AtomicU64::new(0),
            last_trigger_nanos: AtomicU64::new(0),
            active: AtomicBool::new(false),
            reset_requested: AtomicBool::new(false),
            enabled: AtomicBool::new(true),
            paused: AtomicBool::new(false),
            rolling: Mutex::new(RollingWindow::new()),
        }
    }

    /// Push supervisor state to shared atomics after each invocation.
    pub fn sync_from_supervisor(
        &self,
        total_invocations: u64,
        total_failures: u64,
        consecutive_failures: u32,
        circuit_open: bool,
    ) {
        self.total_invocations
            .store(total_invocations, Ordering::Relaxed);
        self.total_failures.store(total_failures, Ordering::Relaxed);
        self.consecutive_failures
            .store(consecutive_failures as u64, Ordering::Relaxed);
        self.circuit_open.store(circuit_open, Ordering::Relaxed);
    }

    /// Record a one-second tick: snapshot current counters, compute deltas,
    /// and store in the rolling ring buffer.
    ///
    /// Called once per second by the engine's metrics tick task.
    pub fn record_tick(&self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let ff_in = self.flowfiles_in.load(Ordering::Relaxed);
        let ff_out = self.flowfiles_out.load(Ordering::Relaxed);
        let b_in = self.bytes_in.load(Ordering::Relaxed);
        let b_out = self.bytes_out.load(Ordering::Relaxed);

        self.rolling
            .lock()
            .record(now_secs, ff_in, ff_out, b_in, b_out);
    }

    /// Get a rolling 5-minute snapshot of throughput rates.
    pub fn rolling_snapshot(&self) -> RollingSnapshot {
        self.rolling.lock().snapshot()
    }

    /// Take a point-in-time snapshot for serialization (includes rolling metrics).
    pub fn snapshot(&self) -> MetricsSnapshot {
        let enabled = self.enabled.load(Ordering::Relaxed);
        let paused = self.paused.load(Ordering::Relaxed);
        let active = self.active.load(Ordering::Relaxed);

        let state = if !enabled {
            ProcessorState::Stopped
        } else if paused {
            ProcessorState::Paused
        } else if active {
            ProcessorState::Running
        } else {
            ProcessorState::Stopped
        };

        let rolling = self.rolling_snapshot();

        MetricsSnapshot {
            total_invocations: self.total_invocations.load(Ordering::Relaxed),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            circuit_open: self.circuit_open.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            flowfiles_in: self.flowfiles_in.load(Ordering::Relaxed),
            flowfiles_out: self.flowfiles_out.load(Ordering::Relaxed),
            last_trigger_nanos: self.last_trigger_nanos.load(Ordering::Relaxed),
            active,
            state,
            rolling,
        }
    }
}

impl Default for ProcessorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Derived processor state from control flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorState {
    Running,
    Paused,
    Stopped,
}

impl ProcessorState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

/// Copyable point-in-time snapshot of processor metrics.
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub total_invocations: u64,
    pub total_failures: u64,
    pub consecutive_failures: u64,
    pub circuit_open: bool,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub flowfiles_in: u64,
    pub flowfiles_out: u64,
    pub last_trigger_nanos: u64,
    pub active: bool,
    pub state: ProcessorState,
    pub rolling: RollingSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_window_records_deltas() {
        let mut w = RollingWindow::new();
        // First tick initializes previous values.
        w.record(1000, 10, 5, 1000, 500);
        assert_eq!(w.prev_flowfiles_in, 10);

        // Second tick records delta.
        w.record(1001, 20, 12, 2000, 1200);
        let snap = w.snapshot();
        assert_eq!(snap.flowfiles_in_5m, 10);
        assert_eq!(snap.flowfiles_out_5m, 7);
        assert_eq!(snap.bytes_in_5m, 1000);
        assert_eq!(snap.bytes_out_5m, 700);
    }

    #[test]
    fn rolling_window_handles_gaps() {
        let mut w = RollingWindow::new();
        w.record(1000, 0, 0, 0, 0);
        w.record(1001, 100, 50, 10000, 5000);
        // Skip 3 seconds.
        w.record(1004, 200, 100, 20000, 10000);
        let snap = w.snapshot();
        // Buckets: [1001]=100/50/10000/5000, [1002]=0, [1003]=0, [1004]=100/50/10000/5000
        assert_eq!(snap.flowfiles_in_5m, 200);
        assert_eq!(snap.flowfiles_out_5m, 100);
    }

    #[test]
    fn rolling_window_wraps_around() {
        let mut w = RollingWindow::new();
        w.record(1000, 0, 0, 0, 0);
        // Fill 300 ticks.
        for i in 1..=300 {
            w.record(1000 + i, i * 10, i * 5, i * 100, i * 50);
        }
        // The window should only contain deltas from the last 300 ticks.
        let snap = w.snapshot();
        // Each tick has delta of 10 ff_in, so 300 * 10 = 3000.
        assert_eq!(snap.flowfiles_in_5m, 3000);
        assert_eq!(snap.flowfiles_out_5m, 1500);

        // Now one more tick pushes the oldest out.
        w.record(1301, 301 * 10, 301 * 5, 301 * 100, 301 * 50);
        let snap = w.snapshot();
        // Still 300 buckets, each with delta=10, so still 3000.
        assert_eq!(snap.flowfiles_in_5m, 3000);
    }

    #[test]
    fn rolling_window_same_second_merges() {
        let mut w = RollingWindow::new();
        w.record(1000, 0, 0, 0, 0);
        w.record(1001, 10, 5, 100, 50);
        // Same second again — delta should merge.
        w.record(1001, 20, 12, 200, 120);
        let snap = w.snapshot();
        // Total: first call delta 10/5/100/50, merge delta 10/7/100/70 → 20/12/200/120.
        assert_eq!(snap.flowfiles_in_5m, 20);
        assert_eq!(snap.flowfiles_out_5m, 12);
    }

    #[test]
    fn rolling_rates_computed_correctly() {
        let mut w = RollingWindow::new();
        w.record(1000, 0, 0, 0, 0);
        w.record(1001, 300, 0, 0, 0);
        let snap = w.snapshot();
        // 300 ff_in over 300 second window = 1.0 ff/s.
        assert!((snap.flowfiles_in_rate - 1.0).abs() < 0.01);
    }

    #[test]
    fn processor_metrics_record_tick_and_snapshot() {
        let m = ProcessorMetrics::new();
        m.flowfiles_in.store(50, Ordering::Relaxed);
        m.flowfiles_out.store(25, Ordering::Relaxed);
        m.bytes_in.store(5000, Ordering::Relaxed);
        m.bytes_out.store(2500, Ordering::Relaxed);

        m.record_tick();

        // After first tick, rolling should have zero deltas (just initialization).
        let snap = m.snapshot();
        assert_eq!(snap.rolling.flowfiles_in_5m, 0);

        // Simulate more data and another tick.
        m.flowfiles_in.store(100, Ordering::Relaxed);
        m.flowfiles_out.store(50, Ordering::Relaxed);
        m.bytes_in.store(10000, Ordering::Relaxed);
        m.bytes_out.store(5000, Ordering::Relaxed);

        // Force a different second by manipulating internals is tricky,
        // so just verify the rolling snapshot API works without panicking.
        m.record_tick();
        let snap = m.snapshot();
        // In the same second, deltas get merged into the same bucket.
        // The exact values depend on timing, so just verify non-panic.
        assert!(snap.rolling.flowfiles_in_5m <= 50);
    }

    #[test]
    fn large_gap_clears_window() {
        let mut w = RollingWindow::new();
        w.record(1000, 0, 0, 0, 0);
        w.record(1001, 100, 50, 10000, 5000);
        // Gap larger than the window — should clear everything and write new delta.
        w.record(2000, 200, 100, 20000, 10000);
        let snap = w.snapshot();
        assert_eq!(snap.flowfiles_in_5m, 100);
        assert_eq!(snap.flowfiles_out_5m, 50);
    }
}
