use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared processor metrics — atomics for zero-contention reads from the API.
///
/// The processor loop writes to these atomics; the API reads them.
/// All operations use `Relaxed` ordering (no cross-field consistency needed).
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

    /// Take a point-in-time snapshot for serialization.
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
}
