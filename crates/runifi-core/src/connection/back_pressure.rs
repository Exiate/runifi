/// Configuration for connection back-pressure thresholds.
#[derive(Debug, Clone, Copy)]
pub struct BackPressureConfig {
    /// Maximum number of FlowFiles in the queue before back-pressure is applied.
    pub max_count: usize,
    /// Maximum total bytes of content in the queue before back-pressure is applied.
    pub max_bytes: u64,
}

impl BackPressureConfig {
    pub const DEFAULT_MAX_COUNT: usize = 10_000;
    pub const DEFAULT_MAX_BYTES: u64 = 1_073_741_824; // 1 GB (matches NiFi default)

    pub const fn new(max_count: usize, max_bytes: u64) -> Self {
        Self {
            max_count,
            max_bytes,
        }
    }
}

impl Default for BackPressureConfig {
    fn default() -> Self {
        Self {
            max_count: Self::DEFAULT_MAX_COUNT,
            max_bytes: Self::DEFAULT_MAX_BYTES,
        }
    }
}
