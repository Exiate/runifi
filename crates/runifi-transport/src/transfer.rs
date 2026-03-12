//! Transfer strategies for different file size tiers.
//! TODO: Implement QUIC-based transport, zero-copy sendfile, and io_uring backends.

/// Size thresholds for transfer strategy selection.
pub const SMALL_FILE_THRESHOLD: u64 = 64 * 1024; // 64 KB
pub const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100 MB

/// Determines the optimal transfer strategy based on file size.
pub enum TransferStrategy {
    /// Inline in QUIC stream — for files under 64KB (batch-friendly)
    InlineStream,
    /// Chunked streaming — for typical files (30MB range)
    ChunkedStream,
    /// Memory-mapped + zero-copy — for large files (disk images, databases)
    ZeroCopy,
}

impl TransferStrategy {
    pub fn for_size(size: u64) -> Self {
        if size <= SMALL_FILE_THRESHOLD {
            Self::InlineStream
        } else if size <= LARGE_FILE_THRESHOLD {
            Self::ChunkedStream
        } else {
            Self::ZeroCopy
        }
    }
}
