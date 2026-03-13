use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::event::AuditEvent;

/// Trait for audit event sinks.
///
/// Implementations must be Send + Sync for use in async contexts.
/// The `log` method is intentionally synchronous — audit logging
/// should not block on async IO in the hot path.
pub trait AuditLogger: Send + Sync {
    fn log(&self, event: &AuditEvent);
}

/// Emits audit events via the `tracing` framework with structured fields.
///
/// Suitable for development, testing, and integration with existing
/// log aggregation pipelines (ELK, Splunk, etc.).
pub struct TracingAuditLogger;

impl AuditLogger for TracingAuditLogger {
    fn log(&self, event: &AuditEvent) {
        let json = serde_json::to_string(event).unwrap_or_else(|e| {
            format!("{{\"error\": \"failed to serialize audit event: {}\"}}", e)
        });
        tracing::info!(target: "audit", event = %json, "audit_event");
    }
}

/// Writes audit events as JSON lines to a dedicated file.
///
/// Each event is serialized as a single JSON line followed by a newline,
/// making the file suitable for log shipping, SIEM ingestion, and
/// compliance evidence collection.
///
/// The writer is buffered and flushed after each event to balance
/// performance with durability.
pub struct FileAuditLogger {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl FileAuditLogger {
    /// Open (or create) the audit log file at `path`.
    ///
    /// Parent directories must already exist. The file is opened in
    /// append mode so existing entries are preserved across restarts.
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            path,
        })
    }

    /// Return the path this logger writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AuditLogger for FileAuditLogger {
    fn log(&self, event: &AuditEvent) {
        let mut line = match serde_json::to_vec(event) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "Failed to serialize audit event");
                return;
            }
        };
        line.push(b'\n');

        let mut writer = match self.writer.lock() {
            Ok(w) => w,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Err(e) = writer.write_all(&line) {
            tracing::error!(error = %e, path = %self.path.display(), "Failed to write audit event to file");
            return;
        }
        if let Err(e) = writer.flush() {
            tracing::error!(error = %e, path = %self.path.display(), "Failed to flush audit log file");
        }
    }
}

/// Composite logger that forwards events to multiple sinks.
pub struct CompositeAuditLogger {
    loggers: Vec<Box<dyn AuditLogger>>,
}

impl CompositeAuditLogger {
    pub fn new(loggers: Vec<Box<dyn AuditLogger>>) -> Self {
        Self { loggers }
    }
}

impl AuditLogger for CompositeAuditLogger {
    fn log(&self, event: &AuditEvent) {
        for logger in &self.loggers {
            logger.log(event);
        }
    }
}

/// A no-op logger for when auditing is disabled.
pub struct NullAuditLogger;

impl AuditLogger for NullAuditLogger {
    fn log(&self, _event: &AuditEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::event::{AuditAction, AuditOutcome, AuditTarget};
    use std::sync::Arc;

    #[test]
    fn test_file_logger_writes_jsonl() {
        let dir = std::env::temp_dir().join("runifi-audit-test");
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("audit.jsonl");

        let logger = FileAuditLogger::new(&path).expect("create file logger");
        let event = AuditEvent::success(
            AuditAction::ProcessorCreated,
            AuditTarget::processor("test-proc"),
        );

        logger.log(&event);
        logger.log(&event);

        // Read back and verify
        let contents = std::fs::read_to_string(&path).expect("read audit log");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);

        for line in &lines {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            assert_eq!(parsed["action"], "ProcessorCreated");
            assert_eq!(parsed["target"]["resource_type"], "processor");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_composite_logger_forwards_to_all_sinks() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingLogger(Arc<AtomicUsize>);
        impl AuditLogger for CountingLogger {
            fn log(&self, _event: &AuditEvent) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let count_a = Arc::new(AtomicUsize::new(0));
        let count_b = Arc::new(AtomicUsize::new(0));

        let composite = CompositeAuditLogger::new(vec![
            Box::new(CountingLogger(count_a.clone())),
            Box::new(CountingLogger(count_b.clone())),
        ]);

        let event = AuditEvent::success(AuditAction::EngineStarted, AuditTarget::system());
        composite.log(&event);
        composite.log(&event);

        assert_eq!(count_a.load(Ordering::Relaxed), 2);
        assert_eq!(count_b.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_null_logger_does_not_panic() {
        let logger = NullAuditLogger;
        let event = AuditEvent::new(
            AuditAction::EngineShutdown,
            AuditTarget::system(),
            AuditOutcome::Success,
            "system",
            None,
        );
        logger.log(&event);
    }
}
