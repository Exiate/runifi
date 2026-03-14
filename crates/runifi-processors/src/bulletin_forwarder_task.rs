use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::reporting::{ReportingContext, ReportingTask, ReportingTaskDescriptor};
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::state::StatefulSpec;

/// Forwards new bulletins since last trigger to the application log.
///
/// Tracks the last seen bulletin ID to avoid duplicate reporting.
pub struct BulletinForwarderTask {
    last_seen_id: u64,
}

impl BulletinForwarderTask {
    pub fn new() -> Self {
        Self { last_seen_id: 0 }
    }
}

impl Default for BulletinForwarderTask {
    fn default() -> Self {
        Self::new()
    }
}

impl ReportingTask for BulletinForwarderTask {
    fn on_trigger(&mut self, context: &dyn ReportingContext) -> ProcessResult {
        let min_sev_prop = context.get_property("Min Severity");
        let min_severity = min_sev_prop.unwrap_or("warn");

        let bulletins = context.bulletins();
        for b in bulletins {
            if b.id <= self.last_seen_id {
                continue;
            }

            // Filter by minimum severity.
            let should_forward = match min_severity {
                "error" => b.severity == "error",
                _ => b.severity == "warn" || b.severity == "error",
            };

            if should_forward {
                tracing::warn!(
                    bulletin_id = b.id,
                    source = %b.source_name,
                    severity = %b.severity,
                    message = %b.message,
                    timestamp_ms = b.timestamp_ms,
                    "Forwarded bulletin"
                );
            }

            self.last_seen_id = b.id;
        }

        Ok(())
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("Min Severity", "Minimum severity to forward")
                .default_value("warn")
                .allowed_values(&["warn", "error"]),
        ]
    }

    fn stateful(&self) -> Option<StatefulSpec> {
        Some(StatefulSpec::local("Tracks last forwarded bulletin ID"))
    }
}

inventory::submit! {
    ReportingTaskDescriptor {
        type_name: "BulletinForwarderTask",
        description: "Forwards new bulletins to the application log",
        factory: || Box::new(BulletinForwarderTask::new()),
        tags: &["Monitoring", "Bulletins"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::reporting::{BulletinSnapshot, ConnectionStatus, ProcessorStatus};

    struct MockContext {
        bulletins: Vec<BulletinSnapshot>,
    }

    impl ReportingContext for MockContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            if name == "Min Severity" {
                PropertyValue::String("warn".to_string())
            } else {
                PropertyValue::Unset
            }
        }
        fn name(&self) -> &str {
            "bulletin-forwarder"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn processor_statuses(&self) -> Vec<ProcessorStatus> {
            Vec::new()
        }
        fn connection_statuses(&self) -> Vec<ConnectionStatus> {
            Vec::new()
        }
        fn bulletins(&self) -> Vec<BulletinSnapshot> {
            self.bulletins.clone()
        }
    }

    #[test]
    fn forwards_new_bulletins() {
        let ctx = MockContext {
            bulletins: vec![
                BulletinSnapshot {
                    id: 1,
                    timestamp_ms: 1000,
                    severity: "warn".to_string(),
                    source_name: "gen".to_string(),
                    message: "test warning".to_string(),
                },
                BulletinSnapshot {
                    id: 2,
                    timestamp_ms: 2000,
                    severity: "error".to_string(),
                    source_name: "gen".to_string(),
                    message: "test error".to_string(),
                },
            ],
        };

        let mut task = BulletinForwarderTask::new();
        assert!(task.on_trigger(&ctx).is_ok());
        assert_eq!(task.last_seen_id, 2);
    }

    #[test]
    fn skips_already_seen_bulletins() {
        let ctx = MockContext {
            bulletins: vec![BulletinSnapshot {
                id: 1,
                timestamp_ms: 1000,
                severity: "warn".to_string(),
                source_name: "gen".to_string(),
                message: "test warning".to_string(),
            }],
        };

        let mut task = BulletinForwarderTask::new();
        task.last_seen_id = 1;
        assert!(task.on_trigger(&ctx).is_ok());
        assert_eq!(task.last_seen_id, 1);
    }

    #[test]
    fn handles_empty_bulletins() {
        let ctx = MockContext {
            bulletins: Vec::new(),
        };

        let mut task = BulletinForwarderTask::new();
        assert!(task.on_trigger(&ctx).is_ok());
        assert_eq!(task.last_seen_id, 0);
    }

    #[test]
    fn declares_stateful() {
        let task = BulletinForwarderTask::new();
        assert!(task.stateful().is_some());
    }

    #[test]
    fn has_property_descriptors() {
        let task = BulletinForwarderTask::new();
        let descriptors = task.property_descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].name, "Min Severity");
    }
}
