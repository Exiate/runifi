use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::reporting::{ReportingContext, ReportingTask, ReportingTaskDescriptor};
use runifi_plugin_api::result::ProcessResult;

/// Logs processor and connection status at configured intervals.
pub struct LogReportingTask;

impl ReportingTask for LogReportingTask {
    fn on_trigger(&mut self, context: &dyn ReportingContext) -> ProcessResult {
        let log_level_prop = context.get_property("Log Level");
        let log_level = log_level_prop.unwrap_or("INFO");

        let statuses = context.processor_statuses();
        for s in &statuses {
            match log_level {
                "DEBUG" => tracing::debug!(
                    processor = %s.name,
                    r#type = %s.type_name,
                    state = %s.state,
                    invocations = s.total_invocations,
                    failures = s.total_failures,
                    ff_in = s.flowfiles_in,
                    ff_out = s.flowfiles_out,
                    bytes_in = s.bytes_in,
                    bytes_out = s.bytes_out,
                    "Processor status"
                ),
                "WARN" => tracing::warn!(
                    processor = %s.name,
                    state = %s.state,
                    failures = s.total_failures,
                    "Processor status"
                ),
                _ => tracing::info!(
                    processor = %s.name,
                    r#type = %s.type_name,
                    state = %s.state,
                    invocations = s.total_invocations,
                    failures = s.total_failures,
                    ff_in = s.flowfiles_in,
                    ff_out = s.flowfiles_out,
                    "Processor status"
                ),
            }
        }

        let connections = context.connection_statuses();
        for c in &connections {
            tracing::info!(
                connection = %c.id,
                source = %c.source_name,
                dest = %c.destination_name,
                queued = c.queued_count,
                queued_bytes = c.queued_bytes,
                back_pressured = c.back_pressured,
                "Connection status"
            );
        }

        Ok(())
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("Log Level", "Log level for status output")
                .default_value("INFO")
                .allowed_values(&["DEBUG", "INFO", "WARN"]),
        ]
    }
}

inventory::submit! {
    ReportingTaskDescriptor {
        type_name: "LogReportingTask",
        description: "Logs processor and connection status at configured intervals",
        factory: || Box::new(LogReportingTask),
        tags: &["Monitoring", "Logging"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::reporting::{BulletinSnapshot, ConnectionStatus, ProcessorStatus};

    struct MockContext {
        processors: Vec<ProcessorStatus>,
        connections: Vec<ConnectionStatus>,
    }

    impl ReportingContext for MockContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            if name == "Log Level" {
                PropertyValue::String("INFO".to_string())
            } else {
                PropertyValue::Unset
            }
        }
        fn name(&self) -> &str {
            "test-logger"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn processor_statuses(&self) -> Vec<ProcessorStatus> {
            self.processors.clone()
        }
        fn connection_statuses(&self) -> Vec<ConnectionStatus> {
            self.connections.clone()
        }
        fn bulletins(&self) -> Vec<BulletinSnapshot> {
            Vec::new()
        }
    }

    #[test]
    fn on_trigger_succeeds_with_data() {
        let ctx = MockContext {
            processors: vec![ProcessorStatus {
                name: "gen".to_string(),
                type_name: "GenerateFlowFile".to_string(),
                state: "running".to_string(),
                total_invocations: 100,
                total_failures: 0,
                flowfiles_in: 0,
                flowfiles_out: 100,
                bytes_in: 0,
                bytes_out: 51200,
                active: true,
            }],
            connections: vec![ConnectionStatus {
                id: "conn-1".to_string(),
                source_name: "gen".to_string(),
                destination_name: "log".to_string(),
                queued_count: 5,
                queued_bytes: 25600,
                back_pressured: false,
            }],
        };

        let mut task = LogReportingTask;
        assert!(task.on_trigger(&ctx).is_ok());
    }

    #[test]
    fn on_trigger_succeeds_with_empty_data() {
        let ctx = MockContext {
            processors: Vec::new(),
            connections: Vec::new(),
        };

        let mut task = LogReportingTask;
        assert!(task.on_trigger(&ctx).is_ok());
    }

    #[test]
    fn has_property_descriptors() {
        let task = LogReportingTask;
        let descriptors = task.property_descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].name, "Log Level");
    }
}
