use std::fmt::Write;
use std::sync::Arc;

use parking_lot::RwLock;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::reporting::{ReportingContext, ReportingTask, ReportingTaskDescriptor};
use runifi_plugin_api::result::ProcessResult;

/// Exposes metrics in Prometheus text exposition format.
///
/// The latest metrics text is stored in a shared buffer that can be served
/// by the API at `/metrics`.
pub struct PrometheusReportingTask {
    metrics_output: Arc<RwLock<String>>,
}

impl PrometheusReportingTask {
    pub fn new() -> Self {
        Self {
            metrics_output: Arc::new(RwLock::new(String::new())),
        }
    }

    /// Get a handle to the shared metrics output buffer.
    pub fn metrics_handle(&self) -> Arc<RwLock<String>> {
        self.metrics_output.clone()
    }
}

impl Default for PrometheusReportingTask {
    fn default() -> Self {
        Self::new()
    }
}

impl ReportingTask for PrometheusReportingTask {
    fn on_trigger(&mut self, context: &dyn ReportingContext) -> ProcessResult {
        let prefix_prop = context.get_property("Metrics Prefix");
        let prefix = prefix_prop.unwrap_or("runifi");

        let mut output = String::with_capacity(4096);

        // Processor metrics
        let statuses = context.processor_statuses();

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_invocations_total",
            "Total processor invocations",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_invocations_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.total_invocations
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_failures_total",
            "Total processor failures",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_failures_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.total_failures
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_flowfiles_in_total",
            "Total FlowFiles received by processor",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_flowfiles_in_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.flowfiles_in
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_flowfiles_out_total",
            "Total FlowFiles sent by processor",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_flowfiles_out_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.flowfiles_out
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_bytes_in_total",
            "Total bytes received by processor",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_bytes_in_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.bytes_in
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "processor_bytes_out_total",
            "Total bytes sent by processor",
            "counter",
        );
        for s in &statuses {
            let _ = writeln!(
                output,
                "{}_processor_bytes_out_total{{processor=\"{}\"}} {}",
                prefix, s.name, s.bytes_out
            );
        }

        // Connection metrics
        let connections = context.connection_statuses();

        write_metric_help_type(
            &mut output,
            prefix,
            "connection_queued_count",
            "Number of FlowFiles queued in connection",
            "gauge",
        );
        for c in &connections {
            let _ = writeln!(
                output,
                "{}_connection_queued_count{{source=\"{}\",destination=\"{}\"}} {}",
                prefix, c.source_name, c.destination_name, c.queued_count
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "connection_queued_bytes",
            "Total bytes of queued content in connection",
            "gauge",
        );
        for c in &connections {
            let _ = writeln!(
                output,
                "{}_connection_queued_bytes{{source=\"{}\",destination=\"{}\"}} {}",
                prefix, c.source_name, c.destination_name, c.queued_bytes
            );
        }

        write_metric_help_type(
            &mut output,
            prefix,
            "connection_back_pressured",
            "Whether connection is back-pressured (1=yes, 0=no)",
            "gauge",
        );
        for c in &connections {
            let _ = writeln!(
                output,
                "{}_connection_back_pressured{{source=\"{}\",destination=\"{}\"}} {}",
                prefix,
                c.source_name,
                c.destination_name,
                if c.back_pressured { 1 } else { 0 }
            );
        }

        *self.metrics_output.write() = output;

        Ok(())
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("Metrics Prefix", "Prefix for all metric names")
                .default_value("runifi"),
        ]
    }
}

fn write_metric_help_type(
    output: &mut String,
    prefix: &str,
    name: &str,
    help: &str,
    metric_type: &str,
) {
    let _ = writeln!(output, "# HELP {}_{} {}", prefix, name, help);
    let _ = writeln!(output, "# TYPE {}_{} {}", prefix, name, metric_type);
}

inventory::submit! {
    ReportingTaskDescriptor {
        type_name: "PrometheusReportingTask",
        description: "Exposes metrics in Prometheus text exposition format",
        factory: || Box::new(PrometheusReportingTask::new()),
        tags: &["Monitoring", "Prometheus", "Metrics"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::reporting::{BulletinSnapshot, ConnectionStatus, ProcessorStatus};

    struct MockContext;

    impl ReportingContext for MockContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            if name == "Metrics Prefix" {
                PropertyValue::String("runifi".to_string())
            } else {
                PropertyValue::Unset
            }
        }
        fn name(&self) -> &str {
            "prometheus-task"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn processor_statuses(&self) -> Vec<ProcessorStatus> {
            vec![ProcessorStatus {
                name: "generate".to_string(),
                type_name: "GenerateFlowFile".to_string(),
                state: "running".to_string(),
                total_invocations: 1000,
                total_failures: 2,
                flowfiles_in: 0,
                flowfiles_out: 998,
                bytes_in: 0,
                bytes_out: 5_115_904,
                active: true,
            }]
        }
        fn connection_statuses(&self) -> Vec<ConnectionStatus> {
            vec![ConnectionStatus {
                id: "conn-1".to_string(),
                source_name: "generate".to_string(),
                destination_name: "log".to_string(),
                queued_count: 10,
                queued_bytes: 51200,
                back_pressured: false,
            }]
        }
        fn bulletins(&self) -> Vec<BulletinSnapshot> {
            Vec::new()
        }
    }

    #[test]
    fn generates_prometheus_format() {
        let mut task = PrometheusReportingTask::new();
        let handle = task.metrics_handle();
        let ctx = MockContext;

        task.on_trigger(&ctx).unwrap();

        let output = handle.read().clone();
        assert!(output.contains("# HELP runifi_processor_invocations_total"));
        assert!(output.contains("# TYPE runifi_processor_invocations_total counter"));
        assert!(output.contains("runifi_processor_invocations_total{processor=\"generate\"} 1000"));
        assert!(output.contains("runifi_processor_failures_total{processor=\"generate\"} 2"));
        assert!(output.contains(
            "runifi_connection_queued_count{source=\"generate\",destination=\"log\"} 10"
        ));
        assert!(output.contains(
            "runifi_connection_back_pressured{source=\"generate\",destination=\"log\"} 0"
        ));
    }

    #[test]
    fn has_property_descriptors() {
        let task = PrometheusReportingTask::new();
        let descriptors = task.property_descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].name, "Metrics Prefix");
    }
}
