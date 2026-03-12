use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_LOG_LEVEL: PropertyDescriptor =
    PropertyDescriptor::new("Log Level", "Logging level: 'info', 'debug', or 'trace'")
        .default_value("info");

const PROP_LOG_PAYLOAD: PropertyDescriptor = PropertyDescriptor::new(
    "Log Payload",
    "Whether to log the FlowFile content (true/false)",
)
.default_value("false");

/// Logs FlowFile attributes (and optionally content) for debugging.
pub struct LogAttribute;

impl LogAttribute {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LogAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for LogAttribute {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let log_payload = context.get_property("Log Payload").unwrap_or("false") == "true";

        while let Some(flowfile) = session.get() {
            let mut attrs = String::new();
            for (key, value) in &flowfile.attributes {
                attrs.push_str(&format!("  {}: {}\n", key, value));
            }

            let content_info = if log_payload {
                match session.read_content(&flowfile) {
                    Ok(data) => {
                        if data.len() <= 1024 {
                            format!(
                                "  content ({} bytes): {:?}",
                                data.len(),
                                String::from_utf8_lossy(&data)
                            )
                        } else {
                            format!("  content ({} bytes): [truncated]", data.len())
                        }
                    }
                    Err(_) => "  content: [unavailable]".to_string(),
                }
            } else {
                format!("  content: {} bytes", flowfile.size)
            };

            tracing::info!(
                processor = context.name(),
                flowfile_id = flowfile.id,
                "FlowFile attributes:\n{}{}",
                attrs,
                content_info
            );

            session.transfer(flowfile, &REL_SUCCESS);
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_LOG_LEVEL, PROP_LOG_PAYLOAD]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "LogAttribute",
        description: "Logs FlowFile attributes and optionally content for debugging",
        factory: || Box::new(LogAttribute::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use std::sync::Arc;

    struct TestContext;
    impl ProcessContext for TestContext {
        fn get_property(&self, _name: &str) -> PropertyValue {
            PropertyValue::Unset
        }
        fn name(&self) -> &str {
            "test-log"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct OneFlowFileSession {
        input: Option<FlowFile>,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl ProcessSession for OneFlowFileSession {
        fn get(&mut self) -> Option<FlowFile> {
            self.input.take()
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            self.input.take().into_iter().collect()
        }
        fn read_content(&self, _ff: &FlowFile) -> ProcessResult<Bytes> {
            Ok(Bytes::from_static(b"test content"))
        }
        fn write_content(&mut self, ff: FlowFile, _data: Bytes) -> ProcessResult<FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> FlowFile {
            unimplemented!()
        }
        fn clone_flowfile(&mut self, _ff: &FlowFile) -> FlowFile {
            unimplemented!()
        }
        fn transfer(&mut self, ff: FlowFile, rel: &Relationship) {
            self.transferred.push((ff, rel.name));
        }
        fn remove(&mut self, _ff: FlowFile) {}
        fn penalize(&mut self, ff: FlowFile) -> FlowFile {
            ff
        }
        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    #[test]
    fn logs_and_transfers() {
        let mut proc = LogAttribute::new();
        let ctx = TestContext;

        let mut ff = FlowFile {
            id: 42,
            attributes: Vec::new(),
            content_claim: None,
            size: 100,
            created_at_nanos: 0,
            lineage_start_id: 42,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from("filename"), Arc::from("test.txt"));

        let mut session = OneFlowFileSession {
            input: Some(ff),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
    }
}
