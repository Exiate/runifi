use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_LOG_LEVEL: PropertyDescriptor = PropertyDescriptor::new(
    "Log Level",
    "Logging level: 'trace', 'debug', 'info', 'warn', or 'error'",
)
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
        let log_level = context
            .get_property("Log Level")
            .unwrap_or("info")
            .to_string();

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

            let message = format!("FlowFile attributes:\n{}{}", attrs, content_info);
            let processor_name = context.name();
            let ff_id = flowfile.id;

            match log_level.as_str() {
                "trace" => tracing::trace!(
                    processor = processor_name,
                    flowfile_id = ff_id,
                    "{}",
                    message
                ),
                "debug" => tracing::debug!(
                    processor = processor_name,
                    flowfile_id = ff_id,
                    "{}",
                    message
                ),
                "warn" => tracing::warn!(
                    processor = processor_name,
                    flowfile_id = ff_id,
                    "{}",
                    message
                ),
                "error" => tracing::error!(
                    processor = processor_name,
                    flowfile_id = ff_id,
                    "{}",
                    message
                ),
                _ => tracing::info!(
                    processor = processor_name,
                    flowfile_id = ff_id,
                    "{}",
                    message
                ),
            }

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
        tags: &["Debug", "Logging"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use std::sync::Arc;

    struct TestContext {
        log_level: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Log Level" => PropertyValue::String(self.log_level.clone()),
                _ => PropertyValue::Unset,
            }
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

    fn make_test_ff() -> FlowFile {
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
        ff
    }

    #[test]
    fn logs_and_transfers() {
        let mut proc = LogAttribute::new();
        let ctx = TestContext {
            log_level: "info".to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_test_ff()),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
    }

    #[test]
    fn supports_debug_level() {
        let mut proc = LogAttribute::new();
        let ctx = TestContext {
            log_level: "debug".to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_test_ff()),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
    }

    #[test]
    fn supports_warn_level() {
        let mut proc = LogAttribute::new();
        let ctx = TestContext {
            log_level: "warn".to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_test_ff()),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
    }
}
