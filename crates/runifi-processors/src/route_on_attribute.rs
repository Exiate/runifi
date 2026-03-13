use regex_lite::Regex;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

const REL_MATCHED: Relationship = Relationship::new("matched", "FlowFiles matching the condition");
const REL_UNMATCHED: Relationship =
    Relationship::new("unmatched", "FlowFiles not matching the condition");

const PROP_ATTRIBUTE: PropertyDescriptor =
    PropertyDescriptor::new("Attribute Name", "The attribute to check for routing").required();

const PROP_VALUE: PropertyDescriptor =
    PropertyDescriptor::new("Attribute Value", "The expected value for matching").required();

const PROP_MATCHING_STRATEGY: PropertyDescriptor = PropertyDescriptor::new(
    "Matching Strategy",
    "How to match the attribute value: 'exact', 'contains', 'regex', 'starts_with', 'ends_with'",
)
.default_value("exact");

/// Routes FlowFiles based on attribute values.
///
/// FlowFiles whose specified attribute matches the expected value are routed
/// to "matched"; all others go to "unmatched".
pub struct RouteOnAttribute;

impl RouteOnAttribute {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RouteOnAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for RouteOnAttribute {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let attr_name = context.get_property("Attribute Name");
        let expected_value = context.get_property("Attribute Value");
        let strategy = context
            .get_property("Matching Strategy")
            .unwrap_or("exact")
            .to_string();

        let attr_name = attr_name.as_str().unwrap_or("");
        let expected_value = expected_value.as_str().unwrap_or("");

        // Pre-compile regex if strategy is "regex".
        let compiled_regex = if strategy == "regex" {
            match Regex::new(expected_value) {
                Ok(re) => Some(re),
                Err(e) => {
                    tracing::error!(
                        pattern = expected_value,
                        error = %e,
                        "Invalid regex pattern"
                    );
                    None
                }
            }
        } else {
            None
        };

        while let Some(flowfile) = session.get() {
            let matches =
                flowfile
                    .get_attribute(attr_name)
                    .is_some_and(|v| match strategy.as_str() {
                        "contains" => v.contains(expected_value),
                        "starts_with" => v.starts_with(expected_value),
                        "ends_with" => v.ends_with(expected_value),
                        "regex" => compiled_regex
                            .as_ref()
                            .is_some_and(|re| re.is_match(v.as_ref())),
                        _ => v.as_ref() == expected_value,
                    });

            if matches {
                session.transfer(flowfile, &REL_MATCHED);
            } else {
                session.transfer(flowfile, &REL_UNMATCHED);
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_MATCHED, REL_UNMATCHED]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_ATTRIBUTE, PROP_VALUE, PROP_MATCHING_STRATEGY]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "RouteOnAttribute",
        description: "Routes FlowFiles based on attribute value matching",
        factory: || Box::new(RouteOnAttribute::new()),
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
        attr_name: String,
        attr_value: String,
        strategy: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Attribute Name" => PropertyValue::String(self.attr_name.clone()),
                "Attribute Value" => PropertyValue::String(self.attr_value.clone()),
                "Matching Strategy" => PropertyValue::String(self.strategy.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-route"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct MultiFlowFileSession {
        inputs: Vec<FlowFile>,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl ProcessSession for MultiFlowFileSession {
        fn get(&mut self) -> Option<FlowFile> {
            if self.inputs.is_empty() {
                None
            } else {
                Some(self.inputs.remove(0))
            }
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            std::mem::take(&mut self.inputs)
        }
        fn read_content(&self, _ff: &FlowFile) -> ProcessResult<Bytes> {
            Ok(Bytes::new())
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

    fn make_ff(id: u64, attr_name: &str, attr_value: &str) -> FlowFile {
        let mut ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from(attr_name), Arc::from(attr_value));
        ff
    }

    #[test]
    fn routes_exact_match() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            attr_name: "type".to_string(),
            attr_value: "sensor".to_string(),
            strategy: "exact".to_string(),
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![make_ff(1, "type", "sensor"), make_ff(2, "type", "log")],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 2);
        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_contains() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            attr_name: "filename".to_string(),
            attr_value: ".csv".to_string(),
            strategy: "contains".to_string(),
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data.csv"),
                make_ff(2, "filename", "data.json"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_starts_with() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            attr_name: "filename".to_string(),
            attr_value: "data-".to_string(),
            strategy: "starts_with".to_string(),
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data-2024.csv"),
                make_ff(2, "filename", "log-2024.csv"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_ends_with() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            attr_name: "filename".to_string(),
            attr_value: ".csv".to_string(),
            strategy: "ends_with".to_string(),
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data.csv"),
                make_ff(2, "filename", "data.json"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_regex() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            attr_name: "filename".to_string(),
            attr_value: r"^data-\d+\.csv$".to_string(),
            strategy: "regex".to_string(),
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data-2024.csv"),
                make_ff(2, "filename", "log-2024.csv"),
                make_ff(3, "filename", "data-abc.csv"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
        assert_eq!(session.transferred[2].1, "unmatched");
    }
}
