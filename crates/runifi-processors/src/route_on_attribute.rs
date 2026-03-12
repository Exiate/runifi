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

const PROP_VALUE: PropertyDescriptor = PropertyDescriptor::new(
    "Attribute Value",
    "The expected value for matching (exact match)",
)
.required();

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

        let attr_name = attr_name.as_str().unwrap_or("");
        let expected_value = expected_value.as_str().unwrap_or("");

        while let Some(flowfile) = session.get() {
            let matches = flowfile
                .get_attribute(attr_name)
                .is_some_and(|v| v.as_ref() == expected_value);

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
        vec![PROP_ATTRIBUTE, PROP_VALUE]
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

    struct TestContext;
    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Attribute Name" => PropertyValue::String("type".to_string()),
                "Attribute Value" => PropertyValue::String("sensor".to_string()),
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

    fn make_ff(id: u64, type_attr: Option<&str>) -> FlowFile {
        let mut ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        if let Some(t) = type_attr {
            ff.set_attribute(Arc::from("type"), Arc::from(t));
        }
        ff
    }

    #[test]
    fn routes_correctly() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext;

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, Some("sensor")),
                make_ff(2, Some("log")),
                make_ff(3, None),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 3);
        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "unmatched");
        assert_eq!(session.transferred[2].1, "unmatched");
    }
}
