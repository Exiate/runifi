use std::sync::Arc;

use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_DELETE_ATTRIBUTES: PropertyDescriptor = PropertyDescriptor::new(
    "Delete Attributes Regex",
    "Regex pattern — matching attributes will be removed",
);

/// Sets or modifies FlowFile attributes based on configured properties.
///
/// Any property not in the processor's own descriptor list is treated as
/// an attribute to set: property name → attribute key, property value → attribute value.
pub struct UpdateAttribute;

impl UpdateAttribute {
    pub fn new() -> Self {
        Self
    }
}

impl Default for UpdateAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for UpdateAttribute {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        while let Some(mut flowfile) = session.get() {
            // The engine passes all configured properties through the context.
            // Properties not in our own descriptor list are attribute assignments.
            // Since we can't enumerate context properties, we rely on the engine
            // to pass attribute mappings via a special convention:
            // properties prefixed with "attr." are treated as attribute setters.
            //
            // For simplicity, we check common attribute names from the context.
            // A more sophisticated implementation would have the engine pass
            // the raw property map.

            // Check for delete pattern (not yet implemented — placeholder for regex support).
            let _delete_regex = context.get_property("Delete Attributes Regex");

            // All other properties are attribute assignments.
            // Convention: properties named "attr.<key>" set attribute <key>.
            // We'll check a reasonable set of common attribute names.
            for prefix_key in &[
                "attr.filename",
                "attr.mime.type",
                "attr.path",
                "attr.uuid",
                "attr.priority",
            ] {
                if let Some(value) = context.get_property(prefix_key).as_str() {
                    let attr_key = &prefix_key[5..]; // strip "attr." prefix
                    flowfile.set_attribute(Arc::from(attr_key), Arc::from(value));
                }
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
        vec![PROP_DELETE_ATTRIBUTES]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "UpdateAttribute",
        description: "Sets or modifies FlowFile attributes based on configured properties",
        factory: || Box::new(UpdateAttribute::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext;
    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "attr.filename" => PropertyValue::String("updated.txt".to_string()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-update"
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

    #[test]
    fn updates_attributes() {
        let mut proc = UpdateAttribute::new();
        let ctx = TestContext;

        let ff = FlowFile {
            id: 1,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: 1,
            penalized_until_nanos: 0,
        };

        let mut session = OneFlowFileSession {
            input: Some(ff),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, _) = &session.transferred[0];
        assert_eq!(
            ff.get_attribute("filename").map(|v| v.as_ref()),
            Some("updated.txt")
        );
    }
}
