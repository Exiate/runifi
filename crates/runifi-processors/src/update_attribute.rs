use std::sync::Arc;

use regex_lite::Regex;
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

/// The set of property names that are owned by this processor
/// and should not be treated as attribute assignments.
const OWN_PROPERTY_NAMES: &[&str] = &["Delete Attributes Regex"];

/// Sets or modifies FlowFile attributes based on configured properties.
///
/// Any property not in the processor's own descriptor list is treated as
/// an attribute to set: property name becomes the attribute key, property
/// value becomes the attribute value.
///
/// Uses `ProcessContext::property_names()` to dynamically enumerate all
/// configured properties rather than relying on a hardcoded whitelist.
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
        // Compile the delete regex if configured.
        let delete_regex = context
            .get_property("Delete Attributes Regex")
            .as_str()
            .and_then(|pattern| {
                if pattern.is_empty() {
                    return None;
                }
                match Regex::new(pattern) {
                    Ok(re) => Some(re),
                    Err(e) => {
                        tracing::error!(
                            pattern = pattern,
                            error = %e,
                            "Invalid delete attributes regex"
                        );
                        None
                    }
                }
            });

        // Enumerate all configured properties to find attribute assignments.
        // Any property not in OWN_PROPERTY_NAMES is an attribute assignment.
        let all_names = context.property_names();
        let attr_assignments: Vec<(String, String)> = all_names
            .into_iter()
            .filter(|name| !OWN_PROPERTY_NAMES.contains(&name.as_str()))
            .filter_map(|name| {
                context
                    .get_property(&name)
                    .as_str()
                    .map(|v| (name, v.to_string()))
            })
            .collect();

        while let Some(mut flowfile) = session.get() {
            // Apply attribute assignments.
            for (key, value) in &attr_assignments {
                flowfile.set_attribute(Arc::from(key.as_str()), Arc::from(value.as_str()));
            }

            // Apply delete regex if configured.
            if let Some(ref re) = delete_regex {
                flowfile
                    .attributes
                    .retain(|(key, _)| !re.is_match(key.as_ref()));
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

    struct TestContext {
        properties: Vec<(String, String)>,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            for (k, v) in &self.properties {
                if k == name {
                    return PropertyValue::String(v.clone());
                }
            }
            PropertyValue::Unset
        }
        fn property_names(&self) -> Vec<String> {
            self.properties.iter().map(|(k, _)| k.clone()).collect()
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

    fn make_ff() -> FlowFile {
        FlowFile {
            id: 1,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: 1,
            penalized_until_nanos: 0,
        }
    }

    #[test]
    fn sets_arbitrary_attributes() {
        let mut proc = UpdateAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("filename".to_string(), "updated.txt".to_string()),
                ("custom.tag".to_string(), "important".to_string()),
                ("priority".to_string(), "high".to_string()),
            ],
        };

        let mut session = OneFlowFileSession {
            input: Some(make_ff()),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, _) = &session.transferred[0];
        assert_eq!(
            ff.get_attribute("filename").map(|v| v.as_ref().to_string()),
            Some("updated.txt".to_string())
        );
        assert_eq!(
            ff.get_attribute("custom.tag")
                .map(|v| v.as_ref().to_string()),
            Some("important".to_string())
        );
        assert_eq!(
            ff.get_attribute("priority").map(|v| v.as_ref().to_string()),
            Some("high".to_string())
        );
    }

    #[test]
    fn delete_attributes_regex() {
        let mut proc = UpdateAttribute::new();
        let ctx = TestContext {
            properties: vec![(
                "Delete Attributes Regex".to_string(),
                "^temp\\..*".to_string(),
            )],
        };

        let mut ff = make_ff();
        ff.set_attribute(Arc::from("filename"), Arc::from("test.txt"));
        ff.set_attribute(Arc::from("temp.scratch"), Arc::from("value1"));
        ff.set_attribute(Arc::from("temp.debug"), Arc::from("value2"));
        ff.set_attribute(Arc::from("keep.this"), Arc::from("value3"));

        let mut session = OneFlowFileSession {
            input: Some(ff),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, _) = &session.transferred[0];

        // temp.* attributes should be deleted.
        assert!(ff.get_attribute("temp.scratch").is_none());
        assert!(ff.get_attribute("temp.debug").is_none());

        // Non-matching attributes should be preserved.
        assert!(ff.get_attribute("filename").is_some());
        assert!(ff.get_attribute("keep.this").is_some());
    }

    #[test]
    fn skips_own_properties() {
        let mut proc = UpdateAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Delete Attributes Regex".to_string(), "".to_string()),
                ("my-attr".to_string(), "my-value".to_string()),
            ],
        };

        let mut session = OneFlowFileSession {
            input: Some(make_ff()),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        let (ff, _) = &session.transferred[0];
        // "Delete Attributes Regex" should NOT be set as an attribute.
        assert!(ff.get_attribute("Delete Attributes Regex").is_none());
        // "my-attr" should be set.
        assert_eq!(
            ff.get_attribute("my-attr").map(|v| v.as_ref().to_string()),
            Some("my-value".to_string())
        );
    }
}
