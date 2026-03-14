use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_ORIGINAL};

const REL_SPLIT: Relationship = Relationship::new(
    "split",
    "Individual FlowFiles, one per element from the split array",
);

const PROP_JSONPATH: PropertyDescriptor = PropertyDescriptor::new(
    "JsonPath Expression",
    "JSONPath expression pointing to the array to split. Use '$' for the root array.",
)
.default_value("$");

const PROP_NULL_VALUE: PropertyDescriptor = PropertyDescriptor::new(
    "Null Value Representation",
    "How to represent null values in output JSON",
)
.default_value("");

/// Splits a JSON array in FlowFile content into individual FlowFiles.
///
/// Each element of the target array becomes a separate FlowFile with the element's
/// JSON serialization as content. Fragment metadata attributes are added to each
/// split FlowFile (`fragment.index`, `fragment.count`, `segment.original.filename`).
///
/// If the JSONPath expression targets a nested array, only that array is split.
/// The original FlowFile is always routed to the "original" relationship on success.
/// FlowFiles that cannot be parsed as JSON or whose JSONPath result is not an array
/// are routed to "failure".
pub struct SplitJson;

impl SplitJson {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SplitJson {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for SplitJson {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let jsonpath_expr = context
            .get_property("JsonPath Expression")
            .unwrap_or("$")
            .to_string();
        let null_repr = context
            .get_property("Null Value Representation")
            .unwrap_or("")
            .to_string();

        while let Some(flowfile) = session.get() {
            let content = session.read_content(&flowfile)?;

            // Parse the content as JSON.
            let json_value: serde_json::Value = match serde_json::from_slice(&content) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        processor = context.name(),
                        error = %e,
                        "Failed to parse FlowFile content as JSON"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            // Apply JSONPath expression.
            let selected = if jsonpath_expr == "$" {
                // Root path — use the parsed value directly.
                vec![json_value.clone()]
            } else {
                match jsonpath_lib::select(&json_value, &jsonpath_expr) {
                    Ok(results) => results.into_iter().cloned().collect(),
                    Err(e) => {
                        tracing::warn!(
                            processor = context.name(),
                            jsonpath = %jsonpath_expr,
                            error = %e,
                            "JSONPath expression evaluation failed"
                        );
                        session.transfer(flowfile, &REL_FAILURE);
                        continue;
                    }
                }
            };

            // We expect exactly one result, and it must be an array.
            let array = if selected.len() == 1 {
                match &selected[0] {
                    serde_json::Value::Array(arr) => arr.clone(),
                    _ => {
                        tracing::warn!(
                            processor = context.name(),
                            jsonpath = %jsonpath_expr,
                            "JSONPath result is not an array"
                        );
                        session.transfer(flowfile, &REL_FAILURE);
                        continue;
                    }
                }
            } else if selected.is_empty() {
                tracing::warn!(
                    processor = context.name(),
                    jsonpath = %jsonpath_expr,
                    "JSONPath expression matched no results"
                );
                session.transfer(flowfile, &REL_FAILURE);
                continue;
            } else {
                // Multiple results — treat them as the array elements directly.
                // This handles cases like `$.items[*]` which returns each element.
                selected
            };

            let fragment_count = array.len();

            // Get original filename for fragment metadata.
            let original_filename = flowfile
                .get_attribute("filename")
                .cloned()
                .unwrap_or_else(|| Arc::from(""));

            // Create a split FlowFile for each element.
            for (index, element) in array.iter().enumerate() {
                let mut split_ff = session.create();

                // Copy attributes from the original FlowFile.
                for (key, value) in &flowfile.attributes {
                    split_ff.set_attribute(key.clone(), value.clone());
                }

                // Serialize the element to JSON.
                let element_json = if element.is_null() && !null_repr.is_empty() {
                    Bytes::from(null_repr.clone())
                } else {
                    match serde_json::to_vec(element) {
                        Ok(bytes) => Bytes::from(bytes),
                        Err(e) => {
                            tracing::error!(
                                processor = context.name(),
                                index = index,
                                error = %e,
                                "Failed to serialize array element"
                            );
                            session.remove(split_ff);
                            continue;
                        }
                    }
                };

                split_ff = session.write_content(split_ff, element_json)?;

                // Set fragment metadata attributes.
                split_ff.set_attribute(
                    Arc::from("fragment.index"),
                    Arc::from(index.to_string().as_str()),
                );
                split_ff.set_attribute(
                    Arc::from("fragment.count"),
                    Arc::from(fragment_count.to_string().as_str()),
                );
                split_ff.set_attribute(
                    Arc::from("segment.original.filename"),
                    original_filename.clone(),
                );

                session.transfer(split_ff, &REL_SPLIT);
            }

            // Route original to "original" relationship.
            session.transfer(flowfile, &REL_ORIGINAL);
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SPLIT, REL_ORIGINAL, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_JSONPATH, PROP_NULL_VALUE]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "SplitJson",
        description: "Splits a JSON array into individual FlowFiles, one per element",
        factory: || Box::new(SplitJson::new()),
        tags: &["JSON", "Transformation", "Split"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        jsonpath: String,
        null_repr: String,
    }

    impl TestContext {
        fn default_ctx() -> Self {
            Self {
                jsonpath: "$".to_string(),
                null_repr: String::new(),
            }
        }

        fn with_jsonpath(jsonpath: &str) -> Self {
            Self {
                jsonpath: jsonpath.to_string(),
                null_repr: String::new(),
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "JsonPath Expression" => PropertyValue::String(self.jsonpath.clone()),
                "Null Value Representation" => PropertyValue::String(self.null_repr.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-split-json"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct SplitTestSession {
        inputs: Vec<FlowFile>,
        content_store: Vec<(u64, Bytes)>,
        transferred: Vec<(FlowFile, &'static str)>,
        next_id: u64,
    }

    impl SplitTestSession {
        fn new() -> Self {
            Self {
                inputs: Vec::new(),
                content_store: Vec::new(),
                transferred: Vec::new(),
                next_id: 100,
            }
        }

        fn add_input(&mut self, id: u64, content: &str) -> &mut FlowFile {
            let ff = FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: content.len() as u64,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            };
            self.content_store
                .push((id, Bytes::from(content.to_string())));
            self.inputs.push(ff);
            self.inputs.last_mut().unwrap()
        }

        fn get_transferred_content(&self, ff: &FlowFile) -> Option<Bytes> {
            self.content_store
                .iter()
                .find(|(id, _)| *id == ff.id)
                .map(|(_, data)| data.clone())
        }

        fn transferred_to(&self, rel: &str) -> Vec<&FlowFile> {
            self.transferred
                .iter()
                .filter(|(_, r)| *r == rel)
                .map(|(ff, _)| ff)
                .collect()
        }
    }

    impl ProcessSession for SplitTestSession {
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

        fn read_content(&self, ff: &FlowFile) -> ProcessResult<Bytes> {
            self.content_store
                .iter()
                .find(|(id, _)| *id == ff.id)
                .map(|(_, data)| data.clone())
                .ok_or(runifi_plugin_api::PluginError::ContentNotFound(ff.id))
        }

        fn write_content(&mut self, mut ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            ff.size = data.len() as u64;
            // Remove existing content for this id, then store new.
            self.content_store.retain(|(id, _)| *id != ff.id);
            self.content_store.push((ff.id, data));
            Ok(ff)
        }

        fn create(&mut self) -> FlowFile {
            let id = self.next_id;
            self.next_id += 1;
            FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: 0,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            }
        }

        fn clone_flowfile(&mut self, ff: &FlowFile) -> FlowFile {
            let id = self.next_id;
            self.next_id += 1;
            let mut cloned = FlowFile {
                id,
                attributes: ff.attributes.clone(),
                content_claim: ff.content_claim.clone(),
                size: ff.size,
                created_at_nanos: ff.created_at_nanos,
                lineage_start_id: ff.lineage_start_id,
                penalized_until_nanos: 0,
            };
            // Copy content.
            let content_copy = self
                .content_store
                .iter()
                .find(|(fid, _)| *fid == ff.id)
                .map(|(_, data)| data.clone());
            if let Some(data) = content_copy {
                cloned.size = data.len() as u64;
                self.content_store.push((id, data));
            }
            cloned
        }

        fn transfer(&mut self, ff: FlowFile, rel: &Relationship) {
            self.transferred.push((ff, rel.name));
        }

        fn remove(&mut self, ff: FlowFile) {
            self.content_store.retain(|(id, _)| *id != ff.id);
        }

        fn penalize(&mut self, ff: FlowFile) -> FlowFile {
            ff
        }

        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    #[test]
    fn splits_simple_array() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, "[1, 2, 3]");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 3);

        let originals = session.transferred_to("original");
        assert_eq!(originals.len(), 1);
        assert_eq!(originals[0].id, 1);

        // Verify content of split FlowFiles.
        for (i, ff) in splits.iter().enumerate() {
            let content = session.get_transferred_content(ff).unwrap();
            let val: serde_json::Value = serde_json::from_slice(&content).unwrap();
            assert_eq!(val, serde_json::json!(i + 1));
        }
    }

    #[test]
    fn splits_nested_objects() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"[{"a":1}, {"a":2}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 2);

        let content0 = session.get_transferred_content(splits[0]).unwrap();
        let val0: serde_json::Value = serde_json::from_slice(&content0).unwrap();
        assert_eq!(val0, serde_json::json!({"a": 1}));

        let content1 = session.get_transferred_content(splits[1]).unwrap();
        let val1: serde_json::Value = serde_json::from_slice(&content1).unwrap();
        assert_eq!(val1, serde_json::json!({"a": 2}));
    }

    #[test]
    fn splits_with_jsonpath_targeting_nested_array() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::with_jsonpath("$.data.items");
        let mut session = SplitTestSession::new();
        session.add_input(
            1,
            r#"{"data": {"items": ["x", "y", "z"], "other": "ignored"}}"#,
        );

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 3);

        let content0 = session.get_transferred_content(splits[0]).unwrap();
        assert_eq!(content0.as_ref(), b"\"x\"");

        let content2 = session.get_transferred_content(splits[2]).unwrap();
        assert_eq!(content2.as_ref(), b"\"z\"");

        assert_eq!(session.transferred_to("original").len(), 1);
    }

    #[test]
    fn invalid_json_routes_to_failure() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, "not valid json {{{");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("failure").len(), 1);
        assert_eq!(session.transferred_to("split").len(), 0);
        assert_eq!(session.transferred_to("original").len(), 0);
    }

    #[test]
    fn non_array_json_routes_to_failure() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"{"key": "value"}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("failure").len(), 1);
        assert_eq!(session.transferred_to("split").len(), 0);
        assert_eq!(session.transferred_to("original").len(), 0);
    }

    #[test]
    fn empty_array_routes_original_only() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, "[]");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("split").len(), 0);
        assert_eq!(session.transferred_to("original").len(), 1);
        assert_eq!(session.transferred_to("failure").len(), 0);
    }

    #[test]
    fn sets_fragment_attributes() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        let ff = session.add_input(1, r#"["a", "b", "c"]"#);
        ff.set_attribute(Arc::from("filename"), Arc::from("test.json"));

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 3);

        for (i, ff) in splits.iter().enumerate() {
            assert_eq!(
                ff.get_attribute("fragment.index").map(|v| v.as_ref()),
                Some(i.to_string().as_str())
            );
            assert_eq!(
                ff.get_attribute("fragment.count").map(|v| v.as_ref()),
                Some("3")
            );
            assert_eq!(
                ff.get_attribute("segment.original.filename")
                    .map(|v| v.as_ref()),
                Some("test.json")
            );
        }
    }

    #[test]
    fn copies_attributes_from_original() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        let ff = session.add_input(1, r#"[1, 2]"#);
        ff.set_attribute(Arc::from("custom.attr"), Arc::from("preserved"));
        ff.set_attribute(Arc::from("mime.type"), Arc::from("application/json"));

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        for ff in &splits {
            assert_eq!(
                ff.get_attribute("custom.attr").map(|v| v.as_ref()),
                Some("preserved")
            );
            assert_eq!(
                ff.get_attribute("mime.type").map(|v| v.as_ref()),
                Some("application/json")
            );
        }
    }

    #[test]
    fn null_value_representation() {
        let mut proc = SplitJson::new();
        let ctx = TestContext {
            jsonpath: "$".to_string(),
            null_repr: "NULL".to_string(),
        };
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"[1, null, 3]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 3);

        // The null element (index 1) should use the null representation.
        let null_content = session.get_transferred_content(splits[1]).unwrap();
        assert_eq!(null_content.as_ref(), b"NULL");

        // Non-null elements should be normal JSON.
        let first_content = session.get_transferred_content(splits[0]).unwrap();
        assert_eq!(first_content.as_ref(), b"1");
    }

    #[test]
    fn null_value_default_empty_string() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"[null]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.transferred_to("split");
        assert_eq!(splits.len(), 1);

        // With empty null_repr, null is serialized as JSON "null".
        let content = session.get_transferred_content(splits[0]).unwrap();
        assert_eq!(content.as_ref(), b"null");
    }

    #[test]
    fn jsonpath_no_match_routes_to_failure() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::with_jsonpath("$.nonexistent");
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"{"data": [1, 2]}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("failure").len(), 1);
        assert_eq!(session.transferred_to("split").len(), 0);
    }

    #[test]
    fn jsonpath_non_array_result_routes_to_failure() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::with_jsonpath("$.name");
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"{"name": "test"}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("failure").len(), 1);
        assert_eq!(session.transferred_to("split").len(), 0);
    }

    #[test]
    fn processes_multiple_flowfiles() {
        let mut proc = SplitJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = SplitTestSession::new();
        session.add_input(1, r#"[1, 2]"#);
        session.add_input(2, r#"[3, 4, 5]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred_to("split").len(), 5);
        assert_eq!(session.transferred_to("original").len(), 2);
    }
}
