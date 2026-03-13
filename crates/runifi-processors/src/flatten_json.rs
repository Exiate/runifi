use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};
use serde_json::Value;

const PROP_SEPARATOR: PropertyDescriptor = PropertyDescriptor::new(
    "Separator",
    "Characters used to join nested key segments in the flattened output",
)
.default_value(".");

const PROP_FLATTEN_MODE: PropertyDescriptor = PropertyDescriptor::new(
    "Flatten Mode",
    "How to flatten: 'normal' flattens all objects and arrays, \
     'keep-arrays' flattens objects but preserves arrays as-is, \
     'dot-notation' flattens nested objects only (no array indexing)",
)
.default_value("normal")
.allowed_values(&["normal", "keep-arrays", "dot-notation"]);

/// Flattens nested JSON objects into single-level objects with configurable
/// key separators.
///
/// For example, `{"a":{"b":1}}` becomes `{"a.b":1}` with the default
/// dot separator.
///
/// Supports three flatten modes:
/// - **normal**: Flatten all nested objects and arrays (arrays become indexed keys)
/// - **keep-arrays**: Flatten objects but preserve arrays as-is
/// - **dot-notation**: Dot-notation for nested objects only (no array expansion)
pub struct FlattenJson;

impl FlattenJson {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FlattenJson {
    fn default() -> Self {
        Self::new()
    }
}

/// Flatten modes controlling how nested structures are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlattenMode {
    /// Flatten all nested objects and arrays. Arrays produce indexed keys.
    Normal,
    /// Flatten objects but preserve arrays as atomic JSON values.
    KeepArrays,
    /// Dot-notation for nested objects only. Arrays are treated like
    /// keep-arrays (preserved as-is), and only object nesting produces
    /// compound keys.
    DotNotation,
}

impl FlattenMode {
    fn from_str(s: &str) -> Self {
        match s {
            "keep-arrays" => Self::KeepArrays,
            "dot-notation" => Self::DotNotation,
            _ => Self::Normal,
        }
    }
}

/// Recursively flatten a JSON value into a flat key-value map.
fn flatten_value(
    prefix: &str,
    value: Value,
    separator: &str,
    mode: FlattenMode,
    out: &mut serde_json::Map<String, Value>,
) {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                // Preserve empty objects as-is (only for nested keys, not top-level).
                if !prefix.is_empty() {
                    out.insert(prefix.to_string(), Value::Object(map));
                }
            } else {
                for (key, val) in map {
                    let new_key = if prefix.is_empty() {
                        key
                    } else {
                        format!("{prefix}{separator}{key}")
                    };
                    flatten_value(&new_key, val, separator, mode, out);
                }
            }
        }
        Value::Array(arr) => match mode {
            FlattenMode::Normal => {
                if arr.is_empty() {
                    // Preserve empty arrays as-is (only for nested keys).
                    if !prefix.is_empty() {
                        out.insert(prefix.to_string(), Value::Array(arr));
                    }
                } else {
                    for (idx, val) in arr.into_iter().enumerate() {
                        let new_key = if prefix.is_empty() {
                            idx.to_string()
                        } else {
                            format!("{prefix}{separator}{idx}")
                        };
                        flatten_value(&new_key, val, separator, mode, out);
                    }
                }
            }
            FlattenMode::KeepArrays | FlattenMode::DotNotation => {
                // Preserve arrays as atomic values.
                out.insert(prefix.to_string(), Value::Array(arr));
            }
        },
        // Scalars (string, number, bool, null) are leaf values.
        scalar => {
            out.insert(prefix.to_string(), scalar);
        }
    }
}

/// Flatten a top-level JSON value.
fn flatten_json(value: Value, separator: &str, mode: FlattenMode) -> Value {
    match value {
        Value::Object(_) => {
            let mut out = serde_json::Map::new();
            flatten_value("", value, separator, mode, &mut out);
            Value::Object(out)
        }
        // Non-object top-level values pass through unchanged.
        other => other,
    }
}

impl Processor for FlattenJson {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let separator = context.get_property("Separator").unwrap_or(".").to_string();
        let mode = FlattenMode::from_str(context.get_property("Flatten Mode").unwrap_or("normal"));

        while let Some(flowfile) = session.get() {
            let content = session.read_content(&flowfile)?;

            match serde_json::from_slice::<Value>(&content) {
                Ok(parsed) => {
                    let flattened = flatten_json(parsed, &separator, mode);
                    match serde_json::to_vec(&flattened) {
                        Ok(output) => {
                            let flowfile = session.write_content(flowfile, Bytes::from(output))?;
                            session.transfer(flowfile, &REL_SUCCESS);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to serialize flattened JSON");
                            session.transfer(flowfile, &REL_FAILURE);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Invalid JSON content — routing to failure");
                    session.transfer(flowfile, &REL_FAILURE);
                }
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_SEPARATOR, PROP_FLATTEN_MODE]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "FlattenJson",
        description: "Flattens nested JSON objects into single-level with dot-notation keys",
        factory: || Box::new(FlattenJson::new()),
        tags: &["JSON", "Transform"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    // ── Test helpers ────────────────────────────────────────────────

    struct TestContext {
        separator: String,
        mode: String,
    }

    impl TestContext {
        fn default_ctx() -> Self {
            Self {
                separator: ".".to_string(),
                mode: "normal".to_string(),
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Separator" => PropertyValue::String(self.separator.clone()),
                "Flatten Mode" => PropertyValue::String(self.mode.clone()),
                _ => PropertyValue::Unset,
            }
        }

        fn name(&self) -> &str {
            "test-flatten-json"
        }

        fn id(&self) -> &str {
            "test-id"
        }

        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct TestSession {
        inputs: Vec<FlowFile>,
        /// Content stored per FlowFile ID.
        content: std::collections::HashMap<u64, Bytes>,
        transferred: Vec<(FlowFile, &'static str)>,
        next_id: u64,
    }

    impl TestSession {
        fn new() -> Self {
            Self {
                inputs: Vec::new(),
                content: std::collections::HashMap::new(),
                transferred: Vec::new(),
                next_id: 100,
            }
        }

        fn add_flowfile(&mut self, json: &str) -> u64 {
            let id = self.next_id;
            self.next_id += 1;
            let ff = FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: json.len() as u64,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            };
            self.content.insert(id, Bytes::from(json.to_string()));
            self.inputs.push(ff);
            id
        }

        fn output_json(&self, idx: usize) -> Value {
            let (ff, _) = &self.transferred[idx];
            let bytes = self.content.get(&ff.id).expect("content missing");
            serde_json::from_slice(bytes).expect("invalid JSON output")
        }

        fn output_relationship(&self, idx: usize) -> &'static str {
            self.transferred[idx].1
        }
    }

    impl ProcessSession for TestSession {
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
            self.content
                .get(&ff.id)
                .cloned()
                .ok_or(runifi_plugin_api::PluginError::ContentNotFound(ff.id))
        }

        fn write_content(&mut self, ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            self.content.insert(ff.id, data);
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
            let mut cloned = ff.clone();
            cloned.id = id;
            if let Some(bytes) = self.content.get(&ff.id) {
                self.content.insert(id, bytes.clone());
            }
            cloned
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

    // ── Unit tests for flatten logic ────────────────────────────────

    #[test]
    fn flatten_simple_nested_object() {
        let input: Value = serde_json::json!({"a": {"b": 1}});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a.b": 1}));
    }

    #[test]
    fn flatten_deeply_nested() {
        let input: Value = serde_json::json!({"a": {"b": {"c": {"d": 42}}}});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a.b.c.d": 42}));
    }

    #[test]
    fn flatten_with_arrays_normal_mode() {
        let input: Value = serde_json::json!({"a": [1, 2, 3]});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a.0": 1, "a.1": 2, "a.2": 3}));
    }

    #[test]
    fn flatten_nested_array_of_objects_normal() {
        let input: Value = serde_json::json!({
            "users": [
                {"name": "alice", "age": 30},
                {"name": "bob", "age": 25}
            ]
        });
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(
            result,
            serde_json::json!({
                "users.0.name": "alice",
                "users.0.age": 30,
                "users.1.name": "bob",
                "users.1.age": 25
            })
        );
    }

    #[test]
    fn flatten_keep_arrays_mode() {
        let input: Value = serde_json::json!({
            "a": {"b": 1},
            "c": [1, 2, 3]
        });
        let result = flatten_json(input, ".", FlattenMode::KeepArrays);
        assert_eq!(result, serde_json::json!({"a.b": 1, "c": [1, 2, 3]}));
    }

    #[test]
    fn flatten_dot_notation_mode() {
        let input: Value = serde_json::json!({
            "a": {"b": {"c": 1}},
            "d": [10, 20]
        });
        let result = flatten_json(input, ".", FlattenMode::DotNotation);
        assert_eq!(result, serde_json::json!({"a.b.c": 1, "d": [10, 20]}));
    }

    #[test]
    fn flatten_custom_separator() {
        let input: Value = serde_json::json!({"a": {"b": {"c": 1}}});
        let result = flatten_json(input, "_", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a_b_c": 1}));
    }

    #[test]
    fn flatten_slash_separator() {
        let input: Value = serde_json::json!({"a": {"b": 1}});
        let result = flatten_json(input, "/", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a/b": 1}));
    }

    #[test]
    fn flatten_preserves_scalars() {
        let input: Value = serde_json::json!({
            "str": "hello",
            "num": 42,
            "bool": true,
            "null": null
        });
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(
            result,
            serde_json::json!({
                "str": "hello",
                "num": 42,
                "bool": true,
                "null": null
            })
        );
    }

    #[test]
    fn flatten_empty_object() {
        let input: Value = serde_json::json!({});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn flatten_nested_empty_object() {
        let input: Value = serde_json::json!({"a": {}});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a": {}}));
    }

    #[test]
    fn flatten_empty_array() {
        let input: Value = serde_json::json!({"a": []});
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!({"a": []}));
    }

    #[test]
    fn flatten_non_object_passthrough() {
        // Top-level non-objects pass through unchanged.
        let result = flatten_json(Value::String("hello".to_string()), ".", FlattenMode::Normal);
        assert_eq!(result, Value::String("hello".to_string()));

        let result = flatten_json(serde_json::json!(42), ".", FlattenMode::Normal);
        assert_eq!(result, serde_json::json!(42));
    }

    // ── Integration tests (processor on_trigger) ────────────────────

    #[test]
    fn processor_flattens_nested_json() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"a":{"b":1},"c":{"d":{"e":2}}}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.output_relationship(0), "success");
        let output = session.output_json(0);
        assert_eq!(output, serde_json::json!({"a.b": 1, "c.d.e": 2}));
    }

    #[test]
    fn processor_handles_invalid_json() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = TestSession::new();

        session.add_flowfile("this is not json");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.output_relationship(0), "failure");
    }

    #[test]
    fn processor_custom_separator() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext {
            separator: "__".to_string(),
            mode: "normal".to_string(),
        };
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"x":{"y":{"z":99}}}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.output_relationship(0), "success");
        let output = session.output_json(0);
        assert_eq!(output, serde_json::json!({"x__y__z": 99}));
    }

    #[test]
    fn processor_keep_arrays_mode() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext {
            separator: ".".to_string(),
            mode: "keep-arrays".to_string(),
        };
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"a":{"b":1},"c":[1,2,3]}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.output_relationship(0), "success");
        let output = session.output_json(0);
        assert_eq!(output, serde_json::json!({"a.b": 1, "c": [1, 2, 3]}));
    }

    #[test]
    fn processor_dot_notation_mode() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext {
            separator: ".".to_string(),
            mode: "dot-notation".to_string(),
        };
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"a":{"b":1},"c":[10,20]}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.output_relationship(0), "success");
        let output = session.output_json(0);
        assert_eq!(output, serde_json::json!({"a.b": 1, "c": [10, 20]}));
    }

    #[test]
    fn processor_multiple_flowfiles() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"a":{"b":1}}"#);
        session.add_flowfile("invalid");
        session.add_flowfile(r#"{"x":{"y":2}}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 3);
        assert_eq!(session.output_relationship(0), "success");
        assert_eq!(session.output_relationship(1), "failure");
        assert_eq!(session.output_relationship(2), "success");

        assert_eq!(session.output_json(0), serde_json::json!({"a.b": 1}));
        assert_eq!(session.output_json(2), serde_json::json!({"x.y": 2}));
    }

    #[test]
    fn processor_deeply_nested_with_arrays() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = TestSession::new();

        session.add_flowfile(r#"{"level1":{"level2":{"level3":[{"key":"val1"},{"key":"val2"}]}}}"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.output_relationship(0), "success");
        let output = session.output_json(0);
        assert_eq!(
            output,
            serde_json::json!({
                "level1.level2.level3.0.key": "val1",
                "level1.level2.level3.1.key": "val2"
            })
        );
    }

    #[test]
    fn processor_empty_content() {
        let mut proc = FlattenJson::new();
        let ctx = TestContext::default_ctx();
        let mut session = TestSession::new();

        session.add_flowfile("");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.output_relationship(0), "failure");
    }

    #[test]
    fn processor_relationships_and_properties() {
        let proc = FlattenJson::new();
        let rels = proc.relationships();
        assert_eq!(rels.len(), 2);
        assert!(rels.iter().any(|r| r.name == "success"));
        assert!(rels.iter().any(|r| r.name == "failure"));

        let props = proc.property_descriptors();
        assert_eq!(props.len(), 2);
        assert!(props.iter().any(|p| p.name == "Separator"));
        assert!(props.iter().any(|p| p.name == "Flatten Mode"));
    }

    #[test]
    fn flatten_mixed_types_in_array() {
        let input: Value = serde_json::json!({
            "data": [1, "two", true, null, {"nested": "obj"}]
        });
        let result = flatten_json(input, ".", FlattenMode::Normal);
        assert_eq!(
            result,
            serde_json::json!({
                "data.0": 1,
                "data.1": "two",
                "data.2": true,
                "data.3": null,
                "data.4.nested": "obj"
            })
        );
    }
}
