use std::sync::Arc;

use bytes::Bytes;
use jsonpath_rust::parser::parse_json_path;
use jsonpath_rust::query::js_path_process;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use serde_json::Value;

const REL_MATCHED: Relationship =
    Relationship::new("matched", "FlowFiles where at least one JSONPath matched");
const REL_UNMATCHED: Relationship =
    Relationship::new("unmatched", "FlowFiles where no JSONPath matched");
const REL_FAILURE: Relationship = Relationship::new(
    "failure",
    "FlowFiles with invalid JSON or evaluation errors",
);

const PROP_DESTINATION: PropertyDescriptor = PropertyDescriptor::new(
    "Destination",
    "Where to store extracted values: flowfile-attribute or flowfile-content",
)
.default_value("flowfile-attribute")
.allowed_values(&["flowfile-attribute", "flowfile-content"]);

const PROP_RETURN_TYPE: PropertyDescriptor = PropertyDescriptor::new(
    "Return Type",
    "How to format results: auto (scalar for single primitives, JSON otherwise), json, or scalar",
)
.default_value("auto")
.allowed_values(&["auto", "json", "scalar"]);

const PROP_NULL_VALUE: PropertyDescriptor = PropertyDescriptor::new(
    "Null Value Representation",
    "String representation for JSON null values",
)
.default_value("");

/// The set of property names that are owned by this processor
/// and should not be treated as JSONPath expressions.
const OWN_PROPERTY_NAMES: &[&str] = &["Destination", "Return Type", "Null Value Representation"];

/// Extracts values from JSON FlowFile content using JSONPath expressions.
///
/// Dynamic properties define the JSONPath expressions to evaluate:
/// - Property name becomes the attribute name (in attribute mode)
/// - Property value is the JSONPath expression (e.g., `$.store.book[0].title`)
///
/// When Destination is `flowfile-attribute`, each matched expression sets an
/// attribute on the FlowFile. When `flowfile-content`, only a single dynamic
/// property is expected and its result replaces the FlowFile content.
pub struct EvaluateJsonPath;

impl EvaluateJsonPath {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EvaluateJsonPath {
    fn default() -> Self {
        Self::new()
    }
}

/// Formats a JSON value according to the specified return type.
fn format_value(value: &Value, return_type: &str, null_repr: &str) -> String {
    match return_type {
        "json" => serde_json::to_string(value).unwrap_or_default(),
        "scalar" => scalar_value(value, null_repr),
        // "auto": use scalar for primitives, JSON for objects/arrays
        _ => match value {
            Value::Null => null_repr.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => serde_json::to_string(value).unwrap_or_default(),
        },
    }
}

/// Extracts a scalar string from a JSON value.
fn scalar_value(value: &Value, null_repr: &str) -> String {
    match value {
        Value::Null => null_repr.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

impl Processor for EvaluateJsonPath {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let destination = context
            .get_property("Destination")
            .unwrap_or("flowfile-attribute")
            .to_string();
        let return_type = context
            .get_property("Return Type")
            .unwrap_or("auto")
            .to_string();
        let null_repr = context
            .get_property("Null Value Representation")
            .unwrap_or("")
            .to_string();

        // Collect dynamic properties: each is name -> JSONPath expression.
        let all_names = context.property_names();
        let jsonpath_exprs: Vec<(String, String)> = all_names
            .into_iter()
            .filter(|name| !OWN_PROPERTY_NAMES.contains(&name.as_str()))
            .filter_map(|name| {
                context
                    .get_property(&name)
                    .as_str()
                    .map(|v| (name, v.to_string()))
            })
            .collect();

        // Pre-parse all JSONPath expressions.
        let mut parsed_exprs = Vec::with_capacity(jsonpath_exprs.len());
        for (attr_name, expr_str) in &jsonpath_exprs {
            match parse_json_path(expr_str) {
                Ok(parsed) => {
                    parsed_exprs.push((attr_name.as_str(), parsed));
                }
                Err(e) => {
                    tracing::error!(
                        expression = expr_str.as_str(),
                        attribute = attr_name.as_str(),
                        error = %e,
                        "Failed to parse JSONPath expression"
                    );
                    // Route all remaining FlowFiles to failure and return.
                    while let Some(ff) = session.get() {
                        session.transfer(ff, &REL_FAILURE);
                    }
                    session.commit();
                    return Ok(());
                }
            }
        }

        while let Some(flowfile) = session.get() {
            // Read content and parse as JSON.
            let content = match session.read_content(&flowfile) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(
                        flowfile_id = flowfile.id,
                        error = %e,
                        "Failed to read FlowFile content"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            let json_value: Value = match serde_json::from_slice(&content) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        flowfile_id = flowfile.id,
                        error = %e,
                        "FlowFile content is not valid JSON"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            // Evaluate each JSONPath expression.
            let mut any_matched = false;
            let mut flowfile = flowfile;

            if destination == "flowfile-content" {
                // Content mode: only the first dynamic property is used.
                // Its result replaces the FlowFile content.
                if let Some((_, parsed)) = parsed_exprs.first() {
                    match js_path_process(parsed, &json_value) {
                        Ok(results) if !results.is_empty() => {
                            any_matched = true;
                            let vals: Vec<&Value> = results.into_iter().map(|r| r.val()).collect();
                            let output = if vals.len() == 1 {
                                format_value(vals[0], &return_type, &null_repr)
                            } else {
                                // Multiple results: wrap in a JSON array.
                                let arr = Value::Array(vals.into_iter().cloned().collect());
                                format_value(&arr, &return_type, &null_repr)
                            };
                            match session.write_content(flowfile, Bytes::from(output)) {
                                Ok(ff) => flowfile = ff,
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "Failed to write content"
                                    );
                                    // FlowFile consumed by write_content, continue
                                    continue;
                                }
                            }
                        }
                        Ok(_) => {
                            // No match — flowfile stays unmodified
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "JSONPath evaluation error"
                            );
                            session.transfer(flowfile, &REL_FAILURE);
                            continue;
                        }
                    }
                }
            } else {
                // Attribute mode: each expression result is set as an attribute.
                let mut eval_failed = false;
                for (attr_name, parsed) in &parsed_exprs {
                    match js_path_process(parsed, &json_value) {
                        Ok(results) if !results.is_empty() => {
                            any_matched = true;
                            let vals: Vec<&Value> = results.into_iter().map(|r| r.val()).collect();
                            let value_str = if vals.len() == 1 {
                                format_value(vals[0], &return_type, &null_repr)
                            } else {
                                // Multiple results: wrap in a JSON array.
                                let arr = Value::Array(vals.into_iter().cloned().collect());
                                format_value(&arr, &return_type, &null_repr)
                            };
                            flowfile.set_attribute(
                                Arc::from(*attr_name),
                                Arc::from(value_str.as_str()),
                            );
                        }
                        Ok(_) => {
                            // No match for this expression — skip attribute.
                        }
                        Err(e) => {
                            tracing::warn!(
                                attribute = *attr_name,
                                error = %e,
                                "JSONPath evaluation error"
                            );
                            eval_failed = true;
                            break;
                        }
                    }
                }

                if eval_failed {
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            }

            if any_matched {
                session.transfer(flowfile, &REL_MATCHED);
            } else {
                session.transfer(flowfile, &REL_UNMATCHED);
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_MATCHED, REL_UNMATCHED, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_DESTINATION, PROP_RETURN_TYPE, PROP_NULL_VALUE]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "EvaluateJsonPath",
        description: "Extracts values from JSON content using JSONPath expressions",
        factory: || Box::new(EvaluateJsonPath::new()),
        tags: &["JSON", "Extraction"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            "test-evaluate-jsonpath"
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
        transferred: Vec<(FlowFile, &'static str)>,
        content: Vec<(u64, Bytes)>,
        next_id: u64,
    }

    impl TestSession {
        fn new(inputs: Vec<FlowFile>, content: Vec<(u64, Bytes)>) -> Self {
            let next_id = inputs.iter().map(|ff| ff.id).max().unwrap_or(0) + 100;
            Self {
                inputs,
                transferred: Vec::new(),
                content,
                next_id,
            }
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
            if let Some(claim) = &ff.content_claim {
                for (id, data) in &self.content {
                    if *id == claim.resource_id {
                        return Ok(data.clone());
                    }
                }
            }
            Ok(Bytes::new())
        }
        fn write_content(&mut self, mut ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            let id = self.next_id;
            self.next_id += 1;
            ff.size = data.len() as u64;
            ff.content_claim = Some(runifi_plugin_api::flowfile::ContentClaim {
                resource_id: id,
                offset: 0,
                length: data.len() as u64,
            });
            self.content.push((id, data));
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
            let mut cloned = ff.clone();
            cloned.id = self.next_id;
            self.next_id += 1;
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

    fn make_json_ff(id: u64, json: &str) -> (FlowFile, (u64, Bytes)) {
        let content = Bytes::from(json.to_string());
        let resource_id = id * 1000;
        let ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: Some(runifi_plugin_api::flowfile::ContentClaim {
                resource_id,
                offset: 0,
                length: content.len() as u64,
            }),
            size: content.len() as u64,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        (ff, (resource_id, content))
    }

    #[test]
    fn extracts_simple_attribute() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("Destination".to_string(), "flowfile-attribute".to_string()),
                ("user.name".to_string(), "$.name".to_string()),
            ],
        };

        let (ff, content) = make_json_ff(1, r#"{"name": "Alice", "age": 30}"#);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(
            ff.get_attribute("user.name").map(|v| v.as_ref()),
            Some("Alice")
        );
    }

    #[test]
    fn extracts_nested_path() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("city".to_string(), "$.address.city".to_string())],
        };

        let json = r#"{"address": {"city": "Portland", "state": "OR"}}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(
            ff.get_attribute("city").map(|v| v.as_ref()),
            Some("Portland")
        );
    }

    #[test]
    fn extracts_multiple_expressions() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("user.name".to_string(), "$.name".to_string()),
                ("user.age".to_string(), "$.age".to_string()),
                ("user.email".to_string(), "$.email".to_string()),
            ],
        };

        let json = r#"{"name": "Bob", "age": 25, "email": "bob@example.com"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(
            ff.get_attribute("user.name").map(|v| v.as_ref()),
            Some("Bob")
        );
        assert_eq!(ff.get_attribute("user.age").map(|v| v.as_ref()), Some("25"));
        assert_eq!(
            ff.get_attribute("user.email").map(|v| v.as_ref()),
            Some("bob@example.com")
        );
    }

    #[test]
    fn routes_unmatched_when_no_path_matches() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("missing".to_string(), "$.nonexistent".to_string())],
        };

        let json = r#"{"name": "Alice"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn routes_failure_on_invalid_json() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("field".to_string(), "$.name".to_string())],
        };

        let (ff, content) = make_json_ff(1, "this is not json{{{");
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn content_destination_replaces_content() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("Destination".to_string(), "flowfile-content".to_string()),
                ("result".to_string(), "$.name".to_string()),
            ],
        };

        let json = r#"{"name": "Alice", "age": 30}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");

        // Read the new content.
        let new_content = session.read_content(ff).unwrap();
        assert_eq!(std::str::from_utf8(&new_content).unwrap(), "Alice");
    }

    #[test]
    fn content_destination_unmatched() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("Destination".to_string(), "flowfile-content".to_string()),
                ("result".to_string(), "$.nonexistent".to_string()),
            ],
        };

        let json = r#"{"name": "Alice"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn extracts_array_values() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("first_item".to_string(), "$.items[0]".to_string())],
        };

        let json = r#"{"items": ["apple", "banana", "cherry"]}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(
            ff.get_attribute("first_item").map(|v| v.as_ref()),
            Some("apple")
        );
    }

    #[test]
    fn handles_null_value_representation() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                (
                    "Null Value Representation".to_string(),
                    "<NULL>".to_string(),
                ),
                ("nullable".to_string(), "$.value".to_string()),
            ],
        };

        let json = r#"{"value": null}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(
            ff.get_attribute("nullable").map(|v| v.as_ref()),
            Some("<NULL>")
        );
    }

    #[test]
    fn json_return_type_wraps_strings_in_quotes() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("Return Type".to_string(), "json".to_string()),
                ("name".to_string(), "$.name".to_string()),
            ],
        };

        let json = r#"{"name": "Alice"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        // JSON return type wraps string in quotes.
        assert_eq!(
            ff.get_attribute("name").map(|v| v.as_ref()),
            Some("\"Alice\"")
        );
    }

    #[test]
    fn extracts_object_as_json() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("addr".to_string(), "$.address".to_string())],
        };

        let json = r#"{"address": {"city": "Portland", "zip": "97201"}}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        // Auto mode returns JSON for objects.
        let attr_val = ff
            .get_attribute("addr")
            .map(|v| v.as_ref().to_string())
            .unwrap();
        let parsed: Value = serde_json::from_str(&attr_val).unwrap();
        assert_eq!(parsed["city"], "Portland");
        assert_eq!(parsed["zip"], "97201");
    }

    #[test]
    fn handles_boolean_and_number_values() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("active".to_string(), "$.active".to_string()),
                ("count".to_string(), "$.count".to_string()),
                ("ratio".to_string(), "$.ratio".to_string()),
            ],
        };

        let json = r#"{"active": true, "count": 42, "ratio": 3.14}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(ff.get_attribute("active").map(|v| v.as_ref()), Some("true"));
        assert_eq!(ff.get_attribute("count").map(|v| v.as_ref()), Some("42"));
        assert_eq!(ff.get_attribute("ratio").map(|v| v.as_ref()), Some("3.14"));
    }

    #[test]
    fn routes_failure_on_invalid_jsonpath() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                // Invalid JSONPath syntax
                ("bad".to_string(), "$[[[invalid".to_string()),
            ],
        };

        let json = r#"{"name": "Alice"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        // All FlowFiles should be routed to failure when expression is invalid.
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn partial_match_routes_to_matched() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![
                ("name".to_string(), "$.name".to_string()),
                ("missing".to_string(), "$.nonexistent".to_string()),
            ],
        };

        let json = r#"{"name": "Alice"}"#;
        let (ff, content) = make_json_ff(1, json);
        let mut session = TestSession::new(vec![ff], vec![content]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        let (ff, rel) = &session.transferred[0];
        assert_eq!(*rel, "matched");
        assert_eq!(ff.get_attribute("name").map(|v| v.as_ref()), Some("Alice"));
        // Missing path should not create an attribute.
        assert!(ff.get_attribute("missing").is_none());
    }

    #[test]
    fn processes_multiple_flowfiles() {
        let mut proc = EvaluateJsonPath::new();
        let ctx = TestContext {
            properties: vec![("name".to_string(), "$.name".to_string())],
        };

        let (ff1, c1) = make_json_ff(1, r#"{"name": "Alice"}"#);
        let (ff2, c2) = make_json_ff(2, r#"{"name": "Bob"}"#);
        let (ff3, c3) = make_json_ff(3, r#"{"other": "value"}"#);
        let mut session = TestSession::new(vec![ff1, ff2, ff3], vec![c1, c2, c3]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 3);
        assert_eq!(session.transferred[0].1, "matched");
        assert_eq!(session.transferred[1].1, "matched");
        assert_eq!(session.transferred[2].1, "unmatched");

        assert_eq!(
            session.transferred[0]
                .0
                .get_attribute("name")
                .map(|v| v.as_ref()),
            Some("Alice")
        );
        assert_eq!(
            session.transferred[1]
                .0
                .get_attribute("name")
                .map(|v| v.as_ref()),
            Some("Bob")
        );
    }

    #[test]
    fn returns_correct_relationships() {
        let proc = EvaluateJsonPath::new();
        let rels = proc.relationships();
        assert_eq!(rels.len(), 3);
        assert!(rels.iter().any(|r| r.name == "matched"));
        assert!(rels.iter().any(|r| r.name == "unmatched"));
        assert!(rels.iter().any(|r| r.name == "failure"));
    }

    #[test]
    fn returns_correct_property_descriptors() {
        let proc = EvaluateJsonPath::new();
        let props = proc.property_descriptors();
        assert_eq!(props.len(), 3);
        assert!(props.iter().any(|p| p.name == "Destination"));
        assert!(props.iter().any(|p| p.name == "Return Type"));
        assert!(props.iter().any(|p| p.name == "Null Value Representation"));
    }
}
