use std::sync::Arc;

use runifi_plugin_api::REL_FAILURE;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

const REL_VALID: Relationship = Relationship::new("valid", "FlowFiles that pass schema validation");
const REL_INVALID: Relationship =
    Relationship::new("invalid", "FlowFiles that fail schema validation");

const PROP_SCHEMA_CONTENT: PropertyDescriptor = PropertyDescriptor::new(
    "Schema Content",
    "Inline JSON Schema definition. Exactly one of 'Schema Content' or 'Schema File' must be set.",
);

const PROP_SCHEMA_FILE: PropertyDescriptor = PropertyDescriptor::new(
    "Schema File",
    "Path to a JSON Schema file. Exactly one of 'Schema Content' or 'Schema File' must be set.",
);

const PROP_SCHEMA_VERSION: PropertyDescriptor = PropertyDescriptor::new(
    "Schema Version",
    "JSON Schema draft version to use for validation",
)
.default_value("draft-07")
.allowed_values(&["draft-07", "2019-09", "2020-12"]);

/// Validates FlowFile JSON content against a JSON Schema.
///
/// FlowFiles whose content passes validation are routed to "valid".
/// FlowFiles whose content fails validation are routed to "invalid" with
/// validation error details stored in attributes.
/// FlowFiles that cannot be parsed as JSON or whose schema cannot be loaded
/// are routed to "failure".
pub struct ValidateJson;

impl ValidateJson {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ValidateJson {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for ValidateJson {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let schema_content = context.get_property("Schema Content");
        let schema_file = context.get_property("Schema File");

        // Load the schema JSON. Exactly one of Schema Content or Schema File must be set.
        let schema_json = match (schema_content.as_str(), schema_file.as_str()) {
            (Some(content), None) => match serde_json::from_str::<serde_json::Value>(content) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to parse inline schema JSON");
                    // Route all pending FlowFiles to failure.
                    while let Some(mut flowfile) = session.get() {
                        flowfile.set_attribute(
                            Arc::from("validation.error.message"),
                            Arc::from(format!("Failed to parse inline schema: {}", e).as_str()),
                        );
                        session.transfer(flowfile, &REL_FAILURE);
                    }
                    session.commit();
                    return Ok(());
                }
            },
            (None, Some(path)) => match std::fs::read_to_string(path) {
                Ok(content) => match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(path = path, error = %e, "Failed to parse schema file JSON");
                        while let Some(mut flowfile) = session.get() {
                            flowfile.set_attribute(
                                Arc::from("validation.error.message"),
                                Arc::from(
                                    format!("Failed to parse schema file '{}': {}", path, e)
                                        .as_str(),
                                ),
                            );
                            session.transfer(flowfile, &REL_FAILURE);
                        }
                        session.commit();
                        return Ok(());
                    }
                },
                Err(e) => {
                    tracing::error!(path = path, error = %e, "Failed to read schema file");
                    while let Some(mut flowfile) = session.get() {
                        flowfile.set_attribute(
                            Arc::from("validation.error.message"),
                            Arc::from(
                                format!("Failed to read schema file '{}': {}", path, e).as_str(),
                            ),
                        );
                        session.transfer(flowfile, &REL_FAILURE);
                    }
                    session.commit();
                    return Ok(());
                }
            },
            (Some(_), Some(_)) => {
                tracing::error!(
                    "Both 'Schema Content' and 'Schema File' are set; exactly one must be specified"
                );
                while let Some(mut flowfile) = session.get() {
                    flowfile.set_attribute(
                        Arc::from("validation.error.message"),
                        Arc::from(
                            "Both 'Schema Content' and 'Schema File' are set; exactly one must be specified",
                        ),
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                }
                session.commit();
                return Ok(());
            }
            (None, None) => {
                tracing::error!(
                    "Neither 'Schema Content' nor 'Schema File' is set; exactly one must be specified"
                );
                while let Some(mut flowfile) = session.get() {
                    flowfile.set_attribute(
                        Arc::from("validation.error.message"),
                        Arc::from(
                            "Neither 'Schema Content' nor 'Schema File' is set; exactly one must be specified",
                        ),
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                }
                session.commit();
                return Ok(());
            }
        };

        // Compile the validator for the specified draft version.
        let schema_version = context
            .get_property("Schema Version")
            .unwrap_or("draft-07")
            .to_string();

        let validator = match schema_version.as_str() {
            "2019-09" => jsonschema::draft201909::new(&schema_json),
            "2020-12" => jsonschema::draft202012::new(&schema_json),
            _ => jsonschema::draft7::new(&schema_json),
        };

        let validator = match validator {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "Failed to compile JSON Schema");
                while let Some(mut flowfile) = session.get() {
                    flowfile.set_attribute(
                        Arc::from("validation.error.message"),
                        Arc::from(format!("Failed to compile JSON Schema: {}", e).as_str()),
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                }
                session.commit();
                return Ok(());
            }
        };

        // Process each FlowFile.
        while let Some(flowfile) = session.get() {
            let content = match session.read_content(&flowfile) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!(flowfile_id = flowfile.id, error = %e, "Failed to read FlowFile content");
                    let mut ff = flowfile;
                    ff.set_attribute(
                        Arc::from("validation.error.message"),
                        Arc::from(format!("Failed to read content: {}", e).as_str()),
                    );
                    session.transfer(ff, &REL_FAILURE);
                    continue;
                }
            };

            // Parse the FlowFile content as JSON.
            let instance: serde_json::Value = match serde_json::from_slice(&content) {
                Ok(v) => v,
                Err(e) => {
                    let mut ff = flowfile;
                    ff.set_attribute(
                        Arc::from("validation.error.message"),
                        Arc::from(format!("Content is not valid JSON: {}", e).as_str()),
                    );
                    ff.set_attribute(Arc::from("validation.error.count"), Arc::from("1"));
                    session.transfer(ff, &REL_FAILURE);
                    continue;
                }
            };

            // Validate the JSON against the schema.
            let errors: Vec<String> = validator
                .iter_errors(&instance)
                .map(|e| format!("{} at {}", e, e.instance_path()))
                .collect();

            if errors.is_empty() {
                session.transfer(flowfile, &REL_VALID);
            } else {
                let mut ff = flowfile;
                let error_count = errors.len();
                ff.set_attribute(
                    Arc::from("validation.error.count"),
                    Arc::from(error_count.to_string().as_str()),
                );
                ff.set_attribute(
                    Arc::from("validation.error.message"),
                    Arc::from(errors[0].as_str()),
                );
                // Build a JSON array of all error messages.
                let errors_json = serde_json::to_string(&errors).unwrap_or_else(|_| "[]".into());
                ff.set_attribute(
                    Arc::from("validation.errors"),
                    Arc::from(errors_json.as_str()),
                );
                session.transfer(ff, &REL_INVALID);
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_VALID, REL_INVALID, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_SCHEMA_CONTENT, PROP_SCHEMA_FILE, PROP_SCHEMA_VERSION]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "ValidateJson",
        description: "Validates FlowFile JSON content against a JSON Schema",
        factory: || Box::new(ValidateJson::new()),
        tags: &["JSON", "Validation", "Schema"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use std::io::Write;

    struct TestContext {
        schema_content: Option<String>,
        schema_file: Option<String>,
        schema_version: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Schema Content" => match &self.schema_content {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
                "Schema File" => match &self.schema_file {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
                "Schema Version" => PropertyValue::String(self.schema_version.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-validate-json"
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
        content_map: Vec<(u64, Bytes)>,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl TestSession {
        fn new(inputs: Vec<(FlowFile, Bytes)>) -> Self {
            let content_map: Vec<(u64, Bytes)> = inputs
                .iter()
                .map(|(ff, data)| (ff.id, data.clone()))
                .collect();
            let inputs: Vec<FlowFile> = inputs.into_iter().map(|(ff, _)| ff).collect();
            Self {
                inputs,
                content_map,
                transferred: Vec::new(),
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
            self.content_map
                .iter()
                .find(|(id, _)| *id == ff.id)
                .map(|(_, data)| data.clone())
                .ok_or(runifi_plugin_api::PluginError::ContentNotFound(ff.id))
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

    fn make_ff(id: u64) -> FlowFile {
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

    const TEST_SCHEMA: &str = r#"{
        "type": "object",
        "required": ["name", "age"],
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer", "minimum": 0 }
        }
    }"#;

    #[test]
    fn valid_json_routes_to_valid() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Alice", "age": 30}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "valid");
    }

    #[test]
    fn invalid_json_routes_to_invalid_with_error_attrs() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        // Missing required "age" field and "name" has wrong type.
        let invalid_json = r#"{"name": 123}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(invalid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "invalid");

        let ff = &session.transferred[0].0;
        // Should have error count attribute.
        let error_count = ff.get_attribute("validation.error.count").unwrap();
        let count: usize = error_count.parse().unwrap();
        assert!(count >= 1, "Expected at least 1 validation error");

        // Should have error message.
        assert!(ff.get_attribute("validation.error.message").is_some());

        // Should have errors JSON array.
        assert!(ff.get_attribute("validation.errors").is_some());
    }

    #[test]
    fn not_json_routes_to_failure() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        let not_json = "this is not json";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(not_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
        assert!(
            session.transferred[0]
                .0
                .get_attribute("validation.error.message")
                .is_some()
        );
    }

    #[test]
    fn no_schema_routes_to_failure() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: None,
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Alice"}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn both_schemas_routes_to_failure() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: Some("/tmp/schema.json".to_string()),
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Alice"}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn schema_file_loading() {
        // Write a temporary schema file.
        let dir = std::env::temp_dir().join("runifi-test-validate-json");
        std::fs::create_dir_all(&dir).unwrap();
        let schema_path = dir.join("test-schema.json");
        let mut f = std::fs::File::create(&schema_path).unwrap();
        f.write_all(TEST_SCHEMA.as_bytes()).unwrap();

        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: None,
            schema_file: Some(schema_path.to_str().unwrap().to_string()),
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Bob", "age": 25}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "valid");

        // Clean up.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_schema_file_routes_to_failure() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: None,
            schema_file: Some("/nonexistent/path/schema.json".to_string()),
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Alice"}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn invalid_inline_schema_routes_to_failure() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some("not valid json".to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        let valid_json = r#"{"name": "Alice"}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn schema_version_2019_09() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "2019-09".to_string(),
        };

        let valid_json = r#"{"name": "Alice", "age": 30}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "valid");
    }

    #[test]
    fn schema_version_2020_12() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "2020-12".to_string(),
        };

        let valid_json = r#"{"name": "Alice", "age": 30}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(valid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "valid");
    }

    #[test]
    fn multiple_validation_errors() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        // Wrong type for name, negative age, both violate schema.
        let invalid_json = r#"{"name": 123, "age": -5}"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(invalid_json))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "invalid");

        let ff = &session.transferred[0].0;
        let error_count: usize = ff
            .get_attribute("validation.error.count")
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            error_count >= 2,
            "Expected at least 2 validation errors, got {}",
            error_count
        );

        // Verify errors is a JSON array.
        let errors_json = ff.get_attribute("validation.errors").unwrap();
        let errors: Vec<String> = serde_json::from_str(errors_json.as_ref()).unwrap();
        assert_eq!(errors.len(), error_count);
    }

    #[test]
    fn multiple_flowfiles_mixed_results() {
        let mut proc = ValidateJson::new();
        let ctx = TestContext {
            schema_content: Some(TEST_SCHEMA.to_string()),
            schema_file: None,
            schema_version: "draft-07".to_string(),
        };

        let inputs = vec![
            (make_ff(1), Bytes::from(r#"{"name": "Alice", "age": 30}"#)),
            (make_ff(2), Bytes::from(r#"{"name": 123}"#)),
            (make_ff(3), Bytes::from("not json")),
            (make_ff(4), Bytes::from(r#"{"name": "Bob", "age": 1}"#)),
        ];
        let mut session = TestSession::new(inputs);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 4);
        assert_eq!(session.transferred[0].1, "valid"); // Valid JSON.
        assert_eq!(session.transferred[1].1, "invalid"); // Schema violation.
        assert_eq!(session.transferred[2].1, "failure"); // Not JSON.
        assert_eq!(session.transferred[3].1, "valid"); // Valid JSON.
    }

    #[test]
    fn property_descriptors_are_correct() {
        let proc = ValidateJson::new();
        let descriptors = proc.property_descriptors();
        assert_eq!(descriptors.len(), 3);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Schema Content"));
        assert!(names.contains(&"Schema File"));
        assert!(names.contains(&"Schema Version"));

        // Schema Version should have a default and allowed values.
        let version_desc = descriptors
            .iter()
            .find(|d| d.name == "Schema Version")
            .unwrap();
        assert_eq!(version_desc.default_value, Some("draft-07"));
        assert!(version_desc.allowed_values.is_some());
        let allowed = version_desc.allowed_values.unwrap();
        assert!(allowed.contains(&"draft-07"));
        assert!(allowed.contains(&"2019-09"));
        assert!(allowed.contains(&"2020-12"));
    }

    #[test]
    fn relationships_are_correct() {
        let proc = ValidateJson::new();
        let rels = proc.relationships();
        assert_eq!(rels.len(), 3);

        let names: Vec<&str> = rels.iter().map(|r| r.name).collect();
        assert!(names.contains(&"valid"));
        assert!(names.contains(&"invalid"));
        assert!(names.contains(&"failure"));
    }
}
