//! UpdateRecord processor — modifies record fields within FlowFile content.
//!
//! Reads records from a FlowFile, applies field-level modifications configured
//! as dynamic properties, and writes the modified records back. Supports
//! setting, removing, and renaming fields.

use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::record::{RecordReader, RecordValue, RecordWriter};
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

use super::csv_reader::CsvRecordReader;
use super::csv_writer::CsvRecordWriter;
use super::json_reader::JsonRecordReader;
use super::json_writer::{JsonOutputFormat, JsonRecordWriter};

const PROP_FORMAT: PropertyDescriptor = PropertyDescriptor::new(
    "Record Format",
    "Data format: 'json', 'json-lines', 'csv', 'tsv'",
)
.required()
.default_value("json")
.allowed_values(&["json", "json-lines", "csv", "tsv"]);

const PROP_REMOVE_FIELDS: PropertyDescriptor = PropertyDescriptor::new(
    "Remove Fields",
    "Comma-separated list of field names to remove from each record",
);

/// The set of property names owned by this processor (not treated as field updates).
const OWN_PROPERTY_NAMES: &[&str] = &["Record Format", "Remove Fields"];

/// Modifies record fields within FlowFile content.
///
/// Dynamic properties (any property not in the processor's own descriptor list)
/// are treated as field assignments: property name = field name, property value = new value.
///
/// The "Remove Fields" property specifies a comma-separated list of field names
/// to remove from each record.
///
/// All updates are applied in-place: the FlowFile retains its identity but gets
/// updated content with the modified records.
pub struct UpdateRecord;

impl UpdateRecord {
    pub fn new() -> Self {
        Self
    }
}

impl Default for UpdateRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for UpdateRecord {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let format = context
            .get_property("Record Format")
            .unwrap_or("json")
            .to_string();

        let remove_fields: Vec<String> = context
            .get_property("Remove Fields")
            .as_str()
            .map(|s| {
                s.split(',')
                    .map(|f| f.trim().to_string())
                    .filter(|f| !f.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        // Collect field assignments from dynamic properties.
        let all_names = context.property_names();
        let field_updates: Vec<(String, String)> = all_names
            .into_iter()
            .filter(|name| !OWN_PROPERTY_NAMES.contains(&name.as_str()))
            .filter_map(|name| {
                context
                    .get_property(&name)
                    .as_str()
                    .map(|v| (name, v.to_string()))
            })
            .collect();

        let reader = build_reader(&format);
        let writer = build_writer(&format);

        while let Some(flowfile) = session.get() {
            let content = session.read_content(&flowfile)?;

            match reader.read_records(&content, None) {
                Ok(mut records) => {
                    // Apply modifications to each record.
                    for record in &mut records {
                        // Set/update fields.
                        for (field_name, value_str) in &field_updates {
                            let value = parse_value_string(value_str);
                            record.set(field_name.clone(), value);
                        }
                        // Remove fields.
                        for field_name in &remove_fields {
                            record.remove(field_name);
                        }
                    }

                    let record_count = records.len();
                    match writer.write_records(&records, None) {
                        Ok(output) => {
                            let mut ff = session.write_content(flowfile, Bytes::from(output))?;
                            ff.set_attribute(
                                Arc::from("record.count"),
                                Arc::from(record_count.to_string().as_str()),
                            );
                            session.transfer(ff, &REL_SUCCESS);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to write updated records");
                            session.transfer(flowfile, &REL_FAILURE);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to read records for update");
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
        vec![PROP_FORMAT, PROP_REMOVE_FIELDS]
    }
}

/// Parse a string property value into a [`RecordValue`].
///
/// Attempts numeric and boolean parsing; falls back to string.
fn parse_value_string(s: &str) -> RecordValue {
    if s.eq_ignore_ascii_case("null") {
        return RecordValue::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return RecordValue::Int(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return RecordValue::Float(f);
    }
    match s.to_lowercase().as_str() {
        "true" => RecordValue::Boolean(true),
        "false" => RecordValue::Boolean(false),
        _ => RecordValue::String(s.to_string()),
    }
}

fn build_reader(format: &str) -> Box<dyn RecordReader> {
    match format {
        "csv" => Box::new(CsvRecordReader::new()),
        "tsv" => Box::new(CsvRecordReader::new().delimiter(b'\t')),
        "json-lines" => Box::new(JsonRecordReader::new()),
        _ => Box::new(JsonRecordReader::new()),
    }
}

fn build_writer(format: &str) -> Box<dyn RecordWriter> {
    match format {
        "csv" => Box::new(CsvRecordWriter::new()),
        "tsv" => Box::new(CsvRecordWriter::new().delimiter(b'\t')),
        "json-lines" => Box::new(JsonRecordWriter::new(JsonOutputFormat::LineDelimited)),
        _ => Box::new(JsonRecordWriter::new(JsonOutputFormat::Array)),
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "UpdateRecord",
        description: "Modifies record fields within FlowFile content using dynamic properties",
        factory: || Box::new(UpdateRecord::new()),
        tags: &["Record", "Transformation", "Update"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::result::PluginError;

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
            "test-update-record"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct UpdateRecordSession {
        inputs: Vec<FlowFile>,
        content: std::collections::HashMap<u64, Bytes>,
        transferred: Vec<(FlowFile, &'static str)>,
        next_id: u64,
    }

    impl UpdateRecordSession {
        fn new() -> Self {
            Self {
                inputs: Vec::new(),
                content: std::collections::HashMap::new(),
                transferred: Vec::new(),
                next_id: 100,
            }
        }

        fn add_flowfile_with_content(&mut self, data: &[u8]) {
            let id = self.next_id;
            self.next_id += 1;
            let ff = FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: data.len() as u64,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            };
            self.content.insert(id, Bytes::from(data.to_vec()));
            self.inputs.push(ff);
        }
    }

    impl ProcessSession for UpdateRecordSession {
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
                .ok_or(PluginError::ContentNotFound(ff.id))
        }
        fn write_content(&mut self, mut ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            ff.size = data.len() as u64;
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
            if let Some(content) = self.content.get(&ff.id) {
                self.content.insert(id, content.clone());
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

    #[test]
    fn adds_field_to_records() {
        let mut proc = UpdateRecord::new();
        let ctx = TestContext {
            properties: vec![
                ("Record Format".to_string(), "json".to_string()),
                ("status".to_string(), "processed".to_string()),
            ],
        };

        let mut session = UpdateRecordSession::new();
        session.add_flowfile_with_content(br#"[{"name":"Alice"}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "success");
        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(output).unwrap();
        assert_eq!(parsed[0]["status"], "processed");
        assert_eq!(parsed[0]["name"], "Alice");
    }

    #[test]
    fn removes_fields() {
        let mut proc = UpdateRecord::new();
        let ctx = TestContext {
            properties: vec![
                ("Record Format".to_string(), "json".to_string()),
                ("Remove Fields".to_string(), "secret, temp".to_string()),
            ],
        };

        let mut session = UpdateRecordSession::new();
        session.add_flowfile_with_content(br#"[{"name":"Alice","secret":"xxx","temp":"yyy"}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(output).unwrap();
        assert_eq!(parsed[0]["name"], "Alice");
        assert!(parsed[0].get("secret").is_none());
        assert!(parsed[0].get("temp").is_none());
    }

    #[test]
    fn updates_existing_field() {
        let mut proc = UpdateRecord::new();
        let ctx = TestContext {
            properties: vec![
                ("Record Format".to_string(), "json".to_string()),
                ("age".to_string(), "99".to_string()),
            ],
        };

        let mut session = UpdateRecordSession::new();
        session.add_flowfile_with_content(br#"[{"name":"Alice","age":30}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(output).unwrap();
        assert_eq!(parsed[0]["age"], 99);
    }

    #[test]
    fn sets_null_value() {
        let mut proc = UpdateRecord::new();
        let ctx = TestContext {
            properties: vec![
                ("Record Format".to_string(), "json".to_string()),
                ("field".to_string(), "null".to_string()),
            ],
        };

        let mut session = UpdateRecordSession::new();
        session.add_flowfile_with_content(br#"[{"field":"value"}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(output).unwrap();
        assert!(parsed[0]["field"].is_null());
    }

    #[test]
    fn parse_value_string_types() {
        assert_eq!(parse_value_string("42"), RecordValue::Int(42));
        assert_eq!(parse_value_string("3.14"), RecordValue::Float(3.14));
        assert_eq!(parse_value_string("true"), RecordValue::Boolean(true));
        assert_eq!(parse_value_string("false"), RecordValue::Boolean(false));
        assert_eq!(parse_value_string("null"), RecordValue::Null);
        assert_eq!(
            parse_value_string("hello"),
            RecordValue::String("hello".to_string())
        );
    }
}
