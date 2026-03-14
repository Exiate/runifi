//! ConvertRecord processor — reads FlowFile content with one format and writes
//! with another (e.g., CSV -> JSON, JSON -> CSV).

use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::record::{RecordReader, RecordWriter};
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

use super::csv_reader::CsvRecordReader;
use super::csv_writer::CsvRecordWriter;
use super::json_reader::JsonRecordReader;
use super::json_writer::{JsonOutputFormat, JsonRecordWriter};

const PROP_READER_FORMAT: PropertyDescriptor = PropertyDescriptor::new(
    "Record Reader",
    "Input format: 'json', 'json-lines', 'csv', 'tsv'",
)
.required()
.default_value("json")
.allowed_values(&["json", "json-lines", "csv", "tsv"]);

const PROP_WRITER_FORMAT: PropertyDescriptor = PropertyDescriptor::new(
    "Record Writer",
    "Output format: 'json', 'json-lines', 'csv', 'tsv'",
)
.required()
.default_value("json")
.allowed_values(&["json", "json-lines", "csv", "tsv"]);

const PROP_CSV_HEADER: PropertyDescriptor = PropertyDescriptor::new(
    "Include Header",
    "Whether CSV/TSV output includes a header row",
)
.default_value("true")
.allowed_values(&["true", "false"]);

/// Converts FlowFile content between record formats.
///
/// Reads the incoming FlowFile using the configured reader format, then
/// writes the records using the configured writer format. This enables
/// format conversion (e.g., CSV to JSON) without per-record FlowFile overhead.
pub struct ConvertRecord;

impl ConvertRecord {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConvertRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for ConvertRecord {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let reader_format = context
            .get_property("Record Reader")
            .unwrap_or("json")
            .to_string();
        let writer_format = context
            .get_property("Record Writer")
            .unwrap_or("json")
            .to_string();
        let include_header = context.get_property("Include Header").unwrap_or("true") == "true";

        let reader = build_reader(&reader_format);
        let writer = build_writer(&writer_format, include_header);

        while let Some(flowfile) = session.get() {
            let content = session.read_content(&flowfile)?;

            match reader.read_records(&content, None) {
                Ok(records) => {
                    let record_count = records.len();
                    match writer.write_records(&records, None) {
                        Ok(output) => {
                            let mut ff = session.write_content(flowfile, Bytes::from(output))?;
                            ff.set_attribute(
                                Arc::from("record.count"),
                                Arc::from(record_count.to_string().as_str()),
                            );
                            ff.set_attribute(
                                Arc::from("mime.type"),
                                Arc::from(mime_type_for_format(&writer_format)),
                            );
                            session.transfer(ff, &REL_SUCCESS);
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                format = writer_format,
                                "Failed to write records"
                            );
                            session.transfer(flowfile, &REL_FAILURE);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        format = reader_format,
                        "Failed to read records"
                    );
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
        vec![PROP_READER_FORMAT, PROP_WRITER_FORMAT, PROP_CSV_HEADER]
    }
}

fn build_reader(format: &str) -> Box<dyn RecordReader> {
    match format {
        "csv" => Box::new(CsvRecordReader::new()),
        "tsv" => Box::new(CsvRecordReader::new().delimiter(b'\t')),
        "json-lines" => Box::new(JsonRecordReader::new()),
        _ => Box::new(JsonRecordReader::new()), // "json" and default
    }
}

fn build_writer(format: &str, include_header: bool) -> Box<dyn RecordWriter> {
    match format {
        "csv" => Box::new(CsvRecordWriter::new().write_header(include_header)),
        "tsv" => Box::new(
            CsvRecordWriter::new()
                .delimiter(b'\t')
                .write_header(include_header),
        ),
        "json-lines" => Box::new(JsonRecordWriter::new(JsonOutputFormat::LineDelimited)),
        _ => Box::new(JsonRecordWriter::new(JsonOutputFormat::Array)), // "json" and default
    }
}

fn mime_type_for_format(format: &str) -> &'static str {
    match format {
        "json" | "json-lines" => "application/json",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        _ => "application/octet-stream",
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "ConvertRecord",
        description: "Converts FlowFile content between record formats (CSV, JSON, TSV)",
        factory: || Box::new(ConvertRecord::new()),
        tags: &["Record", "Transformation", "Convert"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::result::PluginError;

    struct TestContext {
        reader_format: String,
        writer_format: String,
        include_header: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Record Reader" => PropertyValue::String(self.reader_format.clone()),
                "Record Writer" => PropertyValue::String(self.writer_format.clone()),
                "Include Header" => PropertyValue::String(self.include_header.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-convert"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct ConvertSession {
        inputs: Vec<FlowFile>,
        content: std::collections::HashMap<u64, Bytes>,
        transferred: Vec<(FlowFile, &'static str)>,
        next_id: u64,
    }

    impl ConvertSession {
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

    impl ProcessSession for ConvertSession {
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
    fn converts_csv_to_json() {
        let mut proc = ConvertRecord::new();
        let ctx = TestContext {
            reader_format: "csv".to_string(),
            writer_format: "json".to_string(),
            include_header: "true".to_string(),
        };

        let mut session = ConvertSession::new();
        session.add_flowfile_with_content(b"name,age\nAlice,30\nBob,25\n");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(output).unwrap();

        assert!(parsed.is_array());
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // Check record count attribute.
        assert_eq!(
            ff.get_attribute("record.count").map(|v| v.as_ref()),
            Some("2")
        );
    }

    #[test]
    fn converts_json_to_csv() {
        let mut proc = ConvertRecord::new();
        let ctx = TestContext {
            reader_format: "json".to_string(),
            writer_format: "csv".to_string(),
            include_header: "true".to_string(),
        };

        let mut session = ConvertSession::new();
        session
            .add_flowfile_with_content(br#"[{"name":"Alice","age":30},{"name":"Bob","age":25}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let text = std::str::from_utf8(output).unwrap();

        // Should have header + 2 rows.
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(
            ff.get_attribute("mime.type").map(|v| v.as_ref()),
            Some("text/csv")
        );
    }

    #[test]
    fn converts_json_to_ndjson() {
        let mut proc = ConvertRecord::new();
        let ctx = TestContext {
            reader_format: "json".to_string(),
            writer_format: "json-lines".to_string(),
            include_header: "true".to_string(),
        };

        let mut session = ConvertSession::new();
        session.add_flowfile_with_content(br#"[{"x":1},{"x":2}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "success");
        let ff = &session.transferred[0].0;
        let output = session.content.get(&ff.id).unwrap();
        let text = std::str::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn invalid_input_goes_to_failure() {
        let mut proc = ConvertRecord::new();
        let ctx = TestContext {
            reader_format: "json".to_string(),
            writer_format: "csv".to_string(),
            include_header: "true".to_string(),
        };

        let mut session = ConvertSession::new();
        session.add_flowfile_with_content(b"this is not json");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }
}
