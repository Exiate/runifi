//! PartitionRecord processor — splits records into groups by field value.
//!
//! Reads records from a FlowFile, groups them by the value of a specified field,
//! and creates one output FlowFile per group. Each output FlowFile's content
//! contains only the records belonging to that group.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::record::{Record, RecordReader, RecordValue, RecordWriter};
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
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

const PROP_PARTITION_FIELD: PropertyDescriptor = PropertyDescriptor::new(
    "Partition Field",
    "The record field to partition by. Records with the same value in this field are grouped together.",
)
.required();

/// Partitions records by a field value, creating one FlowFile per group.
///
/// For example, if the partition field is "department", a FlowFile containing:
/// ```json
/// [{"name":"Alice","department":"eng"},{"name":"Bob","department":"sales"},{"name":"Carol","department":"eng"}]
/// ```
/// produces two FlowFiles:
/// - One with `[{"name":"Alice","department":"eng"},{"name":"Carol","department":"eng"}]`
///   and attribute `partition.value=eng`
/// - One with `[{"name":"Bob","department":"sales"}]`
///   and attribute `partition.value=sales`
pub struct PartitionRecord;

impl PartitionRecord {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PartitionRecord {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for PartitionRecord {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let format = context
            .get_property("Record Format")
            .unwrap_or("json")
            .to_string();
        let partition_field = context
            .get_property("Partition Field")
            .as_str()
            .ok_or(PluginError::PropertyRequired("Partition Field"))?
            .to_string();

        let reader = build_reader(&format);
        let writer = build_writer(&format);

        while let Some(flowfile) = session.get() {
            let content = session.read_content(&flowfile)?;

            match reader.read_records(&content, None) {
                Ok(records) => {
                    // Group records by partition field value.
                    let mut groups: BTreeMap<String, Vec<Record>> = BTreeMap::new();

                    for record in records {
                        let key = record
                            .get(&partition_field)
                            .map(record_value_to_partition_key)
                            .unwrap_or_else(|| "__null__".to_string());
                        groups.entry(key).or_default().push(record);
                    }

                    // Create one FlowFile per group.
                    for (partition_value, group_records) in &groups {
                        let record_count = group_records.len();
                        match writer.write_records(group_records, None) {
                            Ok(output) => {
                                let mut new_ff = session.create();
                                // Copy attributes from the original FlowFile.
                                for (key, value) in &flowfile.attributes {
                                    new_ff.set_attribute(key.clone(), value.clone());
                                }
                                new_ff = session.write_content(new_ff, Bytes::from(output))?;
                                new_ff.set_attribute(
                                    Arc::from("partition.field"),
                                    Arc::from(partition_field.as_str()),
                                );
                                new_ff.set_attribute(
                                    Arc::from("partition.value"),
                                    Arc::from(partition_value.as_str()),
                                );
                                new_ff.set_attribute(
                                    Arc::from("record.count"),
                                    Arc::from(record_count.to_string().as_str()),
                                );
                                session.transfer(new_ff, &REL_SUCCESS);
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    partition = partition_value,
                                    "Failed to write partition records"
                                );
                                session.transfer(flowfile, &REL_FAILURE);
                                session.commit();
                                return Ok(());
                            }
                        }
                    }

                    // Remove the original FlowFile (it has been replaced by partitions).
                    session.remove(flowfile);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to read records for partitioning");
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
        vec![PROP_FORMAT, PROP_PARTITION_FIELD]
    }
}

/// Convert a [`RecordValue`] to a string key for partitioning.
fn record_value_to_partition_key(value: &RecordValue) -> String {
    match value {
        RecordValue::Null => "__null__".to_string(),
        RecordValue::String(s) => s.clone(),
        RecordValue::Int(n) => n.to_string(),
        RecordValue::Float(f) => f.to_string(),
        RecordValue::Boolean(b) => b.to_string(),
        RecordValue::Array(_) => "__array__".to_string(),
        RecordValue::Record(_) => "__record__".to_string(),
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
        type_name: "PartitionRecord",
        description: "Splits records into groups by field value, creating one FlowFile per group",
        factory: || Box::new(PartitionRecord::new()),
        tags: &["Record", "Routing", "Partition"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        format: String,
        partition_field: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Record Format" => PropertyValue::String(self.format.clone()),
                "Partition Field" => PropertyValue::String(self.partition_field.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-partition"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct PartitionSession {
        inputs: Vec<FlowFile>,
        content: std::collections::HashMap<u64, Bytes>,
        transferred: Vec<(FlowFile, &'static str)>,
        removed: Vec<u64>,
        next_id: u64,
    }

    impl PartitionSession {
        fn new() -> Self {
            Self {
                inputs: Vec::new(),
                content: std::collections::HashMap::new(),
                transferred: Vec::new(),
                removed: Vec::new(),
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

    impl ProcessSession for PartitionSession {
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
        fn remove(&mut self, ff: FlowFile) {
            self.removed.push(ff.id);
        }
        fn penalize(&mut self, ff: FlowFile) -> FlowFile {
            ff
        }
        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    #[test]
    fn partitions_by_field() {
        let mut proc = PartitionRecord::new();
        let ctx = TestContext {
            format: "json".to_string(),
            partition_field: "dept".to_string(),
        };

        let mut session = PartitionSession::new();
        session.add_flowfile_with_content(
            br#"[{"name":"Alice","dept":"eng"},{"name":"Bob","dept":"sales"},{"name":"Carol","dept":"eng"}]"#,
        );

        proc.on_trigger(&ctx, &mut session).unwrap();

        // Should produce 2 FlowFiles (eng, sales).
        let success: Vec<_> = session
            .transferred
            .iter()
            .filter(|(_, rel)| *rel == "success")
            .collect();
        assert_eq!(success.len(), 2);

        // Check partition attributes.
        let mut partition_values: Vec<String> = success
            .iter()
            .map(|(ff, _)| {
                ff.get_attribute("partition.value")
                    .unwrap()
                    .as_ref()
                    .to_string()
            })
            .collect();
        partition_values.sort();
        assert_eq!(partition_values, vec!["eng", "sales"]);

        // Check record counts.
        let eng = success
            .iter()
            .find(|(ff, _)| ff.get_attribute("partition.value").unwrap().as_ref() == "eng")
            .unwrap();
        assert_eq!(eng.0.get_attribute("record.count").unwrap().as_ref(), "2");

        let sales = success
            .iter()
            .find(|(ff, _)| ff.get_attribute("partition.value").unwrap().as_ref() == "sales")
            .unwrap();
        assert_eq!(sales.0.get_attribute("record.count").unwrap().as_ref(), "1");

        // Original should be removed.
        assert_eq!(session.removed.len(), 1);
    }

    #[test]
    fn handles_missing_partition_field() {
        let mut proc = PartitionRecord::new();
        let ctx = TestContext {
            format: "json".to_string(),
            partition_field: "missing_field".to_string(),
        };

        let mut session = PartitionSession::new();
        session.add_flowfile_with_content(br#"[{"a":1},{"a":2}]"#);

        proc.on_trigger(&ctx, &mut session).unwrap();

        // All records should go into the "__null__" partition.
        let success: Vec<_> = session
            .transferred
            .iter()
            .filter(|(_, rel)| *rel == "success")
            .collect();
        assert_eq!(success.len(), 1);
        assert_eq!(
            success[0]
                .0
                .get_attribute("partition.value")
                .unwrap()
                .as_ref(),
            "__null__"
        );
    }

    #[test]
    fn partitions_numeric_field() {
        let mut proc = PartitionRecord::new();
        let ctx = TestContext {
            format: "json".to_string(),
            partition_field: "level".to_string(),
        };

        let mut session = PartitionSession::new();
        session.add_flowfile_with_content(
            br#"[{"msg":"a","level":1},{"msg":"b","level":2},{"msg":"c","level":1}]"#,
        );

        proc.on_trigger(&ctx, &mut session).unwrap();

        let success: Vec<_> = session
            .transferred
            .iter()
            .filter(|(_, rel)| *rel == "success")
            .collect();
        assert_eq!(success.len(), 2);
    }

    #[test]
    fn invalid_input_goes_to_failure() {
        let mut proc = PartitionRecord::new();
        let ctx = TestContext {
            format: "json".to_string(),
            partition_field: "x".to_string(),
        };

        let mut session = PartitionSession::new();
        session.add_flowfile_with_content(b"not json");

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn copies_original_attributes() {
        let mut proc = PartitionRecord::new();
        let ctx = TestContext {
            format: "json".to_string(),
            partition_field: "type".to_string(),
        };

        let mut session = PartitionSession::new();
        let id = session.next_id;
        session.next_id += 1;
        let mut ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from("source"), Arc::from("test-source"));
        let data = br#"[{"type":"a"}]"#;
        session.content.insert(id, Bytes::from(data.to_vec()));
        ff.size = data.len() as u64;
        session.inputs.push(ff);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let success = &session.transferred[0];
        assert_eq!(success.1, "success");
        assert_eq!(
            success.0.get_attribute("source").unwrap().as_ref(),
            "test-source"
        );
    }
}
