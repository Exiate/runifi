//! CSV RecordWriter — serializes Records into CSV format.

use runifi_plugin_api::record::{Record, RecordSchema, RecordValue, RecordWriter};
use runifi_plugin_api::result::{PluginError, ProcessResult};

/// A [`RecordWriter`] that serializes records to CSV.
///
/// Supports configurable delimiter and header output. Field order is determined
/// by the schema (if provided) or by the first record's field ordering.
pub struct CsvRecordWriter {
    delimiter: u8,
    write_header: bool,
}

impl CsvRecordWriter {
    pub fn new() -> Self {
        Self {
            delimiter: b',',
            write_header: true,
        }
    }

    /// Set the field delimiter (default: `,`).
    pub fn delimiter(mut self, delim: u8) -> Self {
        self.delimiter = delim;
        self
    }

    /// Set whether to write a header row (default: `true`).
    pub fn write_header(mut self, write: bool) -> Self {
        self.write_header = write;
        self
    }
}

impl Default for CsvRecordWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordWriter for CsvRecordWriter {
    fn write_records(
        &self,
        records: &[Record],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<u8>> {
        if records.is_empty() {
            // Write just headers if schema is provided and header writing is enabled.
            if self.write_header
                && let Some(s) = schema
            {
                let mut wtr = csv::WriterBuilder::new()
                    .delimiter(self.delimiter)
                    .has_headers(false) // We write headers manually.
                    .from_writer(Vec::new());
                let headers: Vec<&str> = s.field_names();
                wtr.write_record(&headers).map_err(|e| {
                    PluginError::ProcessingFailed(format!("CSV header write error: {e}"))
                })?;
                return wtr
                    .into_inner()
                    .map_err(|e| PluginError::ProcessingFailed(format!("CSV flush error: {e}")));
            }
            return Ok(Vec::new());
        }

        // Determine field order: from schema or from first record.
        let field_order: Vec<String> = if let Some(s) = schema {
            s.field_names().iter().map(|n| n.to_string()).collect()
        } else {
            records[0]
                .field_names()
                .iter()
                .map(|n| n.to_string())
                .collect()
        };

        let mut wtr = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(false) // We manage headers ourselves for ordering.
            .from_writer(Vec::new());

        // Write header row.
        if self.write_header {
            let headers: Vec<&str> = field_order.iter().map(|s| s.as_str()).collect();
            wtr.write_record(&headers).map_err(|e| {
                PluginError::ProcessingFailed(format!("CSV header write error: {e}"))
            })?;
        }

        // Write data rows.
        for record in records {
            let row: Vec<String> = field_order
                .iter()
                .map(|field_name| {
                    record
                        .get(field_name)
                        .map(record_value_to_csv_string)
                        .unwrap_or_default()
                })
                .collect();
            wtr.write_record(&row)
                .map_err(|e| PluginError::ProcessingFailed(format!("CSV write error: {e}")))?;
        }

        wtr.into_inner()
            .map_err(|e| PluginError::ProcessingFailed(format!("CSV flush error: {e}")))
    }
}

/// Convert a [`RecordValue`] to its CSV string representation.
fn record_value_to_csv_string(value: &RecordValue) -> String {
    match value {
        RecordValue::Null => String::new(),
        RecordValue::String(s) => s.clone(),
        RecordValue::Int(n) => n.to_string(),
        RecordValue::Float(f) => f.to_string(),
        RecordValue::Boolean(b) => b.to_string(),
        RecordValue::Array(arr) => {
            // Serialize arrays as JSON-like representation within CSV.
            let items: Vec<String> = arr.iter().map(record_value_to_csv_string).collect();
            format!("[{}]", items.join(","))
        }
        RecordValue::Record(_) => {
            // Nested records are not well-represented in CSV; use a marker.
            "{...}".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::RecordReader;

    fn make_record(fields: Vec<(&str, RecordValue)>) -> Record {
        Record::from_fields(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    #[test]
    fn writes_csv_with_header() {
        let writer = CsvRecordWriter::new();
        let records = vec![
            make_record(vec![
                ("age", RecordValue::Int(30)),
                ("name", RecordValue::String("Alice".to_string())),
            ]),
            make_record(vec![
                ("age", RecordValue::Int(25)),
                ("name", RecordValue::String("Bob".to_string())),
            ]),
        ];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();

        assert_eq!(lines.len(), 3); // header + 2 rows
        assert_eq!(lines[0], "age,name");
        assert_eq!(lines[1], "30,Alice");
        assert_eq!(lines[2], "25,Bob");
    }

    #[test]
    fn writes_csv_without_header() {
        let writer = CsvRecordWriter::new().write_header(false);
        let records = vec![make_record(vec![
            ("a", RecordValue::Int(1)),
            ("b", RecordValue::Int(2)),
        ])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "1,2");
    }

    #[test]
    fn writes_tsv() {
        let writer = CsvRecordWriter::new().delimiter(b'\t');
        let records = vec![make_record(vec![
            ("a", RecordValue::String("x".to_string())),
            ("b", RecordValue::String("y".to_string())),
        ])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("a\tb"));
        assert!(text.contains("x\ty"));
    }

    #[test]
    fn writes_empty_records() {
        let writer = CsvRecordWriter::new();
        let output = writer.write_records(&[], None).unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn null_becomes_empty() {
        let writer = CsvRecordWriter::new();
        let records = vec![make_record(vec![
            ("a", RecordValue::Int(1)),
            ("b", RecordValue::Null),
        ])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();
        assert_eq!(lines[1], "1,");
    }

    #[test]
    fn boolean_values() {
        let writer = CsvRecordWriter::new();
        let records = vec![make_record(vec![("flag", RecordValue::Boolean(true))])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("true"));
    }

    #[test]
    fn schema_controls_field_order() {
        use runifi_plugin_api::record::{RecordFieldType, SchemaField};

        let schema = RecordSchema::new(
            None,
            vec![
                SchemaField {
                    name: "z".to_string(),
                    field_type: RecordFieldType::String,
                    nullable: false,
                },
                SchemaField {
                    name: "a".to_string(),
                    field_type: RecordFieldType::String,
                    nullable: false,
                },
            ],
        );

        let writer = CsvRecordWriter::new();
        let records = vec![make_record(vec![
            ("a", RecordValue::String("first".to_string())),
            ("z", RecordValue::String("last".to_string())),
        ])];

        let output = writer.write_records(&records, Some(&schema)).unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();

        // Schema specifies z before a.
        assert_eq!(lines[0], "z,a");
        assert_eq!(lines[1], "last,first");
    }

    #[test]
    fn roundtrip_csv() {
        use crate::record::csv_reader::CsvRecordReader;

        let original = vec![
            make_record(vec![
                ("name", RecordValue::String("Alice".to_string())),
                ("score", RecordValue::String("95.5".to_string())),
            ]),
            make_record(vec![
                ("name", RecordValue::String("Bob".to_string())),
                ("score", RecordValue::String("88".to_string())),
            ]),
        ];

        let writer = CsvRecordWriter::new();
        let bytes = writer.write_records(&original, None).unwrap();

        let reader = CsvRecordReader::new();
        let parsed = reader.read_records(&bytes, None).unwrap();

        // CSV round-trip may infer different types (strings -> ints/floats).
        // Check field names and count.
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].get("name").is_some());
        assert!(parsed[0].get("score").is_some());
    }
}
