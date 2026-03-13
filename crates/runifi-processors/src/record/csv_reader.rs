//! CSV RecordReader — parses CSV data into Records.

use runifi_plugin_api::record::{
    Record, RecordFieldType, RecordReader, RecordSchema, RecordValue, SchemaField,
};
use runifi_plugin_api::result::{PluginError, ProcessResult};

/// A [`RecordReader`] that parses CSV data.
///
/// Supports configurable delimiter and header handling. When headers are present,
/// they become field names. Without headers, fields are named `column_0`, `column_1`, etc.
pub struct CsvRecordReader {
    delimiter: u8,
    has_header: bool,
}

impl CsvRecordReader {
    pub fn new() -> Self {
        Self {
            delimiter: b',',
            has_header: true,
        }
    }

    /// Set the field delimiter (default: `,`).
    pub fn delimiter(mut self, delim: u8) -> Self {
        self.delimiter = delim;
        self
    }

    /// Set whether the CSV has a header row (default: `true`).
    pub fn has_header(mut self, has: bool) -> Self {
        self.has_header = has;
        self
    }
}

impl Default for CsvRecordReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordReader for CsvRecordReader {
    fn read_records(
        &self,
        data: &[u8],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<Record>> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_header)
            .from_reader(data);

        // Determine field names: from headers, schema, or generated.
        let headers: Vec<String> = if self.has_header {
            let hdrs = rdr
                .headers()
                .map_err(|e| PluginError::ProcessingFailed(format!("CSV header error: {e}")))?;
            hdrs.iter().map(|h| h.to_string()).collect()
        } else if let Some(s) = schema {
            s.field_names().iter().map(|n| n.to_string()).collect()
        } else {
            Vec::new() // Will generate column_N names on first record.
        };

        let mut records = Vec::new();

        for (row_idx, result) in rdr.records().enumerate() {
            let csv_record = result.map_err(|e| {
                PluginError::ProcessingFailed(format!("CSV parse error at row {row_idx}: {e}"))
            })?;

            let field_names: Vec<String> = if headers.is_empty() {
                // Generate column names based on the number of fields.
                (0..csv_record.len())
                    .map(|i| format!("column_{i}"))
                    .collect()
            } else {
                headers.clone()
            };

            let fields: Vec<(String, RecordValue)> = field_names
                .into_iter()
                .zip(csv_record.iter())
                .map(|(name, value)| {
                    let record_value = if let Some(s) = schema {
                        parse_typed_value(value, s.field(&name))
                    } else {
                        infer_value(value)
                    };
                    (name, record_value)
                })
                .collect();

            let record = Record::from_fields(fields);

            // Validate against schema if provided.
            if let Some(s) = schema {
                s.validate(&record).map_err(|e| {
                    PluginError::ProcessingFailed(format!("row {row_idx} schema validation: {e}"))
                })?;
            }

            records.push(record);
        }

        Ok(records)
    }

    fn schema_from_header(&self, data: &[u8]) -> Option<RecordSchema> {
        if !self.has_header {
            return None;
        }

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(true)
            .from_reader(data);

        let headers = rdr.headers().ok()?;
        let fields = headers
            .iter()
            .map(|h| SchemaField {
                name: h.to_string(),
                field_type: RecordFieldType::String, // CSV fields are strings by default.
                nullable: true,
            })
            .collect();

        Some(RecordSchema::new(None, fields))
    }
}

/// Parse a CSV field value using the schema field type for guidance.
fn parse_typed_value(raw: &str, field: Option<&SchemaField>) -> RecordValue {
    let Some(field) = field else {
        return infer_value(raw);
    };

    if raw.is_empty() && field.nullable {
        return RecordValue::Null;
    }

    match &field.field_type {
        RecordFieldType::Int => raw
            .parse::<i64>()
            .map(RecordValue::Int)
            .unwrap_or_else(|_| RecordValue::String(raw.to_string())),
        RecordFieldType::Float => raw
            .parse::<f64>()
            .map(RecordValue::Float)
            .unwrap_or_else(|_| RecordValue::String(raw.to_string())),
        RecordFieldType::Boolean => match raw.to_lowercase().as_str() {
            "true" | "1" | "yes" => RecordValue::Boolean(true),
            "false" | "0" | "no" => RecordValue::Boolean(false),
            _ => RecordValue::String(raw.to_string()),
        },
        _ => RecordValue::String(raw.to_string()),
    }
}

/// Attempt to infer the type of a CSV field value.
fn infer_value(raw: &str) -> RecordValue {
    if raw.is_empty() {
        return RecordValue::Null;
    }
    // Try integer.
    if let Ok(i) = raw.parse::<i64>() {
        return RecordValue::Int(i);
    }
    // Try float.
    if let Ok(f) = raw.parse::<f64>() {
        return RecordValue::Float(f);
    }
    // Try boolean.
    match raw.to_lowercase().as_str() {
        "true" => return RecordValue::Boolean(true),
        "false" => return RecordValue::Boolean(false),
        _ => {}
    }
    RecordValue::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_csv_with_header() {
        let reader = CsvRecordReader::new();
        let data = b"name,age,score\nAlice,30,95.5\nBob,25,88.0\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].get("name"),
            Some(&RecordValue::String("Alice".to_string()))
        );
        assert_eq!(records[0].get("age"), Some(&RecordValue::Int(30)));
        assert_eq!(records[0].get("score"), Some(&RecordValue::Float(95.5)));
    }

    #[test]
    fn reads_csv_without_header() {
        let reader = CsvRecordReader::new().has_header(false);
        let data = b"Alice,30\nBob,25\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].get("column_0"),
            Some(&RecordValue::String("Alice".to_string()))
        );
        assert_eq!(records[0].get("column_1"), Some(&RecordValue::Int(30)));
    }

    #[test]
    fn reads_tsv() {
        let reader = CsvRecordReader::new().delimiter(b'\t');
        let data = b"name\tage\nAlice\t30\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].get("name"),
            Some(&RecordValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn infers_types() {
        let reader = CsvRecordReader::new();
        let data = b"int,float,bool,str\n42,3.14,true,hello\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records[0].get("int"), Some(&RecordValue::Int(42)));
        assert_eq!(records[0].get("float"), Some(&RecordValue::Float(3.14)));
        assert_eq!(records[0].get("bool"), Some(&RecordValue::Boolean(true)));
        assert_eq!(
            records[0].get("str"),
            Some(&RecordValue::String("hello".to_string()))
        );
    }

    #[test]
    fn empty_field_is_null() {
        let reader = CsvRecordReader::new();
        let data = b"a,b\n1,\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records[0].get("b"), Some(&RecordValue::Null));
    }

    #[test]
    fn schema_typed_parsing() {
        let schema = RecordSchema::new(
            None,
            vec![
                SchemaField {
                    name: "count".to_string(),
                    field_type: RecordFieldType::Int,
                    nullable: false,
                },
                SchemaField {
                    name: "ratio".to_string(),
                    field_type: RecordFieldType::Float,
                    nullable: false,
                },
                SchemaField {
                    name: "active".to_string(),
                    field_type: RecordFieldType::Boolean,
                    nullable: false,
                },
            ],
        );

        let reader = CsvRecordReader::new();
        let data = b"count,ratio,active\n10,0.5,yes\n";
        let records = reader.read_records(data, Some(&schema)).unwrap();

        assert_eq!(records[0].get("count"), Some(&RecordValue::Int(10)));
        assert_eq!(records[0].get("ratio"), Some(&RecordValue::Float(0.5)));
        assert_eq!(records[0].get("active"), Some(&RecordValue::Boolean(true)));
    }

    #[test]
    fn schema_from_header_works() {
        let reader = CsvRecordReader::new();
        let data = b"name,age,score\n";
        let schema = reader.schema_from_header(data).unwrap();

        assert_eq!(schema.field_names(), vec!["name", "age", "score"]);
    }

    #[test]
    fn reads_empty_csv() {
        let reader = CsvRecordReader::new();
        let data = b"name,age\n";
        let records = reader.read_records(data, None).unwrap();
        assert!(records.is_empty());
    }
}
