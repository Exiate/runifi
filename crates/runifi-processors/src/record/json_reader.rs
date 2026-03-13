//! JSON RecordReader — parses JSON arrays and line-delimited JSON into Records.

use runifi_plugin_api::record::{Record, RecordReader, RecordSchema, RecordValue};
use runifi_plugin_api::result::{PluginError, ProcessResult};
use serde_json::Value;

/// A [`RecordReader`] that parses JSON data.
///
/// Supports two formats:
/// - **Array format**: `[{"a":1}, {"a":2}]` — a JSON array of objects.
/// - **Line-delimited (NDJSON)**: One JSON object per line, separated by newlines.
///
/// The reader auto-detects the format: if the trimmed input starts with `[`,
/// it is treated as an array; otherwise as line-delimited.
pub struct JsonRecordReader;

impl JsonRecordReader {
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonRecordReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordReader for JsonRecordReader {
    fn read_records(
        &self,
        data: &[u8],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<Record>> {
        let text = std::str::from_utf8(data).map_err(|e| {
            PluginError::ProcessingFailed(format!("invalid UTF-8 in JSON input: {e}"))
        })?;

        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let records = if trimmed.starts_with('[') {
            // Array format.
            let arr: Vec<Value> = serde_json::from_str(trimmed).map_err(|e| {
                PluginError::ProcessingFailed(format!("failed to parse JSON array: {e}"))
            })?;
            arr.into_iter()
                .map(json_value_to_record)
                .collect::<ProcessResult<Vec<_>>>()?
        } else {
            // Line-delimited (NDJSON) format.
            let mut records = Vec::new();
            for (line_num, line) in trimmed.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(line).map_err(|e| {
                    PluginError::ProcessingFailed(format!(
                        "failed to parse JSON on line {}: {e}",
                        line_num + 1
                    ))
                })?;
                records.push(json_value_to_record(value)?);
            }
            records
        };

        // Validate against schema if provided.
        if let Some(schema) = schema {
            for (i, record) in records.iter().enumerate() {
                schema.validate(record).map_err(|e| {
                    PluginError::ProcessingFailed(format!("record {i} schema validation: {e}"))
                })?;
            }
        }

        Ok(records)
    }
}

/// Convert a `serde_json::Value` into a [`Record`].
///
/// Only JSON objects become records. Other values produce an error.
fn json_value_to_record(value: Value) -> ProcessResult<Record> {
    match value {
        Value::Object(map) => {
            let fields: Vec<(String, RecordValue)> = map
                .into_iter()
                .map(|(k, v)| (k, json_value_to_record_value(v)))
                .collect();
            Ok(Record::from_fields(fields))
        }
        other => Err(PluginError::ProcessingFailed(format!(
            "expected JSON object, got {other}"
        ))),
    }
}

/// Convert a `serde_json::Value` into a [`RecordValue`].
fn json_value_to_record_value(value: Value) -> RecordValue {
    match value {
        Value::Null => RecordValue::Null,
        Value::Bool(b) => RecordValue::Boolean(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                RecordValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                RecordValue::Float(f)
            } else {
                RecordValue::String(n.to_string())
            }
        }
        Value::String(s) => RecordValue::String(s),
        Value::Array(arr) => {
            RecordValue::Array(arr.into_iter().map(json_value_to_record_value).collect())
        }
        Value::Object(map) => {
            let fields: Vec<(String, RecordValue)> = map
                .into_iter()
                .map(|(k, v)| (k, json_value_to_record_value(v)))
                .collect();
            RecordValue::Record(Record::from_fields(fields))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_json_array() {
        let reader = JsonRecordReader::new();
        let data = br#"[{"name":"Alice","age":30},{"name":"Bob","age":25}]"#;
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].get("name"),
            Some(&RecordValue::String("Alice".to_string()))
        );
        assert_eq!(records[0].get("age"), Some(&RecordValue::Int(30)));
        assert_eq!(
            records[1].get("name"),
            Some(&RecordValue::String("Bob".to_string()))
        );
    }

    #[test]
    fn reads_ndjson() {
        let reader = JsonRecordReader::new();
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n";
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].get("x"), Some(&RecordValue::Int(1)));
        assert_eq!(records[2].get("x"), Some(&RecordValue::Int(3)));
    }

    #[test]
    fn reads_empty_input() {
        let reader = JsonRecordReader::new();
        let records = reader.read_records(b"", None).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn reads_nested_objects() {
        let reader = JsonRecordReader::new();
        let data = br#"[{"user":{"name":"Alice"},"tags":["a","b"]}]"#;
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records.len(), 1);
        let user = records[0].get("user").unwrap();
        match user {
            RecordValue::Record(inner) => {
                assert_eq!(
                    inner.get("name"),
                    Some(&RecordValue::String("Alice".to_string()))
                );
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn reads_null_and_bool() {
        let reader = JsonRecordReader::new();
        let data = br#"[{"flag":true,"empty":null}]"#;
        let records = reader.read_records(data, None).unwrap();

        assert_eq!(records[0].get("flag"), Some(&RecordValue::Boolean(true)));
        assert_eq!(records[0].get("empty"), Some(&RecordValue::Null));
    }

    #[test]
    fn reads_floats() {
        let reader = JsonRecordReader::new();
        let data = br#"[{"pi":3.14}]"#;
        let records = reader.read_records(data, None).unwrap();
        assert_eq!(records[0].get("pi"), Some(&RecordValue::Float(3.14)));
    }

    #[test]
    fn rejects_non_object() {
        let reader = JsonRecordReader::new();
        let data = br#"[1, 2, 3]"#;
        assert!(reader.read_records(data, None).is_err());
    }

    #[test]
    fn validates_against_schema() {
        use runifi_plugin_api::record::{RecordFieldType, SchemaField};

        let schema = RecordSchema::new(
            None,
            vec![SchemaField {
                name: "name".to_string(),
                field_type: RecordFieldType::String,
                nullable: false,
            }],
        );

        let reader = JsonRecordReader::new();
        // Valid.
        let data = br#"[{"name":"Alice"}]"#;
        assert!(reader.read_records(data, Some(&schema)).is_ok());

        // Missing required field.
        let data2 = br#"[{"age":30}]"#;
        assert!(reader.read_records(data2, Some(&schema)).is_err());
    }

    #[test]
    fn ndjson_skips_blank_lines() {
        let reader = JsonRecordReader::new();
        let data = b"{\"a\":1}\n\n{\"a\":2}\n\n";
        let records = reader.read_records(data, None).unwrap();
        assert_eq!(records.len(), 2);
    }
}
