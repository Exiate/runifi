//! JSON RecordWriter — serializes Records into JSON arrays or NDJSON.

use runifi_plugin_api::record::{Record, RecordSchema, RecordValue, RecordWriter};
use runifi_plugin_api::result::{PluginError, ProcessResult};
use serde_json::{Map, Value};

/// Output format for JSON writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonOutputFormat {
    /// A JSON array of objects: `[{...}, {...}]`
    Array,
    /// Newline-delimited JSON (NDJSON): one object per line.
    LineDelimited,
}

/// A [`RecordWriter`] that serializes records to JSON.
pub struct JsonRecordWriter {
    format: JsonOutputFormat,
    pretty: bool,
}

impl JsonRecordWriter {
    pub fn new(format: JsonOutputFormat) -> Self {
        Self {
            format,
            pretty: false,
        }
    }

    /// Enable pretty-printing (indented JSON).
    pub fn pretty(mut self) -> Self {
        self.pretty = true;
        self
    }
}

impl Default for JsonRecordWriter {
    fn default() -> Self {
        Self::new(JsonOutputFormat::Array)
    }
}

impl RecordWriter for JsonRecordWriter {
    fn write_records(
        &self,
        records: &[Record],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<u8>> {
        match self.format {
            JsonOutputFormat::Array => self.write_array(records, schema),
            JsonOutputFormat::LineDelimited => self.write_ndjson(records, schema),
        }
    }
}

impl JsonRecordWriter {
    fn write_array(
        &self,
        records: &[Record],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<u8>> {
        let values: Vec<Value> = records
            .iter()
            .map(|r| record_to_json_value(r, schema))
            .collect();
        let arr = Value::Array(values);

        let bytes = if self.pretty {
            serde_json::to_vec_pretty(&arr)
        } else {
            serde_json::to_vec(&arr)
        }
        .map_err(|e| PluginError::ProcessingFailed(format!("JSON serialization error: {e}")))?;

        Ok(bytes)
    }

    fn write_ndjson(
        &self,
        records: &[Record],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<u8>> {
        let mut output = Vec::new();
        for record in records {
            let value = record_to_json_value(record, schema);
            let line = serde_json::to_vec(&value).map_err(|e| {
                PluginError::ProcessingFailed(format!("JSON serialization error: {e}"))
            })?;
            output.extend_from_slice(&line);
            output.push(b'\n');
        }
        Ok(output)
    }
}

/// Convert a [`Record`] into a `serde_json::Value`.
///
/// If a schema is provided, fields are written in schema order. Extra fields
/// in the record that are not in the schema are appended at the end.
fn record_to_json_value(record: &Record, schema: Option<&RecordSchema>) -> Value {
    let mut map = Map::new();

    if let Some(schema) = schema {
        // Write fields in schema order first.
        for field_def in &schema.fields {
            let value = record
                .get(&field_def.name)
                .map(record_value_to_json)
                .unwrap_or(Value::Null);
            map.insert(field_def.name.clone(), value);
        }
        // Append any extra fields not in the schema.
        for (name, value) in record.iter() {
            if !map.contains_key(name) {
                map.insert(name.to_string(), record_value_to_json(value));
            }
        }
    } else {
        for (name, value) in record.iter() {
            map.insert(name.to_string(), record_value_to_json(value));
        }
    }

    Value::Object(map)
}

/// Convert a [`RecordValue`] into a `serde_json::Value`.
fn record_value_to_json(value: &RecordValue) -> Value {
    match value {
        RecordValue::Null => Value::Null,
        RecordValue::Boolean(b) => Value::Bool(*b),
        RecordValue::Int(n) => Value::Number((*n).into()),
        RecordValue::Float(f) => {
            serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
        }
        RecordValue::String(s) => Value::String(s.clone()),
        RecordValue::Array(arr) => Value::Array(arr.iter().map(record_value_to_json).collect()),
        RecordValue::Record(rec) => record_to_json_value(rec, None),
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
    fn writes_json_array() {
        let writer = JsonRecordWriter::new(JsonOutputFormat::Array);
        let records = vec![
            make_record(vec![
                ("name", RecordValue::String("Alice".to_string())),
                ("age", RecordValue::Int(30)),
            ]),
            make_record(vec![
                ("name", RecordValue::String("Bob".to_string())),
                ("age", RecordValue::Int(25)),
            ]),
        ];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], "Alice");
        assert_eq!(parsed[0]["age"], 30);
        assert_eq!(parsed[1]["name"], "Bob");
    }

    #[test]
    fn writes_ndjson() {
        let writer = JsonRecordWriter::new(JsonOutputFormat::LineDelimited);
        let records = vec![
            make_record(vec![("x", RecordValue::Int(1))]),
            make_record(vec![("x", RecordValue::Int(2))]),
        ];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = text.trim().lines().collect();

        assert_eq!(lines.len(), 2);
        let v0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["x"], 1);
    }

    #[test]
    fn writes_empty_records() {
        let writer = JsonRecordWriter::new(JsonOutputFormat::Array);
        let output = writer.write_records(&[], None).unwrap();
        let text = String::from_utf8(output).unwrap();
        assert_eq!(text, "[]");
    }

    #[test]
    fn writes_nested_values() {
        let writer = JsonRecordWriter::new(JsonOutputFormat::Array);
        let inner = make_record(vec![("zip", RecordValue::String("90210".to_string()))]);
        let records = vec![make_record(vec![
            ("address", RecordValue::Record(inner)),
            (
                "tags",
                RecordValue::Array(vec![
                    RecordValue::String("a".to_string()),
                    RecordValue::String("b".to_string()),
                ]),
            ),
        ])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed[0]["address"]["zip"], "90210");
        assert_eq!(parsed[0]["tags"][0], "a");
    }

    #[test]
    fn writes_null_and_bool() {
        let writer = JsonRecordWriter::new(JsonOutputFormat::Array);
        let records = vec![make_record(vec![
            ("flag", RecordValue::Boolean(true)),
            ("empty", RecordValue::Null),
        ])];

        let output = writer.write_records(&records, None).unwrap();
        let text = String::from_utf8(output).unwrap();
        let parsed: Vec<Value> = serde_json::from_str(&text).unwrap();

        assert_eq!(parsed[0]["flag"], true);
        assert!(parsed[0]["empty"].is_null());
    }

    #[test]
    fn roundtrip_json() {
        use crate::record::json_reader::JsonRecordReader;

        let original = vec![
            make_record(vec![
                ("name", RecordValue::String("Alice".to_string())),
                ("age", RecordValue::Int(30)),
                ("active", RecordValue::Boolean(true)),
            ]),
            make_record(vec![
                ("name", RecordValue::String("Bob".to_string())),
                ("age", RecordValue::Int(25)),
                ("active", RecordValue::Boolean(false)),
            ]),
        ];

        let writer = JsonRecordWriter::new(JsonOutputFormat::Array);
        let bytes = writer.write_records(&original, None).unwrap();

        let reader = JsonRecordReader::new();
        let parsed = reader.read_records(&bytes, None).unwrap();

        assert_eq!(original, parsed);
    }
}
