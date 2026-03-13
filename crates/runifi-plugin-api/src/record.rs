//! Record-oriented processing types.
//!
//! Provides [`Record`], [`RecordSchema`], [`RecordReader`], and [`RecordWriter`]
//! for structured data processing. Instead of splitting a 1M-line CSV into 1M
//! FlowFiles, a record-aware processor parses FlowFile content into records,
//! transforms/routes at the record level, and writes results back.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::result::ProcessResult;

// ---------------------------------------------------------------------------
// RecordValue — the value type for record fields
// ---------------------------------------------------------------------------

/// A dynamically-typed value within a [`Record`].
#[derive(Debug, Clone, PartialEq)]
pub enum RecordValue {
    /// A string value.
    String(String),
    /// An integer value (i64 covers most use cases without precision loss).
    Int(i64),
    /// A floating-point value.
    Float(f64),
    /// A boolean value.
    Boolean(bool),
    /// A nested array of values.
    Array(Vec<RecordValue>),
    /// A nested record (sub-object).
    Record(Record),
    /// An explicit null/missing value.
    Null,
}

impl fmt::Display for RecordValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordValue::String(s) => write!(f, "{s}"),
            RecordValue::Int(n) => write!(f, "{n}"),
            RecordValue::Float(n) => write!(f, "{n}"),
            RecordValue::Boolean(b) => write!(f, "{b}"),
            RecordValue::Null => write!(f, "null"),
            RecordValue::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            RecordValue::Record(rec) => write!(f, "{rec:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Record — ordered map of field names to values
// ---------------------------------------------------------------------------

/// An ordered map of field names to [`RecordValue`]s.
///
/// Uses `BTreeMap` for deterministic field ordering which is important
/// for CSV output and schema consistency.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    fields: BTreeMap<String, RecordValue>,
}

impl Record {
    /// Create a new empty record.
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
        }
    }

    /// Create a record from a list of (name, value) pairs.
    pub fn from_fields(fields: Vec<(String, RecordValue)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    /// Get a field value by name.
    pub fn get(&self, name: &str) -> Option<&RecordValue> {
        self.fields.get(name)
    }

    /// Set a field value.
    pub fn set(&mut self, name: String, value: RecordValue) {
        self.fields.insert(name, value);
    }

    /// Remove a field, returning the value if it existed.
    pub fn remove(&mut self, name: &str) -> Option<RecordValue> {
        self.fields.remove(name)
    }

    /// Return field names in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|k| k.as_str()).collect()
    }

    /// Return the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the record has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Iterate over (name, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &RecordValue)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Consume the record and return the inner field map.
    pub fn into_fields(self) -> BTreeMap<String, RecordValue> {
        self.fields
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// RecordFieldType — the type descriptor for schema fields
// ---------------------------------------------------------------------------

/// The data type of a schema field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordFieldType {
    String,
    Int,
    Float,
    Boolean,
    Array(Box<RecordFieldType>),
    Record(Arc<RecordSchema>),
    Null,
    /// A union/choice type — the value can be any of these types.
    Choice(Vec<RecordFieldType>),
}

// ---------------------------------------------------------------------------
// RecordSchema — defines the structure of records
// ---------------------------------------------------------------------------

/// A named field in a [`RecordSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    pub name: String,
    pub field_type: RecordFieldType,
    pub nullable: bool,
}

/// Describes the expected structure of records.
///
/// Schemas can be named for registry lookup and support nested schemas
/// via [`RecordFieldType::Record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSchema {
    /// Optional schema name (for registry lookup).
    pub name: Option<String>,
    /// Ordered list of fields.
    pub fields: Vec<SchemaField>,
}

impl RecordSchema {
    /// Create a new schema with a name and fields.
    pub fn new(name: Option<String>, fields: Vec<SchemaField>) -> Self {
        Self { name, fields }
    }

    /// Look up a field by name.
    pub fn field(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Return all field names in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Infer a schema from a single record.
    pub fn infer_from_record(record: &Record) -> Self {
        let fields = record
            .iter()
            .map(|(name, value)| SchemaField {
                name: name.to_string(),
                field_type: Self::infer_type(value),
                nullable: matches!(value, RecordValue::Null),
            })
            .collect();
        Self { name: None, fields }
    }

    /// Merge two schemas, unifying field types with Choice where they differ.
    pub fn merge(&self, other: &RecordSchema) -> RecordSchema {
        let mut merged_fields: Vec<SchemaField> = self.fields.clone();

        for other_field in &other.fields {
            if let Some(existing) = merged_fields
                .iter_mut()
                .find(|f| f.name == other_field.name)
            {
                // If types differ, create a Choice type.
                if existing.field_type != other_field.field_type {
                    let types = match &existing.field_type {
                        RecordFieldType::Choice(types) => {
                            let mut types = types.clone();
                            if !types.contains(&other_field.field_type) {
                                types.push(other_field.field_type.clone());
                            }
                            types
                        }
                        _ => {
                            if existing.field_type == other_field.field_type {
                                vec![existing.field_type.clone()]
                            } else {
                                vec![existing.field_type.clone(), other_field.field_type.clone()]
                            }
                        }
                    };
                    existing.field_type = RecordFieldType::Choice(types);
                }
                existing.nullable = existing.nullable || other_field.nullable;
            } else {
                // Field only exists in `other` — mark as nullable.
                let mut field = other_field.clone();
                field.nullable = true;
                merged_fields.push(field);
            }
        }

        // Fields that only exist in `self` but not in `other` become nullable.
        for field in &mut merged_fields {
            if !other.fields.iter().any(|f| f.name == field.name) {
                field.nullable = true;
            }
        }

        RecordSchema {
            name: None,
            fields: merged_fields,
        }
    }

    fn infer_type(value: &RecordValue) -> RecordFieldType {
        match value {
            RecordValue::String(_) => RecordFieldType::String,
            RecordValue::Int(_) => RecordFieldType::Int,
            RecordValue::Float(_) => RecordFieldType::Float,
            RecordValue::Boolean(_) => RecordFieldType::Boolean,
            RecordValue::Null => RecordFieldType::Null,
            RecordValue::Array(items) => {
                if let Some(first) = items.first() {
                    RecordFieldType::Array(Box::new(Self::infer_type(first)))
                } else {
                    RecordFieldType::Array(Box::new(RecordFieldType::Null))
                }
            }
            RecordValue::Record(rec) => {
                RecordFieldType::Record(Arc::new(RecordSchema::infer_from_record(rec)))
            }
        }
    }

    /// Validate a record against this schema.
    /// Returns `Ok(())` if the record conforms, or an error message.
    pub fn validate(&self, record: &Record) -> Result<(), String> {
        for field in &self.fields {
            match record.get(&field.name) {
                None | Some(RecordValue::Null) => {
                    if !field.nullable {
                        return Err(format!(
                            "required field '{}' is missing or null",
                            field.name
                        ));
                    }
                }
                Some(value) => {
                    if !Self::value_matches_type(value, &field.field_type) {
                        return Err(format!(
                            "field '{}' has type {:?}, expected {:?}",
                            field.name,
                            Self::infer_type(value),
                            field.field_type
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn value_matches_type(value: &RecordValue, expected: &RecordFieldType) -> bool {
        match (value, expected) {
            (RecordValue::Null, _) => true, // null is always valid (checked separately via nullable)
            (RecordValue::String(_), RecordFieldType::String) => true,
            (RecordValue::Int(_), RecordFieldType::Int) => true,
            (RecordValue::Float(_), RecordFieldType::Float) => true,
            // Accept Int where Float is expected (implicit widening).
            (RecordValue::Int(_), RecordFieldType::Float) => true,
            (RecordValue::Boolean(_), RecordFieldType::Boolean) => true,
            (RecordValue::Array(_), RecordFieldType::Array(_)) => true,
            (RecordValue::Record(_), RecordFieldType::Record(_)) => true,
            (_, RecordFieldType::Choice(types)) => {
                types.iter().any(|t| Self::value_matches_type(value, t))
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// RecordReader / RecordWriter traits
// ---------------------------------------------------------------------------

/// Parses raw bytes into an iterator of [`Record`]s.
///
/// Implementations are typically registered as controller services and
/// shared across processors.
pub trait RecordReader: Send + Sync {
    /// Parse the given bytes into a vector of records.
    ///
    /// An optional schema can be provided for validation. If `None`,
    /// the reader should infer the schema from the data.
    fn read_records(
        &self,
        data: &[u8],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<Record>>;

    /// Return the schema of the data, if it can be determined without
    /// reading all records. Returns `None` if schema inference requires
    /// reading data.
    fn schema_from_header(&self, _data: &[u8]) -> Option<RecordSchema> {
        None
    }
}

/// Serializes records into raw bytes.
pub trait RecordWriter: Send + Sync {
    /// Write the given records into bytes.
    ///
    /// An optional schema can be provided to control field ordering and
    /// formatting. If `None`, the writer should use the records' own
    /// field ordering.
    fn write_records(
        &self,
        records: &[Record],
        schema: Option<&RecordSchema>,
    ) -> ProcessResult<Vec<u8>>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_basic_operations() {
        let mut rec = Record::new();
        assert!(rec.is_empty());

        rec.set("name".to_string(), RecordValue::String("Alice".to_string()));
        rec.set("age".to_string(), RecordValue::Int(30));
        rec.set("active".to_string(), RecordValue::Boolean(true));

        assert_eq!(rec.len(), 3);
        assert!(!rec.is_empty());
        assert_eq!(
            rec.get("name"),
            Some(&RecordValue::String("Alice".to_string()))
        );
        assert_eq!(rec.get("age"), Some(&RecordValue::Int(30)));
        assert_eq!(rec.get("missing"), None);

        let removed = rec.remove("age");
        assert_eq!(removed, Some(RecordValue::Int(30)));
        assert_eq!(rec.len(), 2);
    }

    #[test]
    fn record_from_fields() {
        let rec = Record::from_fields(vec![
            ("a".to_string(), RecordValue::Int(1)),
            ("b".to_string(), RecordValue::Int(2)),
        ]);
        assert_eq!(rec.len(), 2);
        assert_eq!(rec.get("a"), Some(&RecordValue::Int(1)));
    }

    #[test]
    fn record_nested() {
        let inner = Record::from_fields(vec![(
            "zip".to_string(),
            RecordValue::String("90210".to_string()),
        )]);
        let outer = Record::from_fields(vec![(
            "address".to_string(),
            RecordValue::Record(inner.clone()),
        )]);
        assert_eq!(outer.get("address"), Some(&RecordValue::Record(inner)));
    }

    #[test]
    fn schema_infer_from_record() {
        let rec = Record::from_fields(vec![
            ("name".to_string(), RecordValue::String("Bob".to_string())),
            ("age".to_string(), RecordValue::Int(25)),
            ("score".to_string(), RecordValue::Float(98.5)),
        ]);
        let schema = RecordSchema::infer_from_record(&rec);

        assert_eq!(schema.fields.len(), 3);
        assert_eq!(
            schema.field("name").unwrap().field_type,
            RecordFieldType::String
        );
        assert_eq!(
            schema.field("age").unwrap().field_type,
            RecordFieldType::Int
        );
        assert_eq!(
            schema.field("score").unwrap().field_type,
            RecordFieldType::Float
        );
    }

    #[test]
    fn schema_validate_pass() {
        let schema = RecordSchema::new(
            None,
            vec![
                SchemaField {
                    name: "name".to_string(),
                    field_type: RecordFieldType::String,
                    nullable: false,
                },
                SchemaField {
                    name: "age".to_string(),
                    field_type: RecordFieldType::Int,
                    nullable: true,
                },
            ],
        );

        let rec = Record::from_fields(vec![
            ("name".to_string(), RecordValue::String("Alice".to_string())),
            ("age".to_string(), RecordValue::Int(30)),
        ]);
        assert!(schema.validate(&rec).is_ok());

        // Null value for nullable field is ok.
        let rec2 = Record::from_fields(vec![
            ("name".to_string(), RecordValue::String("Bob".to_string())),
            ("age".to_string(), RecordValue::Null),
        ]);
        assert!(schema.validate(&rec2).is_ok());
    }

    #[test]
    fn schema_validate_missing_required() {
        let schema = RecordSchema::new(
            None,
            vec![SchemaField {
                name: "name".to_string(),
                field_type: RecordFieldType::String,
                nullable: false,
            }],
        );

        let rec = Record::new();
        let result = schema.validate(&rec);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing or null"));
    }

    #[test]
    fn schema_validate_wrong_type() {
        let schema = RecordSchema::new(
            None,
            vec![SchemaField {
                name: "age".to_string(),
                field_type: RecordFieldType::Int,
                nullable: false,
            }],
        );

        let rec = Record::from_fields(vec![(
            "age".to_string(),
            RecordValue::String("thirty".to_string()),
        )]);
        assert!(schema.validate(&rec).is_err());
    }

    #[test]
    fn schema_validate_int_as_float() {
        let schema = RecordSchema::new(
            None,
            vec![SchemaField {
                name: "value".to_string(),
                field_type: RecordFieldType::Float,
                nullable: false,
            }],
        );

        // Int should be accepted where Float is expected.
        let rec = Record::from_fields(vec![("value".to_string(), RecordValue::Int(42))]);
        assert!(schema.validate(&rec).is_ok());
    }

    #[test]
    fn schema_merge() {
        let s1 = RecordSchema::new(
            None,
            vec![
                SchemaField {
                    name: "a".to_string(),
                    field_type: RecordFieldType::String,
                    nullable: false,
                },
                SchemaField {
                    name: "b".to_string(),
                    field_type: RecordFieldType::Int,
                    nullable: false,
                },
            ],
        );
        let s2 = RecordSchema::new(
            None,
            vec![
                SchemaField {
                    name: "a".to_string(),
                    field_type: RecordFieldType::String,
                    nullable: false,
                },
                SchemaField {
                    name: "c".to_string(),
                    field_type: RecordFieldType::Float,
                    nullable: false,
                },
            ],
        );

        let merged = s1.merge(&s2);
        assert_eq!(merged.fields.len(), 3);
        // "b" only in s1 -> nullable.
        assert!(merged.field("b").unwrap().nullable);
        // "c" only in s2 -> nullable.
        assert!(merged.field("c").unwrap().nullable);
        // "a" in both -> not nullable.
        assert!(!merged.field("a").unwrap().nullable);
    }

    #[test]
    fn record_value_display() {
        assert_eq!(format!("{}", RecordValue::String("hi".to_string())), "hi");
        assert_eq!(format!("{}", RecordValue::Int(42)), "42");
        assert_eq!(format!("{}", RecordValue::Float(3.14)), "3.14");
        assert_eq!(format!("{}", RecordValue::Boolean(true)), "true");
        assert_eq!(format!("{}", RecordValue::Null), "null");
        assert_eq!(
            format!(
                "{}",
                RecordValue::Array(vec![RecordValue::Int(1), RecordValue::Int(2)])
            ),
            "[1, 2]"
        );
    }
}
