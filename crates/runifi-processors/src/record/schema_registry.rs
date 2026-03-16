//! In-memory SchemaRegistry — a ControllerService for managing RecordSchemas.
//!
//! Processors can look up schemas by name through the service registry.
//! Schemas can be registered programmatically or inferred from data.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::record::{Record, RecordFieldType, RecordSchema, SchemaField};
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

/// In-memory schema registry.
///
/// Provides thread-safe storage and retrieval of [`RecordSchema`]s by name.
/// Registered as a controller service so processors can look up shared schemas.
pub struct SchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, Arc<RecordSchema>>>>,
    enabled: bool,
    schema_access_strategy: SchemaAccessStrategy,
}

/// How the registry resolves schemas for processors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaAccessStrategy {
    /// Look up schemas by name from the registry.
    Name,
    /// Infer schema from the data at runtime.
    Infer,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            enabled: false,
            schema_access_strategy: SchemaAccessStrategy::Name,
        }
    }

    /// Get the configured schema access strategy.
    pub fn schema_access_strategy(&self) -> SchemaAccessStrategy {
        self.schema_access_strategy
    }

    /// Register a schema by name. Overwrites any existing schema with the same name.
    pub fn register(&self, name: String, schema: RecordSchema) {
        let mut schemas = self.schemas.write();
        let mut named_schema = schema;
        named_schema.name = Some(name.clone());
        schemas.insert(name, Arc::new(named_schema));
    }

    /// Look up a schema by name.
    pub fn get_schema(&self, name: &str) -> Option<Arc<RecordSchema>> {
        let schemas = self.schemas.read();
        schemas.get(name).cloned()
    }

    /// Remove a schema by name.
    pub fn remove_schema(&self, name: &str) -> Option<Arc<RecordSchema>> {
        let mut schemas = self.schemas.write();
        schemas.remove(name)
    }

    /// List all registered schema names.
    pub fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    /// Infer a schema from a batch of records and optionally register it.
    ///
    /// Examines up to `sample_size` records to build a merged schema that
    /// covers all observed field names and types.
    pub fn infer_schema(
        records: &[Record],
        sample_size: usize,
        name: Option<String>,
    ) -> RecordSchema {
        let sample = &records[..records.len().min(sample_size)];

        if sample.is_empty() {
            return RecordSchema::new(name, Vec::new());
        }

        let mut schema = RecordSchema::infer_from_record(&sample[0]);
        for record in &sample[1..] {
            let record_schema = RecordSchema::infer_from_record(record);
            schema = schema.merge(&record_schema);
        }
        schema.name = name;
        schema
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

const PROP_SCHEMA_ACCESS_STRATEGY: PropertyDescriptor = PropertyDescriptor::new(
    "Schema Access Strategy",
    "How the registry resolves schemas: 'Name' for explicit lookup, 'Infer' for runtime inference.",
)
.default_value("Name")
.allowed_values(&["Name", "Infer"]);

impl ControllerService for SchemaRegistry {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        // Parse schema access strategy.
        if let Some(strategy) = properties.get("Schema Access Strategy") {
            self.schema_access_strategy = match strategy.as_str() {
                "Name" => SchemaAccessStrategy::Name,
                "Infer" => SchemaAccessStrategy::Infer,
                other => {
                    return Err(PluginError::ProcessingFailed(format!(
                        "invalid Schema Access Strategy: '{other}', expected 'Name' or 'Infer'"
                    )));
                }
            };
        }

        // Parse dynamic schema definitions from properties with "schema." prefix.
        // Each property value is a JSON array of field definitions:
        // [{"name": "field_name", "type": "String", "nullable": false}, ...]
        for (key, value) in properties {
            if let Some(schema_name) = key.strip_prefix("schema.") {
                let schema = parse_json_schema_definition(schema_name, value)?;
                self.register(schema_name.to_string(), schema);
            }
        }

        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.enabled = true;
        let names = self.schema_names();
        tracing::info!(
            strategy = ?self.schema_access_strategy,
            schema_count = names.len(),
            "SchemaRegistry enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.enabled = false;
        tracing::info!("SchemaRegistry disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_SCHEMA_ACCESS_STRATEGY]
    }
}

/// Parse a JSON schema definition string into a `RecordSchema`.
///
/// Expected format: `[{"name": "field_name", "type": "String", "nullable": false}, ...]`
///
/// Supported types: "String", "Int", "Float", "Boolean", "Array", "Record"
fn parse_json_schema_definition(schema_name: &str, json: &str) -> ProcessResult<RecordSchema> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        PluginError::ProcessingFailed(format!("failed to parse schema '{schema_name}': {e}"))
    })?;

    let arr = value.as_array().ok_or_else(|| {
        PluginError::ProcessingFailed(format!(
            "schema '{schema_name}' must be a JSON array of field definitions"
        ))
    })?;

    let mut fields = Vec::with_capacity(arr.len());
    for (i, field_def) in arr.iter().enumerate() {
        let name = field_def["name"]
            .as_str()
            .ok_or_else(|| {
                PluginError::ProcessingFailed(format!(
                    "schema '{schema_name}' field {i}: missing 'name'"
                ))
            })?
            .to_string();

        let type_str = field_def["type"].as_str().unwrap_or("String");
        let field_type = parse_field_type(type_str).map_err(|e| {
            PluginError::ProcessingFailed(format!("schema '{schema_name}' field '{name}': {e}"))
        })?;

        let nullable = field_def["nullable"].as_bool().unwrap_or(true);

        fields.push(SchemaField {
            name,
            field_type,
            nullable,
        });
    }

    Ok(RecordSchema::new(Some(schema_name.to_string()), fields))
}

fn parse_field_type(s: &str) -> Result<RecordFieldType, String> {
    match s {
        "String" => Ok(RecordFieldType::String),
        "Int" => Ok(RecordFieldType::Int),
        "Float" => Ok(RecordFieldType::Float),
        "Boolean" => Ok(RecordFieldType::Boolean),
        "Array" => Ok(RecordFieldType::Array(Box::new(RecordFieldType::String))),
        "Record" => Ok(RecordFieldType::Record(Arc::new(RecordSchema::new(
            None,
            Vec::new(),
        )))),
        other => Err(format!("unsupported field type: '{other}'")),
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "SchemaRegistry",
        description: "In-memory schema registry for record-oriented processing",
        factory: || Box::new(SchemaRegistry::new()),
        tags: &["Record", "Schema"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::record::{RecordFieldType, RecordValue};

    fn make_record(fields: Vec<(&str, RecordValue)>) -> Record {
        Record::from_fields(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    #[test]
    fn register_and_lookup() {
        let registry = SchemaRegistry::new();
        let schema = RecordSchema::infer_from_record(&make_record(vec![
            ("name", RecordValue::String("test".to_string())),
            ("age", RecordValue::Int(25)),
        ]));

        registry.register("person".to_string(), schema);

        let found = registry.get_schema("person").unwrap();
        assert_eq!(
            found.field("name").unwrap().field_type,
            RecordFieldType::String
        );
        assert_eq!(found.field("age").unwrap().field_type, RecordFieldType::Int);
        assert_eq!(found.name, Some("person".to_string()));
    }

    #[test]
    fn lookup_missing() {
        let registry = SchemaRegistry::new();
        assert!(registry.get_schema("nonexistent").is_none());
    }

    #[test]
    fn remove_schema() {
        let registry = SchemaRegistry::new();
        let schema = RecordSchema::new(None, Vec::new());
        registry.register("test".to_string(), schema);
        assert!(registry.get_schema("test").is_some());

        registry.remove_schema("test");
        assert!(registry.get_schema("test").is_none());
    }

    #[test]
    fn schema_names() {
        let registry = SchemaRegistry::new();
        registry.register("a".to_string(), RecordSchema::new(None, Vec::new()));
        registry.register("b".to_string(), RecordSchema::new(None, Vec::new()));

        let mut names = registry.schema_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn infer_schema_from_records() {
        let records = vec![
            make_record(vec![
                ("name", RecordValue::String("Alice".to_string())),
                ("age", RecordValue::Int(30)),
            ]),
            make_record(vec![
                ("name", RecordValue::String("Bob".to_string())),
                ("age", RecordValue::Int(25)),
                ("email", RecordValue::String("bob@example.com".to_string())),
            ]),
        ];

        let schema = SchemaRegistry::infer_schema(&records, 10, Some("inferred".to_string()));

        assert_eq!(schema.name, Some("inferred".to_string()));
        assert!(schema.field("name").is_some());
        assert!(schema.field("age").is_some());
        assert!(schema.field("email").is_some());
        // "email" only in second record -> nullable.
        assert!(schema.field("email").unwrap().nullable);
    }

    #[test]
    fn infer_schema_empty() {
        let schema = SchemaRegistry::infer_schema(&[], 10, None);
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn controller_service_lifecycle() {
        let mut registry = SchemaRegistry::new();
        assert!(!registry.is_enabled());

        registry.enable().unwrap();
        assert!(registry.is_enabled());

        registry.disable().unwrap();
        assert!(!registry.is_enabled());
    }

    #[test]
    fn configure_schema_access_strategy() {
        let mut registry = SchemaRegistry::new();
        assert_eq!(
            registry.schema_access_strategy(),
            SchemaAccessStrategy::Name
        );

        let mut props = HashMap::new();
        props.insert("Schema Access Strategy".to_string(), "Infer".to_string());
        registry.on_configure(&props).unwrap();
        assert_eq!(
            registry.schema_access_strategy(),
            SchemaAccessStrategy::Infer
        );
    }

    #[test]
    fn configure_invalid_strategy() {
        let mut registry = SchemaRegistry::new();
        let mut props = HashMap::new();
        props.insert("Schema Access Strategy".to_string(), "Invalid".to_string());
        assert!(registry.on_configure(&props).is_err());
    }

    #[test]
    fn dynamic_schema_from_properties() {
        let mut registry = SchemaRegistry::new();
        let mut props = HashMap::new();
        props.insert(
            "schema.person".to_string(),
            r#"[{"name":"name","type":"String","nullable":false},{"name":"age","type":"Int","nullable":false}]"#.to_string(),
        );
        registry.on_configure(&props).unwrap();

        let schema = registry.get_schema("person").unwrap();
        assert_eq!(schema.name, Some("person".to_string()));
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(
            schema.field("name").unwrap().field_type,
            RecordFieldType::String
        );
        assert_eq!(
            schema.field("age").unwrap().field_type,
            RecordFieldType::Int
        );
        assert!(!schema.field("name").unwrap().nullable);
    }

    #[test]
    fn dynamic_schema_invalid_json() {
        let mut registry = SchemaRegistry::new();
        let mut props = HashMap::new();
        props.insert("schema.bad".to_string(), "not json".to_string());
        assert!(registry.on_configure(&props).is_err());
    }

    #[test]
    fn dynamic_schema_missing_field_name() {
        let mut registry = SchemaRegistry::new();
        let mut props = HashMap::new();
        props.insert(
            "schema.bad".to_string(),
            r#"[{"type":"String"}]"#.to_string(),
        );
        assert!(registry.on_configure(&props).is_err());
    }

    #[test]
    fn property_descriptors_include_strategy() {
        let registry = SchemaRegistry::new();
        let descriptors = registry.property_descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].name, "Schema Access Strategy");
    }

    #[test]
    fn overwrite_schema() {
        let registry = SchemaRegistry::new();
        let schema1 =
            RecordSchema::infer_from_record(&make_record(vec![("a", RecordValue::Int(1))]));
        registry.register("test".to_string(), schema1);

        let schema2 = RecordSchema::infer_from_record(&make_record(vec![(
            "b",
            RecordValue::String("x".to_string()),
        )]));
        registry.register("test".to_string(), schema2);

        let found = registry.get_schema("test").unwrap();
        // Should have the second schema's fields.
        assert!(found.field("b").is_some());
        assert!(found.field("a").is_none());
    }
}
