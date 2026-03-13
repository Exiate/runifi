//! In-memory SchemaRegistry — a ControllerService for managing RecordSchemas.
//!
//! Processors can look up schemas by name through the service registry.
//! Schemas can be registered programmatically or inferred from data.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::record::{Record, RecordSchema};
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

/// In-memory schema registry.
///
/// Provides thread-safe storage and retrieval of [`RecordSchema`]s by name.
/// Registered as a controller service so processors can look up shared schemas.
pub struct SchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, Arc<RecordSchema>>>>,
    enabled: bool,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            enabled: false,
        }
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

impl ControllerService for SchemaRegistry {
    fn on_configure(&mut self, _properties: &HashMap<String, String>) -> ProcessResult {
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.enabled = true;
        tracing::info!("SchemaRegistry enabled");
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
        Vec::new()
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "SchemaRegistry",
        description: "In-memory schema registry for record-oriented processing",
        factory: || Box::new(SchemaRegistry::new()),
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
