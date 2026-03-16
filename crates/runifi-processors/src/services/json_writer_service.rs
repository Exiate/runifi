//! JsonRecordWriterService — controller service wrapper for JsonRecordWriter.

use std::collections::HashMap;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

use crate::record::json_writer::{JsonOutputFormat, JsonRecordWriter};

/// Controller service that provides a shared [`JsonRecordWriter`] instance.
///
/// Configurable output format (array or line-delimited) and pretty-printing.
pub struct JsonRecordWriterService {
    enabled: bool,
    output_format: JsonOutputFormat,
    pretty_print: bool,
    writer: Option<JsonRecordWriter>,
}

const PROP_OUTPUT_FORMAT: PropertyDescriptor = PropertyDescriptor::new(
    "Output Format",
    "The JSON output format: 'array' for a JSON array, 'line-delimited' for NDJSON.",
)
.default_value("array")
.allowed_values(&["array", "line-delimited"]);

const PROP_PRETTY_PRINT: PropertyDescriptor = PropertyDescriptor::new(
    "Pretty Print",
    "Whether to pretty-print the JSON output with indentation.",
)
.default_value("false")
.allowed_values(&["true", "false"]);

impl JsonRecordWriterService {
    pub fn new() -> Self {
        Self {
            enabled: false,
            output_format: JsonOutputFormat::Array,
            pretty_print: false,
            writer: None,
        }
    }

    /// Get the configured record writer.
    pub fn writer(&self) -> Option<&JsonRecordWriter> {
        self.writer.as_ref()
    }
}

impl Default for JsonRecordWriterService {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for JsonRecordWriterService {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        if let Some(format) = properties.get("Output Format") {
            self.output_format = match format.as_str() {
                "array" => JsonOutputFormat::Array,
                "line-delimited" => JsonOutputFormat::LineDelimited,
                other => {
                    return Err(PluginError::ProcessingFailed(format!(
                        "invalid Output Format: '{other}', expected 'array' or 'line-delimited'"
                    )));
                }
            };
        }
        if let Some(pretty) = properties.get("Pretty Print") {
            self.pretty_print = pretty == "true";
        }
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        let mut writer = JsonRecordWriter::new(self.output_format);
        if self.pretty_print {
            writer = writer.pretty();
        }
        self.writer = Some(writer);
        self.enabled = true;
        tracing::info!(
            format = ?self.output_format,
            pretty = self.pretty_print,
            "JsonRecordWriterService enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.writer = None;
        self.enabled = false;
        tracing::info!("JsonRecordWriterService disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_OUTPUT_FORMAT, PROP_PRETTY_PRINT]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "JsonRecordWriter",
        description: "Serializes records into JSON arrays or line-delimited JSON",
        factory: || Box::new(JsonRecordWriterService::new()),
        tags: &["Record", "JSON"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_descriptors_returned() {
        let svc = JsonRecordWriterService::new();
        let descriptors = svc.property_descriptors();
        assert_eq!(descriptors.len(), 2);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Output Format"));
        assert!(names.contains(&"Pretty Print"));
    }

    #[test]
    fn lifecycle_default() {
        let mut svc = JsonRecordWriterService::new();
        assert!(!svc.is_enabled());

        svc.on_configure(&HashMap::new()).unwrap();
        svc.validate().unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());
        assert!(svc.writer().is_some());

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
    }

    #[test]
    fn configure_line_delimited() {
        let mut svc = JsonRecordWriterService::new();
        let mut props = HashMap::new();
        props.insert("Output Format".to_string(), "line-delimited".to_string());
        props.insert("Pretty Print".to_string(), "true".to_string());
        svc.on_configure(&props).unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());
    }

    #[test]
    fn configure_invalid_format() {
        let mut svc = JsonRecordWriterService::new();
        let mut props = HashMap::new();
        props.insert("Output Format".to_string(), "invalid".to_string());
        assert!(svc.on_configure(&props).is_err());
    }
}
