//! CsvRecordWriterService — controller service wrapper for CsvRecordWriter.

use std::collections::HashMap;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

use crate::record::csv_writer::CsvRecordWriter;

/// Controller service that provides a shared [`CsvRecordWriter`] instance.
///
/// Configurable delimiter and header writing for CSV output.
pub struct CsvRecordWriterService {
    enabled: bool,
    delimiter: u8,
    write_header: bool,
    writer: Option<CsvRecordWriter>,
}

const PROP_DELIMITER: PropertyDescriptor = PropertyDescriptor::new(
    "Delimiter",
    "The character used to separate fields in the CSV output.",
)
.default_value(",");

const PROP_WRITE_HEADER: PropertyDescriptor = PropertyDescriptor::new(
    "Write Header",
    "Whether to write a header row with field names.",
)
.default_value("true")
.allowed_values(&["true", "false"]);

impl CsvRecordWriterService {
    pub fn new() -> Self {
        Self {
            enabled: false,
            delimiter: b',',
            write_header: true,
            writer: None,
        }
    }

    /// Get the configured record writer.
    pub fn writer(&self) -> Option<&CsvRecordWriter> {
        self.writer.as_ref()
    }
}

impl Default for CsvRecordWriterService {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for CsvRecordWriterService {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        if let Some(delim) = properties.get("Delimiter") {
            let bytes = delim.as_bytes();
            if bytes.len() != 1 {
                return Err(PluginError::ProcessingFailed(format!(
                    "Delimiter must be a single character, got: '{delim}'"
                )));
            }
            self.delimiter = bytes[0];
        }
        if let Some(header) = properties.get("Write Header") {
            self.write_header = header == "true";
        }
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.writer = Some(
            CsvRecordWriter::new()
                .delimiter(self.delimiter)
                .write_header(self.write_header),
        );
        self.enabled = true;
        tracing::info!(
            delimiter = %String::from(self.delimiter as char),
            write_header = self.write_header,
            "CsvRecordWriterService enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.writer = None;
        self.enabled = false;
        tracing::info!("CsvRecordWriterService disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_DELIMITER, PROP_WRITE_HEADER]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "CsvRecordWriter",
        description: "Serializes records into CSV format with configurable delimiter and headers",
        factory: || Box::new(CsvRecordWriterService::new()),
        tags: &["Record", "CSV"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_descriptors_returned() {
        let svc = CsvRecordWriterService::new();
        let descriptors = svc.property_descriptors();
        assert_eq!(descriptors.len(), 2);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Delimiter"));
        assert!(names.contains(&"Write Header"));
    }

    #[test]
    fn lifecycle_default() {
        let mut svc = CsvRecordWriterService::new();
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
    fn configure_pipe_delimiter_no_header() {
        let mut svc = CsvRecordWriterService::new();
        let mut props = HashMap::new();
        props.insert("Delimiter".to_string(), "|".to_string());
        props.insert("Write Header".to_string(), "false".to_string());
        svc.on_configure(&props).unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());
    }

    #[test]
    fn configure_invalid_delimiter() {
        let mut svc = CsvRecordWriterService::new();
        let mut props = HashMap::new();
        props.insert("Delimiter".to_string(), "ab".to_string());
        assert!(svc.on_configure(&props).is_err());
    }
}
