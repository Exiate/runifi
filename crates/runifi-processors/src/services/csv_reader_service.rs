//! CsvRecordReaderService — controller service wrapper for CsvRecordReader.

use std::collections::HashMap;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

use crate::record::csv_reader::CsvRecordReader;

/// Controller service that provides a shared [`CsvRecordReader`] instance.
///
/// Configurable delimiter and header handling for CSV parsing.
pub struct CsvRecordReaderService {
    enabled: bool,
    delimiter: u8,
    has_header: bool,
    reader: Option<CsvRecordReader>,
}

const PROP_DELIMITER: PropertyDescriptor = PropertyDescriptor::new(
    "Delimiter",
    "The character used to separate fields in the CSV data.",
)
.default_value(",");

const PROP_HAS_HEADER: PropertyDescriptor = PropertyDescriptor::new(
    "Has Header",
    "Whether the CSV data has a header row with field names.",
)
.default_value("true")
.allowed_values(&["true", "false"]);

impl CsvRecordReaderService {
    pub fn new() -> Self {
        Self {
            enabled: false,
            delimiter: b',',
            has_header: true,
            reader: None,
        }
    }

    /// Get the configured record reader.
    pub fn reader(&self) -> Option<&CsvRecordReader> {
        self.reader.as_ref()
    }
}

impl Default for CsvRecordReaderService {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for CsvRecordReaderService {
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
        if let Some(header) = properties.get("Has Header") {
            self.has_header = header == "true";
        }
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.reader = Some(
            CsvRecordReader::new()
                .delimiter(self.delimiter)
                .has_header(self.has_header),
        );
        self.enabled = true;
        tracing::info!(
            delimiter = %String::from(self.delimiter as char),
            has_header = self.has_header,
            "CsvRecordReaderService enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.reader = None;
        self.enabled = false;
        tracing::info!("CsvRecordReaderService disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_DELIMITER, PROP_HAS_HEADER]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "CsvRecordReader",
        description: "Parses CSV data into records with configurable delimiter and header handling",
        factory: || Box::new(CsvRecordReaderService::new()),
        tags: &["Record", "CSV"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_descriptors_returned() {
        let svc = CsvRecordReaderService::new();
        let descriptors = svc.property_descriptors();
        assert_eq!(descriptors.len(), 2);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Delimiter"));
        assert!(names.contains(&"Has Header"));
    }

    #[test]
    fn lifecycle_default() {
        let mut svc = CsvRecordReaderService::new();
        assert!(!svc.is_enabled());

        svc.on_configure(&HashMap::new()).unwrap();
        svc.validate().unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());
        assert!(svc.reader().is_some());

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
    }

    #[test]
    fn configure_tab_delimiter() {
        let mut svc = CsvRecordReaderService::new();
        let mut props = HashMap::new();
        props.insert("Delimiter".to_string(), "\t".to_string());
        props.insert("Has Header".to_string(), "false".to_string());
        svc.on_configure(&props).unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());
    }

    #[test]
    fn configure_invalid_delimiter() {
        let mut svc = CsvRecordReaderService::new();
        let mut props = HashMap::new();
        props.insert("Delimiter".to_string(), "ab".to_string());
        assert!(svc.on_configure(&props).is_err());
    }
}
