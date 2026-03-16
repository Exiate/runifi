//! JsonRecordReaderService — controller service wrapper for JsonRecordReader.

use std::collections::HashMap;

use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};

use crate::record::json_reader::JsonRecordReader;

/// Controller service that provides a shared [`JsonRecordReader`] instance.
///
/// Processors reference this service to parse JSON data into records
/// without instantiating their own reader.
pub struct JsonRecordReaderService {
    enabled: bool,
    reader: Option<JsonRecordReader>,
}

impl JsonRecordReaderService {
    pub fn new() -> Self {
        Self {
            enabled: false,
            reader: None,
        }
    }

    /// Get the configured record reader.
    pub fn reader(&self) -> Option<&JsonRecordReader> {
        self.reader.as_ref()
    }
}

impl Default for JsonRecordReaderService {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for JsonRecordReaderService {
    fn on_configure(&mut self, _properties: &HashMap<String, String>) -> ProcessResult {
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        self.reader = Some(JsonRecordReader::new());
        self.enabled = true;
        tracing::info!("JsonRecordReaderService enabled");
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.reader = None;
        self.enabled = false;
        tracing::info!("JsonRecordReaderService disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "JsonRecordReader",
        description: "Parses JSON arrays and line-delimited JSON into records",
        factory: || Box::new(JsonRecordReaderService::new()),
        tags: &["Record", "JSON"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle() {
        let mut svc = JsonRecordReaderService::new();
        assert!(!svc.is_enabled());
        assert!(svc.reader().is_none());

        svc.enable().unwrap();
        assert!(svc.is_enabled());
        assert!(svc.reader().is_some());

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
        assert!(svc.reader().is_none());
    }

    #[test]
    fn configure_and_validate() {
        let mut svc = JsonRecordReaderService::new();
        svc.on_configure(&HashMap::new()).unwrap();
        svc.validate().unwrap();
    }
}
