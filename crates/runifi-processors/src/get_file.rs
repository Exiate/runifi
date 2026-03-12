use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_INPUT_DIR: PropertyDescriptor = PropertyDescriptor::new(
    "Input Directory",
    "The directory to watch for incoming files",
)
.required();

const PROP_KEEP_SOURCE: PropertyDescriptor = PropertyDescriptor::new(
    "Keep Source File",
    "If true, do not delete the source file after ingestion (true/false)",
)
.default_value("false");

const PROP_BATCH_SIZE: PropertyDescriptor = PropertyDescriptor::new(
    "Batch Size",
    "Maximum number of files to ingest per trigger",
)
.default_value("10");

/// Watches a directory and ingests files as FlowFiles.
pub struct GetFile;

impl GetFile {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GetFile {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for GetFile {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let input_dir = context
            .get_property("Input Directory")
            .as_str()
            .map(|s| s.to_string())
            .ok_or(PluginError::PropertyRequired("Input Directory"))?;
        let keep_source = context.get_property("Keep Source File").unwrap_or("false") == "true";
        let batch_size: usize = context
            .get_property("Batch Size")
            .unwrap_or("10")
            .parse()
            .unwrap_or(10);

        let dir = std::path::Path::new(&input_dir);
        if !dir.is_dir() {
            return Err(PluginError::ProcessingFailed(format!(
                "Input directory does not exist: {}",
                input_dir
            )));
        }

        let entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(PluginError::Io)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().ok().is_some_and(|ft| ft.is_file()))
            .take(batch_size)
            .collect();

        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::metadata(&path).map_err(PluginError::Io)?;
            let data = std::fs::read(&path).map_err(PluginError::Io)?;

            let mut flowfile = session.create();

            // Set file attributes.
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                flowfile.set_attribute(Arc::from("filename"), Arc::from(name));
            }
            flowfile.set_attribute(
                Arc::from("path"),
                Arc::from(path.parent().unwrap_or(dir).to_string_lossy().as_ref()),
            );
            flowfile.set_attribute(
                Arc::from("file.size"),
                Arc::from(metadata.len().to_string().as_str()),
            );

            flowfile = session.write_content(flowfile, Bytes::from(data))?;
            session.transfer(flowfile, &REL_SUCCESS);

            // Delete source file unless keep_source is set.
            if !keep_source {
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to delete source file");
                }
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_INPUT_DIR, PROP_KEEP_SOURCE, PROP_BATCH_SIZE]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "GetFile",
        description: "Watches a directory and ingests files as FlowFiles",
        factory: || Box::new(GetFile::new()),
    }
}
