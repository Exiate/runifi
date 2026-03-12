use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_OUTPUT_DIR: PropertyDescriptor = PropertyDescriptor::new(
    "Output Directory",
    "The directory to write FlowFile content to",
)
.required();

const PROP_CONFLICT_STRATEGY: PropertyDescriptor = PropertyDescriptor::new(
    "Conflict Resolution",
    "Strategy when file already exists: 'fail', 'replace', or 'ignore'",
)
.default_value("fail");

/// Writes FlowFile content to files in a directory.
pub struct PutFile;

impl PutFile {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PutFile {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for PutFile {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let output_dir = context
            .get_property("Output Directory")
            .as_str()
            .map(|s| s.to_string())
            .ok_or(PluginError::PropertyRequired("Output Directory"))?;
        let conflict_strategy = context
            .get_property("Conflict Resolution")
            .unwrap_or("fail")
            .to_string();

        let dir = std::path::Path::new(&output_dir);

        // Create directory if it doesn't exist.
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(PluginError::Io)?;
        }

        while let Some(flowfile) = session.get() {
            let filename = flowfile
                .get_attribute("filename")
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("flowfile-{}", flowfile.id));

            let path = dir.join(&filename);

            // Handle conflict.
            if path.exists() {
                match conflict_strategy.as_str() {
                    "replace" => {} // proceed to overwrite
                    "ignore" => {
                        session.transfer(flowfile, &REL_SUCCESS);
                        continue;
                    }
                    _ => {
                        // "fail" or unknown
                        tracing::warn!(path = %path.display(), "File already exists");
                        session.transfer(flowfile, &REL_FAILURE);
                        continue;
                    }
                }
            }

            let content = session.read_content(&flowfile)?;
            std::fs::write(&path, &content).map_err(PluginError::Io)?;

            tracing::debug!(
                path = %path.display(),
                size = content.len(),
                "Wrote FlowFile to disk"
            );

            session.transfer(flowfile, &REL_SUCCESS);
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_OUTPUT_DIR, PROP_CONFLICT_STRATEGY]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "PutFile",
        description: "Writes FlowFile content to files in a directory",
        factory: || Box::new(PutFile::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use std::sync::Arc;

    struct TestContext {
        output_dir: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Output Directory" => PropertyValue::String(self.output_dir.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-put"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct OneFlowFileSession {
        input: Option<FlowFile>,
        content: Bytes,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl ProcessSession for OneFlowFileSession {
        fn get(&mut self) -> Option<FlowFile> {
            self.input.take()
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            self.input.take().into_iter().collect()
        }
        fn read_content(&self, _ff: &FlowFile) -> ProcessResult<Bytes> {
            Ok(self.content.clone())
        }
        fn write_content(&mut self, ff: FlowFile, _data: Bytes) -> ProcessResult<FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> FlowFile {
            unimplemented!()
        }
        fn clone_flowfile(&mut self, _ff: &FlowFile) -> FlowFile {
            unimplemented!()
        }
        fn transfer(&mut self, ff: FlowFile, rel: &Relationship) {
            self.transferred.push((ff, rel.name));
        }
        fn remove(&mut self, _ff: FlowFile) {}
        fn penalize(&mut self, ff: FlowFile) -> FlowFile {
            ff
        }
        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    #[test]
    fn writes_file_to_directory() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext {
            output_dir: tmp.to_string_lossy().to_string(),
        };

        let mut ff = FlowFile {
            id: 1,
            attributes: Vec::new(),
            content_claim: None,
            size: 5,
            created_at_nanos: 0,
            lineage_start_id: 1,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from("filename"), Arc::from("output.txt"));

        let mut session = OneFlowFileSession {
            input: Some(ff),
            content: Bytes::from_static(b"hello"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let written = std::fs::read_to_string(tmp.join("output.txt")).unwrap();
        assert_eq!(written, "hello");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
