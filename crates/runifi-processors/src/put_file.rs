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
///
/// Uses atomic writes (temp file + rename) to prevent partial writes.
/// Sanitizes filenames to prevent path traversal attacks.
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

/// Sanitize a filename against path traversal.
///
/// Rejects absolute paths and `..` components. Returns the sanitized
/// file name component, or `None` if the filename is unsafe.
fn sanitize_filename(filename: &str) -> Option<String> {
    let path = std::path::Path::new(filename);

    // Reject absolute paths.
    if path.is_absolute() {
        return None;
    }

    // Reject any path with `..` components.
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => return None,
            std::path::Component::RootDir => return None,
            std::path::Component::Prefix(_) => return None,
            _ => {}
        }
    }

    // Use only the final filename component to prevent subdirectory creation
    // via embedded path separators like "subdir/file.txt".
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
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
            let raw_filename = flowfile
                .get_attribute("filename")
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("flowfile-{}", flowfile.id));

            // Sanitize filename against path traversal.
            let filename = match sanitize_filename(&raw_filename) {
                Some(name) => name,
                None => {
                    tracing::warn!(
                        filename = %raw_filename,
                        "Rejected filename: path traversal or absolute path detected"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

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

            // Atomic write: write to temp file, then rename.
            let tmp_path = dir.join(format!(".{}.tmp", filename));
            std::fs::write(&tmp_path, &content).map_err(PluginError::Io)?;
            std::fs::rename(&tmp_path, &path).map_err(PluginError::Io)?;

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

    fn make_ff(id: u64, filename: &str) -> FlowFile {
        let mut ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 5,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from("filename"), Arc::from(filename));
        ff
    }

    #[test]
    fn writes_file_to_directory() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext {
            output_dir: tmp.to_string_lossy().to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_ff(1, "output.txt")),
            content: Bytes::from_static(b"hello"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let written = std::fs::read_to_string(tmp.join("output.txt")).unwrap();
        assert_eq!(written, "hello");

        // Verify no temp file left behind.
        assert!(!tmp.join(".output.txt.tmp").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn rejects_path_traversal_dotdot() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-traversal");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext {
            output_dir: tmp.to_string_lossy().to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_ff(1, "../../../etc/passwd")),
            content: Bytes::from_static(b"malicious"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn rejects_absolute_path() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-abs");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext {
            output_dir: tmp.to_string_lossy().to_string(),
        };

        let mut session = OneFlowFileSession {
            input: Some(make_ff(1, "/etc/passwd")),
            content: Bytes::from_static(b"malicious"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sanitize_filename_strips_subdirectories() {
        assert_eq!(
            sanitize_filename("subdir/file.txt"),
            Some("file.txt".to_string())
        );
    }

    #[test]
    fn sanitize_filename_rejects_dotdot() {
        assert_eq!(sanitize_filename("../secret.txt"), None);
        assert_eq!(sanitize_filename("foo/../../bar.txt"), None);
    }

    #[test]
    fn sanitize_filename_allows_normal_names() {
        assert_eq!(
            sanitize_filename("my-file.dat"),
            Some("my-file.dat".to_string())
        );
        assert_eq!(
            sanitize_filename("data.2024.csv"),
            Some("data.2024.csv".to_string())
        );
    }
}
