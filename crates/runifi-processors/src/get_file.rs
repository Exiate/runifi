use std::path::{Component, Path};
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

const PROP_FOLLOW_SYMLINKS: PropertyDescriptor = PropertyDescriptor::new(
    "Follow Symlinks",
    "If true, follow symbolic links when reading files (true/false)",
)
.default_value("false");

/// Validate that the input directory path does not contain `..` components.
fn validate_directory_path(dir_str: &str) -> Result<(), PluginError> {
    let path = Path::new(dir_str);
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(PluginError::ProcessingFailed(format!(
                "Input directory contains '..' component: {dir_str}"
            )));
        }
    }
    Ok(())
}

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
        let follow_symlinks = context.get_property("Follow Symlinks").unwrap_or("false") == "true";

        // Validate the input directory path has no `..` components.
        validate_directory_path(&input_dir)?;

        let dir = Path::new(&input_dir);
        if !dir.is_dir() {
            return Err(PluginError::ProcessingFailed(format!(
                "Input directory does not exist: {}",
                input_dir
            )));
        }

        // Canonicalize the input directory to resolve symlinks at the directory level.
        let canonical_dir = dir.canonicalize().map_err(PluginError::Io)?;

        let entries: Vec<_> = std::fs::read_dir(&canonical_dir)
            .map_err(PluginError::Io)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let ft = match e.file_type() {
                    Ok(ft) => ft,
                    Err(_) => return false,
                };
                if ft.is_file() {
                    return true;
                }
                if ft.is_symlink() && follow_symlinks {
                    // Check if symlink target is a file.
                    e.path().metadata().ok().is_some_and(|m| m.is_file())
                } else {
                    false
                }
            })
            .take(batch_size)
            .collect();

        for entry in entries {
            let path = entry.path();

            // Verify the file's canonical path is within the canonical input directory.
            // This prevents symlink escape attacks.
            let canonical_file = match path.canonicalize() {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to canonicalize file path, skipping"
                    );
                    continue;
                }
            };

            if !canonical_file.starts_with(&canonical_dir) {
                tracing::warn!(
                    path = %path.display(),
                    canonical = %canonical_file.display(),
                    dir = %canonical_dir.display(),
                    "Symlink escape detected: file resolves outside input directory, skipping"
                );
                continue;
            }

            let metadata = std::fs::metadata(&canonical_file).map_err(PluginError::Io)?;
            let data = std::fs::read(&canonical_file).map_err(PluginError::Io)?;

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
            if !keep_source && let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(path = %path.display(), error = %e, "Failed to delete source file");
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SUCCESS, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_INPUT_DIR,
            PROP_KEEP_SOURCE,
            PROP_BATCH_SIZE,
            PROP_FOLLOW_SYMLINKS,
        ]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "GetFile",
        description: "Watches a directory and ingests files as FlowFiles",
        factory: || Box::new(GetFile::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;
    use runifi_plugin_api::relationship::Relationship;
    use runifi_plugin_api::result::ProcessResult;

    struct TestContext {
        input_dir: String,
        follow_symlinks: bool,
        keep_source: bool,
    }

    impl TestContext {
        fn new(input_dir: String) -> Self {
            Self {
                input_dir,
                follow_symlinks: false,
                keep_source: false,
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Input Directory" => PropertyValue::String(self.input_dir.clone()),
                "Follow Symlinks" => PropertyValue::String(
                    if self.follow_symlinks {
                        "true"
                    } else {
                        "false"
                    }
                    .to_string(),
                ),
                "Keep Source File" => PropertyValue::String(
                    if self.keep_source { "true" } else { "false" }.to_string(),
                ),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-get"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct CollectingSession {
        created_count: u64,
        flowfiles: Vec<(FlowFile, &'static str)>,
    }

    impl CollectingSession {
        fn new() -> Self {
            Self {
                created_count: 0,
                flowfiles: Vec::new(),
            }
        }
    }

    impl ProcessSession for CollectingSession {
        fn get(&mut self) -> Option<FlowFile> {
            None
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            Vec::new()
        }
        fn read_content(&self, _ff: &FlowFile) -> ProcessResult<Bytes> {
            Ok(Bytes::new())
        }
        fn write_content(&mut self, ff: FlowFile, _data: Bytes) -> ProcessResult<FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> FlowFile {
            self.created_count += 1;
            FlowFile {
                id: self.created_count,
                attributes: Vec::new(),
                content_claim: None,
                size: 0,
                created_at_nanos: 0,
                lineage_start_id: self.created_count,
                penalized_until_nanos: 0,
            }
        }
        fn clone_flowfile(&mut self, _ff: &FlowFile) -> FlowFile {
            unimplemented!()
        }
        fn transfer(&mut self, ff: FlowFile, rel: &Relationship) {
            self.flowfiles.push((ff, rel.name));
        }
        fn remove(&mut self, _ff: FlowFile) {}
        fn penalize(&mut self, ff: FlowFile) -> FlowFile {
            ff
        }
        fn commit(&mut self) {}
        fn rollback(&mut self) {}
    }

    #[test]
    fn rejects_input_dir_with_dot_dot() {
        let mut proc = GetFile::new();
        let ctx = TestContext::new("/tmp/../etc".to_string());
        let mut session = CollectingSession::new();

        let result = proc.on_trigger(&ctx, &mut session);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains(".."), "Error should mention '..'");
    }

    #[test]
    fn reads_normal_directory() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-normal");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a test file.
        std::fs::write(tmp.join("test.txt"), b"hello").unwrap();

        let mut proc = GetFile::new();
        let mut ctx = TestContext::new(tmp.to_string_lossy().to_string());
        ctx.follow_symlinks = false;

        let mut session = CollectingSession::new();
        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.flowfiles.len(), 1);
        assert_eq!(session.flowfiles[0].1, "success");

        let ff = &session.flowfiles[0].0;
        assert_eq!(
            ff.get_attribute("filename").map(|v| v.as_ref()),
            Some("test.txt")
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn skips_symlinks_when_follow_disabled() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-nosymlink");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a real file and a symlink to /etc/hostname.
        std::fs::write(tmp.join("real.txt"), b"real").unwrap();
        // Create symlink pointing outside the directory.
        let symlink_path = tmp.join("escape.txt");
        let _ = std::os::unix::fs::symlink("/etc/hostname", &symlink_path);

        let mut proc = GetFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());
        let mut session = CollectingSession::new();

        proc.on_trigger(&ctx, &mut session).unwrap();

        // Only the real file should be ingested (symlink skipped due to is_file() check on DirEntry).
        assert_eq!(session.flowfiles.len(), 1);
        assert_eq!(
            session.flowfiles[0]
                .0
                .get_attribute("filename")
                .map(|v| v.as_ref()),
            Some("real.txt")
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn blocks_symlink_escape_when_follow_enabled() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-symlink-escape");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a symlink pointing outside the input directory.
        let symlink_path = tmp.join("escape.txt");
        let _ = std::os::unix::fs::symlink("/etc/hostname", &symlink_path);

        let mut proc = GetFile::new();
        let mut ctx = TestContext::new(tmp.to_string_lossy().to_string());
        ctx.follow_symlinks = true;

        let mut session = CollectingSession::new();
        proc.on_trigger(&ctx, &mut session).unwrap();

        // Symlink pointing outside directory should be skipped.
        assert_eq!(session.flowfiles.len(), 0);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn allows_internal_symlinks_when_follow_enabled() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-internal-symlink");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a real file and a symlink pointing to it (within same dir).
        let real_file = tmp.join("original.txt");
        std::fs::write(&real_file, b"content").unwrap();
        let symlink_path = tmp.join("link.txt");
        let _ = std::os::unix::fs::symlink(&real_file, &symlink_path);

        let mut proc = GetFile::new();
        let mut ctx = TestContext::new(tmp.to_string_lossy().to_string());
        ctx.follow_symlinks = true;
        ctx.keep_source = true;

        let mut session = CollectingSession::new();
        proc.on_trigger(&ctx, &mut session).unwrap();

        // Both files should be ingested (symlink resolves within input dir).
        assert_eq!(session.flowfiles.len(), 2);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
