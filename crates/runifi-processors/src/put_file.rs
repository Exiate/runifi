use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

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

const PROP_FILE_PERMISSIONS: PropertyDescriptor = PropertyDescriptor::new(
    "File Permissions",
    "UNIX file permissions in octal (e.g. '0640')",
)
.default_value("0640");

const PROP_DIR_PERMISSIONS: PropertyDescriptor = PropertyDescriptor::new(
    "Directory Permissions",
    "UNIX directory permissions in octal (e.g. '0750')",
)
.default_value("0750");

/// Sanitize a filename by stripping path separators and `..` components,
/// rejecting null bytes, and collapsing to just the final file name.
fn sanitize_filename(raw: &str) -> Result<String, PluginError> {
    if raw.contains('\0') {
        return Err(PluginError::ProcessingFailed(
            "Filename contains null byte".to_string(),
        ));
    }

    let path = Path::new(raw);
    let mut sanitized = String::new();

    for component in path.components() {
        match component {
            Component::RootDir
            | Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_) => continue,
            Component::Normal(seg) => {
                if let Some(s) = seg.to_str() {
                    sanitized = s.to_string();
                }
            }
        }
    }

    if sanitized.is_empty() {
        return Err(PluginError::ProcessingFailed(
            "Filename is empty after sanitization".to_string(),
        ));
    }

    Ok(sanitized)
}

/// Validate that the resolved output path is within the canonical output directory.
fn validate_path_within_dir(path: &Path, canonical_dir: &Path) -> Result<PathBuf, PluginError> {
    let canonical_path = if let Some(parent) = path.parent() {
        if parent.exists() {
            parent
                .canonicalize()
                .map_err(PluginError::Io)?
                .join(path.file_name().unwrap_or_default())
        } else {
            path.to_path_buf()
        }
    } else {
        path.to_path_buf()
    };

    if !canonical_path.starts_with(canonical_dir) {
        return Err(PluginError::ProcessingFailed(format!(
            "Path traversal detected: resolved path '{}' is outside output directory '{}'",
            canonical_path.display(),
            canonical_dir.display()
        )));
    }

    Ok(canonical_path)
}

fn parse_octal_permissions(s: &str) -> Result<u32, PluginError> {
    let trimmed = s.trim_start_matches('0');
    let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
    u32::from_str_radix(trimmed, 8)
        .map_err(|_| PluginError::ProcessingFailed(format!("Invalid octal permissions: '{s}'")))
}

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
        let file_perms_str = context
            .get_property("File Permissions")
            .unwrap_or("0640")
            .to_string();
        let dir_perms_str = context
            .get_property("Directory Permissions")
            .unwrap_or("0750")
            .to_string();

        let file_mode = parse_octal_permissions(&file_perms_str)?;
        let dir_mode = parse_octal_permissions(&dir_perms_str)?;

        let dir = Path::new(&output_dir);

        // Create directory if it doesn't exist.
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(PluginError::Io)?;
        }

        // Set directory permissions.
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(dir_mode))
            .map_err(PluginError::Io)?;

        // Canonicalize the output directory for path traversal validation.
        let canonical_dir = dir.canonicalize().map_err(PluginError::Io)?;

        while let Some(flowfile) = session.get() {
            let raw_filename = flowfile
                .get_attribute("filename")
                .map(|v| v.to_string())
                .unwrap_or_else(|| format!("flowfile-{}", flowfile.id));

            // Sanitize the filename to prevent path traversal.
            let filename = match sanitize_filename(&raw_filename) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(
                        filename = %raw_filename,
                        error = %e,
                        "Rejected unsafe filename"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            let path = canonical_dir.join(&filename);

            // Verify the resolved path is within the output directory.
            if let Err(e) = validate_path_within_dir(&path, &canonical_dir) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Path traversal attempt blocked"
                );
                session.transfer(flowfile, &REL_FAILURE);
                continue;
            }

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

            // Set file permissions.
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(file_mode))
                .map_err(PluginError::Io)?;

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
        vec![
            PROP_OUTPUT_DIR,
            PROP_CONFLICT_STRATEGY,
            PROP_FILE_PERMISSIONS,
            PROP_DIR_PERMISSIONS,
        ]
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
        file_permissions: Option<String>,
        dir_permissions: Option<String>,
    }

    impl TestContext {
        fn new(output_dir: String) -> Self {
            Self {
                output_dir,
                file_permissions: None,
                dir_permissions: None,
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Output Directory" => PropertyValue::String(self.output_dir.clone()),
                "File Permissions" => match &self.file_permissions {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
                "Directory Permissions" => match &self.dir_permissions {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
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

    fn make_flowfile(id: u64, filename: &str) -> FlowFile {
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
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "output.txt")),
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

    #[test]
    fn sanitizes_path_traversal_with_dot_dot() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-traversal");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "../../etc/passwd")),
            content: Bytes::from_static(b"malicious"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        // The filename is sanitized to "passwd" and written safely within output dir.
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
        assert!(tmp.join("passwd").exists());
        let written = std::fs::read_to_string(tmp.join("passwd")).unwrap();
        assert_eq!(written, "malicious");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sanitizes_absolute_path_filename() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-abspath");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "/etc/shadow")),
            content: Bytes::from_static(b"malicious"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        // The filename is sanitized to "shadow" and written safely within output dir.
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
        assert!(tmp.join("shadow").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn rejects_null_byte_filename() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-null");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "test\0file.txt")),
            content: Bytes::from_static(b"malicious"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        // Should be routed to failure.
        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn allows_normal_filenames() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-normal");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "my-report-2024.csv")),
            content: Bytes::from_static(b"data"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
        assert!(tmp.join("my-report-2024.csv").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sets_file_permissions() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-perms");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let mut ctx = TestContext::new(tmp.to_string_lossy().to_string());
        ctx.file_permissions = Some("0600".to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "secure.txt")),
            content: Bytes::from_static(b"secret"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let metadata = std::fs::metadata(tmp.join("secure.txt")).unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sanitizes_mixed_traversal_attempts() {
        let tmp = std::env::temp_dir().join("runifi-test-putfile-mixed");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut proc = PutFile::new();
        let ctx = TestContext::new(tmp.to_string_lossy().to_string());

        let mut session = OneFlowFileSession {
            input: Some(make_flowfile(1, "./../../secret.txt")),
            content: Bytes::from_static(b"safe"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();
        assert_eq!(session.transferred[0].1, "success");
        assert!(tmp.join("secret.txt").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
