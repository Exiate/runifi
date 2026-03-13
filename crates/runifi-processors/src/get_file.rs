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

const PROP_MAX_FILE_SIZE: PropertyDescriptor = PropertyDescriptor::new(
    "Maximum File Size",
    "Maximum file size in bytes. Files larger than this are routed to failure. Default 100MB.",
)
.default_value("104857600");

const PROP_FILE_FILTER: PropertyDescriptor = PropertyDescriptor::new(
    "File Filter",
    "Glob pattern to filter filenames (e.g. '*.csv', 'data-*.json'). Empty means accept all.",
);

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

/// Simple glob matcher supporting `*` and `?` wildcards.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    glob_match_inner(&pat, &txt)
}

fn glob_match_inner(pattern: &[char], text: &[char]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = usize::MAX;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }

    pi == pattern.len()
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
        let max_file_size: u64 = context
            .get_property("Maximum File Size")
            .unwrap_or("104857600")
            .parse()
            .unwrap_or(104_857_600);
        let file_filter = context
            .get_property("File Filter")
            .as_str()
            .map(|s| s.to_string());

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
            .filter(|e| match &file_filter {
                Some(pattern) if !pattern.is_empty() => {
                    let name = e.file_name();
                    let name_str = name.to_string_lossy();
                    glob_matches(pattern, &name_str)
                }
                _ => true,
            })
            .take(batch_size)
            .collect();

        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::metadata(&path).map_err(PluginError::Io)?;

            // Enforce maximum file size.
            if metadata.len() > max_file_size {
                tracing::warn!(
                    path = %path.display(),
                    size = metadata.len(),
                    max = max_file_size,
                    "File exceeds maximum size limit"
                );
                let mut flowfile = session.create();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    flowfile.set_attribute(Arc::from("filename"), Arc::from(name));
                }
                flowfile.set_attribute(
                    Arc::from("file.size"),
                    Arc::from(metadata.len().to_string().as_str()),
                );
                session.transfer(flowfile, &REL_FAILURE);
                continue;
            }

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
            PROP_MAX_FILE_SIZE,
            PROP_FILE_FILTER,
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
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        input_dir: String,
        max_file_size: Option<String>,
        file_filter: Option<String>,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Input Directory" => PropertyValue::String(self.input_dir.clone()),
                "Keep Source File" => PropertyValue::String("true".to_string()),
                "Maximum File Size" => match &self.max_file_size {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
                "File Filter" => match &self.file_filter {
                    Some(v) => PropertyValue::String(v.clone()),
                    None => PropertyValue::Unset,
                },
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

    struct CollectorSession {
        transferred: Vec<(FlowFile, &'static str)>,
        next_id: u64,
    }

    impl CollectorSession {
        fn new() -> Self {
            Self {
                transferred: Vec::new(),
                next_id: 1,
            }
        }
    }

    impl ProcessSession for CollectorSession {
        fn get(&mut self) -> Option<FlowFile> {
            None
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            vec![]
        }
        fn read_content(&self, _ff: &FlowFile) -> ProcessResult<Bytes> {
            Ok(Bytes::new())
        }
        fn write_content(&mut self, mut ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            ff.size = data.len() as u64;
            Ok(ff)
        }
        fn create(&mut self) -> FlowFile {
            let id = self.next_id;
            self.next_id += 1;
            FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: 0,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            }
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
    fn rejects_oversized_files() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-maxsize");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create a file larger than our limit.
        std::fs::write(tmp.join("big.dat"), vec![0u8; 200]).unwrap();
        std::fs::write(tmp.join("small.dat"), vec![0u8; 50]).unwrap();

        let mut proc = GetFile::new();
        let ctx = TestContext {
            input_dir: tmp.to_string_lossy().to_string(),
            max_file_size: Some("100".to_string()),
            file_filter: None,
        };
        let mut session = CollectorSession::new();

        proc.on_trigger(&ctx, &mut session).unwrap();

        let success_count = session
            .transferred
            .iter()
            .filter(|(_, r)| *r == "success")
            .count();
        let failure_count = session
            .transferred
            .iter()
            .filter(|(_, r)| *r == "failure")
            .count();
        assert_eq!(success_count, 1);
        assert_eq!(failure_count, 1);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn file_filter_glob() {
        let tmp = std::env::temp_dir().join("runifi-test-getfile-filter");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        std::fs::write(tmp.join("data.csv"), b"a,b").unwrap();
        std::fs::write(tmp.join("data.json"), b"{}").unwrap();
        std::fs::write(tmp.join("readme.txt"), b"hi").unwrap();

        let mut proc = GetFile::new();
        let ctx = TestContext {
            input_dir: tmp.to_string_lossy().to_string(),
            max_file_size: None,
            file_filter: Some("*.csv".to_string()),
        };
        let mut session = CollectorSession::new();

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");
        let filename = session.transferred[0].0.get_attribute("filename").unwrap();
        assert_eq!(filename.as_ref(), "data.csv");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn glob_matcher_works() {
        assert!(glob_matches("*.csv", "data.csv"));
        assert!(!glob_matches("*.csv", "data.json"));
        assert!(glob_matches("data-*", "data-2024.csv"));
        assert!(glob_matches("*.?sv", "file.csv"));
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("file.txt", "file.txt"));
        assert!(!glob_matches("file.txt", "other.txt"));
    }
}
