use std::sync::Arc;

use regex_lite::Regex;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

const REL_MATCHED: Relationship = Relationship::new(
    "matched",
    "FlowFiles where at least one regex pattern matched",
);
const REL_UNMATCHED: Relationship =
    Relationship::new("unmatched", "FlowFiles where no regex pattern matched");

const PROP_ENABLE_REPEATING: PropertyDescriptor = PropertyDescriptor::new(
    "Enable Repeating Capture Group",
    "If true, repeating capture groups produce indexed attributes (e.g. prefix.0, prefix.1)",
)
.default_value("false");

const PROP_INCLUDE_GROUP_ZERO: PropertyDescriptor = PropertyDescriptor::new(
    "Include Capture Group 0",
    "If true, include the entire regex match as an attribute (group 0)",
)
.default_value("true");

const PROP_MAX_CAPTURE_LENGTH: PropertyDescriptor = PropertyDescriptor::new(
    "Maximum Capture Group Length",
    "Maximum number of characters to store per captured value",
)
.default_value("1024");

/// The set of property names that are owned by this processor
/// and should not be treated as dynamic regex patterns.
const OWN_PROPERTY_NAMES: &[&str] = &[
    "Enable Repeating Capture Group",
    "Include Capture Group 0",
    "Maximum Capture Group Length",
];

/// Extracts text from FlowFile content using named regex capture groups
/// and stores matches as FlowFile attributes.
///
/// Dynamic properties define regex patterns: the property name becomes the
/// attribute prefix for captured groups, and the property value is the regex
/// pattern. Named capture groups produce attributes like `prefix.groupName`,
/// while unnamed groups produce `prefix.N` where N is the group index.
///
/// When "Enable Repeating Capture Group" is true, all matches of each pattern
/// are found and indexed (e.g. `prefix.groupName.0`, `prefix.groupName.1`).
pub struct ExtractText;

impl ExtractText {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ExtractText {
    fn default() -> Self {
        Self::new()
    }
}

/// A compiled regex pattern with its attribute prefix.
struct CompiledPattern {
    prefix: String,
    regex: Regex,
}

impl Processor for ExtractText {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let enable_repeating = context
            .get_property("Enable Repeating Capture Group")
            .unwrap_or("false")
            == "true";

        let include_group_zero = context
            .get_property("Include Capture Group 0")
            .unwrap_or("true")
            == "true";

        let max_capture_length: usize = context
            .get_property("Maximum Capture Group Length")
            .unwrap_or("1024")
            .parse()
            .unwrap_or(1024);

        // Collect dynamic properties as regex patterns.
        let all_names = context.property_names();
        let mut patterns: Vec<CompiledPattern> = Vec::new();

        for name in &all_names {
            if OWN_PROPERTY_NAMES.contains(&name.as_str()) {
                continue;
            }

            if let Some(pattern_str) = context.get_property(name).as_str() {
                if pattern_str.is_empty() {
                    continue;
                }
                match Regex::new(pattern_str) {
                    Ok(re) => {
                        patterns.push(CompiledPattern {
                            prefix: name.clone(),
                            regex: re,
                        });
                    }
                    Err(e) => {
                        tracing::error!(
                            prefix = name.as_str(),
                            pattern = pattern_str,
                            error = %e,
                            "Invalid regex pattern in dynamic property"
                        );
                    }
                }
            }
        }

        while let Some(mut flowfile) = session.get() {
            // Read the FlowFile content as text.
            let content_text = match session.read_content(&flowfile) {
                Ok(data) => String::from_utf8_lossy(&data).into_owned(),
                Err(e) => {
                    tracing::error!(
                        flowfile_id = flowfile.id,
                        error = %e,
                        "Failed to read FlowFile content"
                    );
                    session.transfer(flowfile, &REL_UNMATCHED);
                    continue;
                }
            };

            let mut any_match = false;

            for pattern in &patterns {
                if enable_repeating {
                    // Repeating mode: find all matches and index them.
                    for (match_index, caps) in
                        pattern.regex.captures_iter(&content_text).enumerate()
                    {
                        any_match = true;

                        // Group 0 (entire match).
                        if include_group_zero && let Some(m) = caps.get(0) {
                            let value = truncate(m.as_str(), max_capture_length);
                            let attr_name = format!("{}.{}", pattern.prefix, match_index);
                            flowfile.set_attribute(Arc::from(attr_name.as_str()), Arc::from(value));
                        }

                        // Named and numbered capture groups.
                        extract_capture_groups(
                            &caps,
                            &pattern.regex,
                            &pattern.prefix,
                            Some(match_index),
                            max_capture_length,
                            &mut flowfile,
                        );
                    }
                } else {
                    // Non-repeating mode: only use the first match.
                    if let Some(caps) = pattern.regex.captures(&content_text) {
                        any_match = true;

                        // Group 0 (entire match).
                        if include_group_zero && let Some(m) = caps.get(0) {
                            let value = truncate(m.as_str(), max_capture_length);
                            flowfile.set_attribute(
                                Arc::from(pattern.prefix.as_str()),
                                Arc::from(value),
                            );
                        }

                        // Named and numbered capture groups.
                        extract_capture_groups(
                            &caps,
                            &pattern.regex,
                            &pattern.prefix,
                            None,
                            max_capture_length,
                            &mut flowfile,
                        );
                    }
                }
            }

            if any_match {
                session.transfer(flowfile, &REL_MATCHED);
            } else {
                session.transfer(flowfile, &REL_UNMATCHED);
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_MATCHED, REL_UNMATCHED]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_ENABLE_REPEATING,
            PROP_INCLUDE_GROUP_ZERO,
            PROP_MAX_CAPTURE_LENGTH,
        ]
    }
}

/// Extract named and numbered capture groups from a match, setting them as
/// FlowFile attributes.
///
/// When `match_index` is `Some(i)`, attributes are indexed for repeating mode:
///   `prefix.groupName.i` or `prefix.groupIndex.i`
/// When `match_index` is `None`, attributes are flat:
///   `prefix.groupName` or `prefix.groupIndex`
fn extract_capture_groups(
    caps: &regex_lite::Captures<'_>,
    regex: &Regex,
    prefix: &str,
    match_index: Option<usize>,
    max_length: usize,
    flowfile: &mut runifi_plugin_api::FlowFile,
) {
    // Build a map of group index -> name for named groups.
    let group_names: Vec<Option<&str>> = regex.capture_names().collect();

    // Iterate over capture groups starting at 1 (skip group 0, handled separately).
    for (i, group_name) in group_names.iter().enumerate().skip(1) {
        if let Some(m) = caps.get(i) {
            let value = truncate(m.as_str(), max_length);
            let group_label = if let Some(name) = group_name {
                name.to_string()
            } else {
                i.to_string()
            };

            let attr_name = if let Some(idx) = match_index {
                format!("{}.{}.{}", prefix, group_label, idx)
            } else {
                format!("{}.{}", prefix, group_label)
            };

            flowfile.set_attribute(Arc::from(attr_name.as_str()), Arc::from(value));
        }
    }
}

/// Truncate a string to the given maximum character length.
fn truncate(s: &str, max_chars: usize) -> &str {
    if s.len() <= max_chars {
        // Fast path: ASCII strings shorter than limit.
        return s;
    }
    // Handle multi-byte chars correctly by finding the char boundary.
    match s.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => &s[..byte_idx],
        None => s,
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "ExtractText",
        description: "Extracts text from FlowFile content using regex capture groups and stores matches as attributes",
        factory: || Box::new(ExtractText::new()),
        tags: &["Text", "Extraction", "Regex"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        properties: Vec<(String, String)>,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            for (k, v) in &self.properties {
                if k == name {
                    return PropertyValue::String(v.clone());
                }
            }
            PropertyValue::Unset
        }
        fn property_names(&self) -> Vec<String> {
            self.properties.iter().map(|(k, _)| k.clone()).collect()
        }
        fn name(&self) -> &str {
            "test-extract"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct TestSession {
        inputs: Vec<FlowFile>,
        content: Bytes,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl ProcessSession for TestSession {
        fn get(&mut self) -> Option<FlowFile> {
            if self.inputs.is_empty() {
                None
            } else {
                Some(self.inputs.remove(0))
            }
        }
        fn get_batch(&mut self, _max: usize) -> Vec<FlowFile> {
            std::mem::take(&mut self.inputs)
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

    fn make_ff(id: u64) -> FlowFile {
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

    #[test]
    fn basic_named_capture_groups() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![(
                "date".to_string(),
                r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})".to_string(),
            )],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"Event on 2024-03-15 was logged."),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        // Group 0 (entire match) stored under the prefix name.
        assert_eq!(
            ff.get_attribute("date").map(|v| v.as_ref().to_string()),
            Some("2024-03-15".to_string())
        );
        // Named groups.
        assert_eq!(
            ff.get_attribute("date.year")
                .map(|v| v.as_ref().to_string()),
            Some("2024".to_string())
        );
        assert_eq!(
            ff.get_attribute("date.month")
                .map(|v| v.as_ref().to_string()),
            Some("03".to_string())
        );
        assert_eq!(
            ff.get_attribute("date.day").map(|v| v.as_ref().to_string()),
            Some("15".to_string())
        );
    }

    #[test]
    fn unnamed_capture_groups() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![("coords".to_string(), r"(\d+),(\d+)".to_string())],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"Position: 42,99"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("coords").map(|v| v.as_ref().to_string()),
            Some("42,99".to_string())
        );
        assert_eq!(
            ff.get_attribute("coords.1").map(|v| v.as_ref().to_string()),
            Some("42".to_string())
        );
        assert_eq!(
            ff.get_attribute("coords.2").map(|v| v.as_ref().to_string()),
            Some("99".to_string())
        );
    }

    #[test]
    fn repeating_capture_groups() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                (
                    "Enable Repeating Capture Group".to_string(),
                    "true".to_string(),
                ),
                (
                    "email".to_string(),
                    r"(?P<user>[a-zA-Z0-9.]+)@(?P<domain>[a-zA-Z0-9.]+)".to_string(),
                ),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from("Contact alice@example.com or bob@test.org"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        // First match (index 0).
        assert_eq!(
            ff.get_attribute("email.0").map(|v| v.as_ref().to_string()),
            Some("alice@example.com".to_string())
        );
        assert_eq!(
            ff.get_attribute("email.user.0")
                .map(|v| v.as_ref().to_string()),
            Some("alice".to_string())
        );
        assert_eq!(
            ff.get_attribute("email.domain.0")
                .map(|v| v.as_ref().to_string()),
            Some("example.com".to_string())
        );
        // Second match (index 1).
        assert_eq!(
            ff.get_attribute("email.1").map(|v| v.as_ref().to_string()),
            Some("bob@test.org".to_string())
        );
        assert_eq!(
            ff.get_attribute("email.user.1")
                .map(|v| v.as_ref().to_string()),
            Some("bob".to_string())
        );
        assert_eq!(
            ff.get_attribute("email.domain.1")
                .map(|v| v.as_ref().to_string()),
            Some("test.org".to_string())
        );
    }

    #[test]
    fn max_length_truncation() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                ("Maximum Capture Group Length".to_string(), "5".to_string()),
                ("text".to_string(), r"(?P<word>\w+)".to_string()),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"HelloWorldTest"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        // Group 0 should be truncated to 5 characters.
        assert_eq!(
            ff.get_attribute("text").map(|v| v.as_ref().to_string()),
            Some("Hello".to_string())
        );
        // Named group should also be truncated.
        assert_eq!(
            ff.get_attribute("text.word")
                .map(|v| v.as_ref().to_string()),
            Some("Hello".to_string())
        );
    }

    #[test]
    fn no_match_routes_to_unmatched() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![("digits".to_string(), r"(\d+)".to_string())],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"no digits here"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn exclude_group_zero() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                ("Include Capture Group 0".to_string(), "false".to_string()),
                (
                    "extract".to_string(),
                    r"(?P<first>\w+)\s+(?P<last>\w+)".to_string(),
                ),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"John Doe"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        // Group 0 should NOT be present.
        assert!(ff.get_attribute("extract").is_none());
        // Named groups should still be present.
        assert_eq!(
            ff.get_attribute("extract.first")
                .map(|v| v.as_ref().to_string()),
            Some("John".to_string())
        );
        assert_eq!(
            ff.get_attribute("extract.last")
                .map(|v| v.as_ref().to_string()),
            Some("Doe".to_string())
        );
    }

    #[test]
    fn multiple_patterns() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                ("ip".to_string(), r"(\d+\.\d+\.\d+\.\d+)".to_string()),
                ("port".to_string(), r":(\d+)".to_string()),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"Server at 192.168.1.1:8080"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("ip.1").map(|v| v.as_ref().to_string()),
            Some("192.168.1.1".to_string())
        );
        assert_eq!(
            ff.get_attribute("port.1").map(|v| v.as_ref().to_string()),
            Some("8080".to_string())
        );
    }

    #[test]
    fn partial_match_routes_to_matched() {
        // If one pattern matches but another does not, route to matched.
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                ("numbers".to_string(), r"(\d+)".to_string()),
                ("emails".to_string(), r"(\w+@\w+\.\w+)".to_string()),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"The value is 42"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        // At least one pattern matched, so route to matched.
        assert_eq!(session.transferred[0].1, "matched");
    }

    #[test]
    fn empty_content_no_match() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![("text".to_string(), r"(\w+)".to_string())],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::new(),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn no_dynamic_properties_routes_unmatched() {
        let mut proc = ExtractText::new();
        let ctx = TestContext { properties: vec![] };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"some text"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn truncate_handles_multibyte() {
        // Ensure truncation doesn't split multi-byte UTF-8 characters.
        let result = truncate("Hello", 3);
        assert_eq!(result, "Hel");

        let result = truncate("AB", 5);
        assert_eq!(result, "AB");

        // Multi-byte: each char is 3 bytes in UTF-8.
        let result = truncate("\u{00E9}\u{00E9}\u{00E9}", 2);
        assert_eq!(result, "\u{00E9}\u{00E9}");
    }

    #[test]
    fn repeating_unnamed_groups() {
        let mut proc = ExtractText::new();
        let ctx = TestContext {
            properties: vec![
                (
                    "Enable Repeating Capture Group".to_string(),
                    "true".to_string(),
                ),
                ("num".to_string(), r"(\d+)".to_string()),
            ],
        };

        let mut session = TestSession {
            inputs: vec![make_ff(1)],
            content: Bytes::from_static(b"values: 10, 20, 30"),
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "matched");

        let ff = &session.transferred[0].0;
        // Check indexed group 0 values.
        assert_eq!(
            ff.get_attribute("num.0").map(|v| v.as_ref().to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            ff.get_attribute("num.1").map(|v| v.as_ref().to_string()),
            Some("20".to_string())
        );
        assert_eq!(
            ff.get_attribute("num.2").map(|v| v.as_ref().to_string()),
            Some("30".to_string())
        );
        // Unnamed capture group 1 with indexing.
        assert_eq!(
            ff.get_attribute("num.1.0").map(|v| v.as_ref().to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            ff.get_attribute("num.1.1").map(|v| v.as_ref().to_string()),
            Some("20".to_string())
        );
        assert_eq!(
            ff.get_attribute("num.1.2").map(|v| v.as_ref().to_string()),
            Some("30".to_string())
        );
    }
}
