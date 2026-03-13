use std::sync::Arc;

use bytes::Bytes;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_ORIGINAL};

const REL_SPLITS: Relationship = Relationship::new("splits", "Individual split FlowFiles");

const PROP_BYTE_SEQUENCE: PropertyDescriptor = PropertyDescriptor::new(
    "Byte Sequence",
    "Delimiter bytes as a hex-encoded string (e.g. '0A' for newline, '0D0A' for CRLF)",
)
.required();

const PROP_KEEP_BYTE_SEQUENCE: PropertyDescriptor = PropertyDescriptor::new(
    "Keep Byte Sequence",
    "Whether to include the delimiter in each output segment",
)
.default_value("false")
.allowed_values(&["true", "false"]);

const PROP_HEADER_LINE_COUNT: PropertyDescriptor = PropertyDescriptor::new(
    "Header Line Count",
    "Number of lines from the beginning of the original content to prepend to each split",
)
.default_value("0");

/// Splits binary FlowFile content by a configurable byte sequence delimiter.
///
/// Each segment between delimiters becomes its own FlowFile, routed to the
/// "splits" relationship. The original FlowFile is routed to "original".
/// If splitting fails, the FlowFile is routed to "failure".
///
/// Split FlowFiles receive fragment attributes for reassembly:
/// - `fragment.identifier`: shared UUID-like ID across all splits from one original
/// - `fragment.index`: zero-based index of this split
/// - `fragment.count`: total number of splits
/// - `segment.original.filename`: filename from the original FlowFile (if present)
pub struct SplitContent;

impl SplitContent {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SplitContent {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode a hex-encoded string into raw bytes.
///
/// Accepts uppercase or lowercase hex characters. The input must have an even
/// number of characters.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("hex string must have even length".to_string());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte_str = &hex[i..i + 2];
        let byte = u8::from_str_radix(byte_str, 16)
            .map_err(|e| format!("invalid hex '{}': {}", byte_str, e))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// Find all occurrences of `needle` in `haystack`, returning their start indices.
fn find_all_occurrences(haystack: &[u8], needle: &[u8]) -> Vec<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return Vec::new();
    }
    let mut positions = Vec::new();
    let mut start = 0;
    while start + needle.len() <= haystack.len() {
        if let Some(pos) = haystack[start..]
            .windows(needle.len())
            .position(|w| w == needle)
        {
            positions.push(start + pos);
            start = start + pos + needle.len();
        } else {
            break;
        }
    }
    positions
}

/// Extract the header from content: the first `header_line_count` lines (delimited by `\n`).
/// Returns the header bytes including the trailing newline(s).
fn extract_header(content: &[u8], header_line_count: usize) -> &[u8] {
    if header_line_count == 0 {
        return &[];
    }
    let mut lines_found = 0;
    for (i, &byte) in content.iter().enumerate() {
        if byte == b'\n' {
            lines_found += 1;
            if lines_found == header_line_count {
                return &content[..=i];
            }
        }
    }
    // If we didn't find enough newlines, the entire content is the header.
    content
}

impl Processor for SplitContent {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let hex_sequence = context.get_property("Byte Sequence");
        let hex_str = match hex_sequence.as_str() {
            Some(s) if !s.is_empty() => s,
            _ => {
                return Err(PluginError::PropertyRequired("Byte Sequence"));
            }
        };

        let delimiter = match decode_hex(hex_str) {
            Ok(d) if !d.is_empty() => d,
            Ok(_) => {
                return Err(PluginError::ProcessingFailed(
                    "decoded byte sequence is empty".to_string(),
                ));
            }
            Err(e) => {
                tracing::error!(hex = hex_str, error = %e, "Failed to decode Byte Sequence");
                // Route all pending FlowFiles to failure.
                while let Some(ff) = session.get() {
                    session.transfer(ff, &REL_FAILURE);
                }
                session.commit();
                return Ok(());
            }
        };

        let keep_delimiter = context
            .get_property("Keep Byte Sequence")
            .unwrap_or("false")
            == "true";

        let header_line_count: usize = context
            .get_property("Header Line Count")
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);

        while let Some(flowfile) = session.get() {
            let content = match session.read_content(&flowfile) {
                Ok(c) => c,
                Err(_) => {
                    tracing::warn!(flowfile_id = flowfile.id, "Failed to read FlowFile content");
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            if content.is_empty() {
                // Empty content: nothing to split, route to failure.
                session.transfer(flowfile, &REL_FAILURE);
                continue;
            }

            let header = extract_header(&content, header_line_count);

            let positions = find_all_occurrences(&content, &delimiter);

            // Build segments from the delimiter positions.
            let mut segments: Vec<Bytes> = Vec::new();
            let mut seg_start = 0;

            for &pos in &positions {
                let seg_end = if keep_delimiter {
                    pos + delimiter.len()
                } else {
                    pos
                };

                let segment_body = &content[seg_start..seg_end];

                // Build the segment: header + segment body (unless the segment IS the header).
                let segment = if !header.is_empty() && seg_start > 0 {
                    // Prepend header to non-first segments.
                    let mut buf = Vec::with_capacity(header.len() + segment_body.len());
                    buf.extend_from_slice(header);
                    buf.extend_from_slice(segment_body);
                    Bytes::from(buf)
                } else {
                    Bytes::copy_from_slice(segment_body)
                };

                if !segment.is_empty() {
                    segments.push(segment);
                }

                seg_start = pos + delimiter.len();
            }

            // Trailing segment after last delimiter.
            if seg_start < content.len() {
                let segment_body = &content[seg_start..];
                let segment = if !header.is_empty() && seg_start > 0 {
                    let mut buf = Vec::with_capacity(header.len() + segment_body.len());
                    buf.extend_from_slice(header);
                    buf.extend_from_slice(segment_body);
                    Bytes::from(buf)
                } else {
                    Bytes::copy_from_slice(segment_body)
                };

                if !segment.is_empty() {
                    segments.push(segment);
                }
            }

            if segments.is_empty() {
                // No segments produced (e.g., content is only delimiters).
                session.transfer(flowfile, &REL_FAILURE);
                continue;
            }

            let fragment_id = Arc::<str>::from(format!("{}", flowfile.id));
            let fragment_count = Arc::<str>::from(format!("{}", segments.len()));
            let original_filename = flowfile.get_attribute("filename").cloned();

            for (index, segment) in segments.into_iter().enumerate() {
                let mut split_ff = session.create();

                // Copy attributes from the original.
                for (key, value) in &flowfile.attributes {
                    split_ff.set_attribute(Arc::clone(key), Arc::clone(value));
                }

                // Set fragment attributes.
                split_ff.set_attribute(Arc::from("fragment.identifier"), Arc::clone(&fragment_id));
                split_ff.set_attribute(
                    Arc::from("fragment.index"),
                    Arc::from(format!("{}", index).as_str()),
                );
                split_ff.set_attribute(Arc::from("fragment.count"), Arc::clone(&fragment_count));

                if let Some(ref orig_name) = original_filename {
                    split_ff.set_attribute(
                        Arc::from("segment.original.filename"),
                        Arc::clone(orig_name),
                    );
                }

                split_ff = session.write_content(split_ff, segment)?;
                session.transfer(split_ff, &REL_SPLITS);
            }

            // Transfer the original FlowFile.
            session.transfer(flowfile, &REL_ORIGINAL);
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_SPLITS, REL_ORIGINAL, REL_FAILURE]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_BYTE_SEQUENCE,
            PROP_KEEP_BYTE_SEQUENCE,
            PROP_HEADER_LINE_COUNT,
        ]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "SplitContent",
        description: "Splits binary FlowFile content by a configurable byte sequence delimiter",
        factory: || Box::new(SplitContent::new()),
        tags: &["Transformation", "Content"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    // -- Test helpers --

    struct TestContext {
        byte_sequence: String,
        keep_byte_sequence: String,
        header_line_count: String,
    }

    impl TestContext {
        fn new(hex: &str) -> Self {
            Self {
                byte_sequence: hex.to_string(),
                keep_byte_sequence: "false".to_string(),
                header_line_count: "0".to_string(),
            }
        }

        fn with_keep(mut self, keep: bool) -> Self {
            self.keep_byte_sequence = keep.to_string();
            self
        }

        fn with_header_lines(mut self, count: usize) -> Self {
            self.header_line_count = count.to_string();
            self
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Byte Sequence" => PropertyValue::String(self.byte_sequence.clone()),
                "Keep Byte Sequence" => PropertyValue::String(self.keep_byte_sequence.clone()),
                "Header Line Count" => PropertyValue::String(self.header_line_count.clone()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-split-content"
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
        contents: Vec<(u64, Bytes)>,
        transferred: Vec<(FlowFile, &'static str)>,
        written: Vec<(u64, Bytes)>,
        next_id: u64,
    }

    impl TestSession {
        fn new() -> Self {
            Self {
                inputs: Vec::new(),
                contents: Vec::new(),
                transferred: Vec::new(),
                written: Vec::new(),
                next_id: 100,
            }
        }

        fn add_flowfile(&mut self, id: u64, content: &[u8]) {
            let ff = FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: content.len() as u64,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            };
            self.inputs.push(ff);
            self.contents.push((id, Bytes::copy_from_slice(content)));
        }

        fn add_flowfile_with_attrs(&mut self, id: u64, content: &[u8], attrs: Vec<(&str, &str)>) {
            let mut ff = FlowFile {
                id,
                attributes: Vec::new(),
                content_claim: None,
                size: content.len() as u64,
                created_at_nanos: 0,
                lineage_start_id: id,
                penalized_until_nanos: 0,
            };
            for (k, v) in attrs {
                ff.set_attribute(Arc::from(k), Arc::from(v));
            }
            self.inputs.push(ff);
            self.contents.push((id, Bytes::copy_from_slice(content)));
        }

        fn get_transfers(&self, rel: &str) -> Vec<&FlowFile> {
            self.transferred
                .iter()
                .filter(|(_, r)| *r == rel)
                .map(|(ff, _)| ff)
                .collect()
        }

        fn get_written_content(&self, ff_id: u64) -> Option<&Bytes> {
            self.written
                .iter()
                .find(|(id, _)| *id == ff_id)
                .map(|(_, b)| b)
        }
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

        fn read_content(&self, ff: &FlowFile) -> ProcessResult<Bytes> {
            self.contents
                .iter()
                .find(|(id, _)| *id == ff.id)
                .map(|(_, b)| b.clone())
                .ok_or(PluginError::ContentNotFound(ff.id))
        }

        fn write_content(&mut self, mut ff: FlowFile, data: Bytes) -> ProcessResult<FlowFile> {
            ff.size = data.len() as u64;
            self.written.push((ff.id, data));
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

        fn clone_flowfile(&mut self, ff: &FlowFile) -> FlowFile {
            let id = self.next_id;
            self.next_id += 1;
            FlowFile {
                id,
                attributes: ff.attributes.clone(),
                content_claim: ff.content_claim.clone(),
                size: ff.size,
                created_at_nanos: ff.created_at_nanos,
                lineage_start_id: ff.lineage_start_id,
                penalized_until_nanos: 0,
            }
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

    // -- Hex decode tests --

    #[test]
    fn decode_hex_valid() {
        assert_eq!(decode_hex("0A").unwrap(), vec![0x0A]);
        assert_eq!(decode_hex("0D0A").unwrap(), vec![0x0D, 0x0A]);
        assert_eq!(decode_hex("FF00AB").unwrap(), vec![0xFF, 0x00, 0xAB]);
        assert_eq!(decode_hex("ff").unwrap(), vec![0xFF]);
    }

    #[test]
    fn decode_hex_odd_length() {
        assert!(decode_hex("0A0").is_err());
    }

    #[test]
    fn decode_hex_invalid_chars() {
        assert!(decode_hex("GG").is_err());
    }

    // -- find_all_occurrences tests --

    #[test]
    fn find_occurrences_basic() {
        let data = b"hello\nworld\nfoo";
        let positions = find_all_occurrences(data, b"\n");
        assert_eq!(positions, vec![5, 11]);
    }

    #[test]
    fn find_occurrences_multi_byte() {
        let data = b"abc--def--ghi";
        let positions = find_all_occurrences(data, b"--");
        assert_eq!(positions, vec![3, 8]);
    }

    #[test]
    fn find_occurrences_none() {
        let data = b"abcdefgh";
        let positions = find_all_occurrences(data, b"--");
        assert_eq!(positions, Vec::<usize>::new());
    }

    #[test]
    fn find_occurrences_at_start_and_end() {
        let data = b"\nhello\n";
        let positions = find_all_occurrences(data, b"\n");
        assert_eq!(positions, vec![0, 6]);
    }

    // -- extract_header tests --

    #[test]
    fn extract_header_zero_lines() {
        let content = b"line1\nline2\nline3";
        assert_eq!(extract_header(content, 0), b"");
    }

    #[test]
    fn extract_header_one_line() {
        let content = b"header\nline2\nline3";
        assert_eq!(extract_header(content, 1), b"header\n");
    }

    #[test]
    fn extract_header_two_lines() {
        let content = b"h1\nh2\ndata";
        assert_eq!(extract_header(content, 2), b"h1\nh2\n");
    }

    #[test]
    fn extract_header_more_than_available() {
        let content = b"only-one-line";
        assert_eq!(extract_header(content, 5), content.as_slice());
    }

    // -- Processor integration tests --

    #[test]
    fn split_by_newline() {
        let mut proc = SplitContent::new();
        // 0A = newline
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"line1\nline2\nline3");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);
        let originals = session.get_transfers("original");
        assert_eq!(originals.len(), 1);
        assert_eq!(originals[0].id, 1);

        // Verify split content.
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"line1");
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"line2");
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), b"line3");
    }

    #[test]
    fn split_with_keep_delimiter() {
        let mut proc = SplitContent::new();
        // 0A = newline
        let ctx = TestContext::new("0A").with_keep(true);
        let mut session = TestSession::new();
        session.add_flowfile(1, b"line1\nline2\nline3");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);

        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"line1\n");
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"line2\n");
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), b"line3");
    }

    #[test]
    fn split_with_multi_byte_delimiter() {
        let mut proc = SplitContent::new();
        // 0D0A = CRLF
        let ctx = TestContext::new("0D0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"part1\r\npart2\r\npart3");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);

        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"part1");
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"part2");
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), b"part3");
    }

    #[test]
    fn split_with_header_lines() {
        let mut proc = SplitContent::new();
        // 0A = newline
        let ctx = TestContext::new("0A").with_header_lines(1);
        let mut session = TestSession::new();
        session.add_flowfile(1, b"HEADER\ndata1\ndata2\ndata3");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 4);

        // First split is "HEADER" (no header prepended to the first segment).
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"HEADER");
        // Subsequent splits get the header prepended.
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"HEADER\ndata1");
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), b"HEADER\ndata2");
        let s3 = session.get_written_content(splits[3].id).unwrap();
        assert_eq!(s3.as_ref(), b"HEADER\ndata3");
    }

    #[test]
    fn split_fragment_attributes() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile_with_attrs(
            1,
            b"a\nb\nc",
            vec![("filename", "test.txt"), ("custom", "value")],
        );

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);

        for (i, split) in splits.iter().enumerate() {
            // Fragment attributes.
            assert_eq!(
                split.get_attribute("fragment.identifier").unwrap().as_ref(),
                "1"
            );
            assert_eq!(
                split.get_attribute("fragment.index").unwrap().as_ref(),
                &i.to_string()
            );
            assert_eq!(split.get_attribute("fragment.count").unwrap().as_ref(), "3");
            assert_eq!(
                split
                    .get_attribute("segment.original.filename")
                    .unwrap()
                    .as_ref(),
                "test.txt"
            );
            // Original attributes are copied.
            assert_eq!(split.get_attribute("custom").unwrap().as_ref(), "value");
        }
    }

    #[test]
    fn empty_content_routes_to_failure() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let failures = session.get_transfers("failure");
        assert_eq!(failures.len(), 1);
        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 0);
    }

    #[test]
    fn no_delimiter_found_produces_single_split() {
        let mut proc = SplitContent::new();
        // 0A = newline, but content has none.
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"no newlines here");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 1);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"no newlines here");

        let originals = session.get_transfers("original");
        assert_eq!(originals.len(), 1);
    }

    #[test]
    fn delimiter_at_start() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"\ndata");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        // Leading delimiter produces empty first segment (skipped) + "data".
        assert_eq!(splits.len(), 1);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"data");
    }

    #[test]
    fn delimiter_at_end() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"data\n");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        // Trailing delimiter: "data" segment, no trailing empty segment.
        assert_eq!(splits.len(), 1);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"data");
    }

    #[test]
    fn consecutive_delimiters_skip_empty_segments() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"a\n\n\nb");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        // "a", "", "", "b" — empty segments are skipped.
        assert_eq!(splits.len(), 2);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"a");
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"b");
    }

    #[test]
    fn only_delimiters_routes_to_failure() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"\n\n\n");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let failures = session.get_transfers("failure");
        assert_eq!(failures.len(), 1);
        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 0);
    }

    #[test]
    fn invalid_hex_routes_to_failure() {
        let mut proc = SplitContent::new();
        let ctx = TestContext {
            byte_sequence: "ZZZZ".to_string(),
            keep_byte_sequence: "false".to_string(),
            header_line_count: "0".to_string(),
        };
        let mut session = TestSession::new();
        session.add_flowfile(1, b"some content");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let failures = session.get_transfers("failure");
        assert_eq!(failures.len(), 1);
    }

    #[test]
    fn multiple_flowfiles_processed() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("0A");
        let mut session = TestSession::new();
        session.add_flowfile(1, b"a\nb");
        session.add_flowfile(2, b"x\ny\nz");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 5); // 2 from first + 3 from second
        let originals = session.get_transfers("original");
        assert_eq!(originals.len(), 2);
    }

    #[test]
    fn binary_delimiter() {
        let mut proc = SplitContent::new();
        // FF00 as delimiter.
        let ctx = TestContext::new("FF00");
        let mut session = TestSession::new();
        let content: Vec<u8> = vec![0x01, 0x02, 0xFF, 0x00, 0x03, 0x04, 0xFF, 0x00, 0x05];
        session.add_flowfile(1, &content);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), &[0x01, 0x02]);
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), &[0x03, 0x04]);
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), &[0x05]);
    }

    #[test]
    fn keep_delimiter_with_binary() {
        let mut proc = SplitContent::new();
        let ctx = TestContext::new("FF00").with_keep(true);
        let mut session = TestSession::new();
        let content: Vec<u8> = vec![0x01, 0x02, 0xFF, 0x00, 0x03, 0x04];
        session.add_flowfile(1, &content);

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 2);
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), &[0x01, 0x02, 0xFF, 0x00]);
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), &[0x03, 0x04]);
    }

    #[test]
    fn header_with_two_lines() {
        let mut proc = SplitContent::new();
        // Split by "---" (2D2D2D).
        let ctx = TestContext::new("2D2D2D").with_header_lines(2);
        let mut session = TestSession::new();
        session.add_flowfile(1, b"h1\nh2\ndata1---data2---data3");

        proc.on_trigger(&ctx, &mut session).unwrap();

        let splits = session.get_transfers("splits");
        assert_eq!(splits.len(), 3);

        // First segment starts at 0, no header prepended.
        let s0 = session.get_written_content(splits[0].id).unwrap();
        assert_eq!(s0.as_ref(), b"h1\nh2\ndata1");
        // Second segment gets header prepended.
        let s1 = session.get_written_content(splits[1].id).unwrap();
        assert_eq!(s1.as_ref(), b"h1\nh2\ndata2");
        let s2 = session.get_written_content(splits[2].id).unwrap();
        assert_eq!(s2.as_ref(), b"h1\nh2\ndata3");
    }

    #[test]
    fn relationships_are_correct() {
        let proc = SplitContent::new();
        let rels = proc.relationships();
        assert_eq!(rels.len(), 3);
        assert!(rels.iter().any(|r| r.name == "splits"));
        assert!(rels.iter().any(|r| r.name == "original"));
        assert!(rels.iter().any(|r| r.name == "failure"));
    }

    #[test]
    fn property_descriptors_are_correct() {
        let proc = SplitContent::new();
        let props = proc.property_descriptors();
        assert_eq!(props.len(), 3);
        assert!(
            props
                .iter()
                .any(|p| p.name == "Byte Sequence" && p.required)
        );
        assert!(props.iter().any(|p| p.name == "Keep Byte Sequence"));
        assert!(props.iter().any(|p| p.name == "Header Line Count"));
    }
}
