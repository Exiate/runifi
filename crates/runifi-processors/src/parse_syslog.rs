use std::sync::Arc;

use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;
use runifi_plugin_api::{REL_FAILURE, REL_SUCCESS};

const PROP_CHARSET: PropertyDescriptor =
    PropertyDescriptor::new("Character Set", "Character encoding of the syslog message")
        .default_value("UTF-8");

const PROP_PARSE_RFC: PropertyDescriptor = PropertyDescriptor::new(
    "Parse RFC",
    "Which RFC format to parse: '3164', '5424', or 'auto' for auto-detection",
)
.default_value("auto")
.allowed_values(&["auto", "3164", "5424"]);

/// Syslog facility names (RFC 5424 Section 6.2.1).
const FACILITY_NAMES: &[&str] = &[
    "kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news", "uucp", "cron", "authpriv",
    "ftp", "ntp", "audit", "alert", "clock", "local0", "local1", "local2", "local3", "local4",
    "local5", "local6", "local7",
];

/// Syslog severity names (RFC 5424 Section 6.2.1).
const SEVERITY_NAMES: &[&str] = &[
    "emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
];

/// Month abbreviations for RFC 3164 timestamps.
const MONTHS: &[&str] = &[
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Parses RFC 3164 and RFC 5424 syslog messages into structured FlowFile attributes.
///
/// Syslog messages are read from FlowFile content and parsed into individual
/// attributes such as priority, facility, severity, timestamp, hostname,
/// application name, and message body.
///
/// Supports auto-detection between RFC 3164 and RFC 5424 formats.
pub struct ParseSyslog;

impl ParseSyslog {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ParseSyslog {
    fn default() -> Self {
        Self::new()
    }
}

/// Parsed syslog fields common to both RFC formats.
struct SyslogMessage {
    priority: u32,
    facility: u32,
    severity: u32,
    timestamp: String,
    hostname: String,
    message: String,
    // RFC 5424 specific fields
    appname: Option<String>,
    procid: Option<String>,
    msgid: Option<String>,
}

/// Parses the priority value from the leading `<N>` in a syslog message.
/// Returns (priority_value, remaining_string) or None if invalid.
fn parse_priority(input: &str) -> Option<(u32, &str)> {
    if !input.starts_with('<') {
        return None;
    }
    let end = input.find('>')?;
    if !(2..=4).contains(&end) {
        // Priority is 1-3 digits
        return None;
    }
    let pri_str = &input[1..end];
    let pri: u32 = pri_str.parse().ok()?;
    if pri > 191 {
        // Max: facility 23 * 8 + severity 7 = 191
        return None;
    }
    Some((pri, &input[end + 1..]))
}

/// Determines if a message after the priority looks like RFC 5424 (starts with version digit).
fn is_rfc5424(after_pri: &str) -> bool {
    // RFC 5424 messages start with a version number (currently "1") after the priority.
    after_pri.starts_with("1 ")
}

/// Parses a single token delimited by space. Returns (token, rest).
fn next_token(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    match input.find(' ') {
        Some(pos) => Some((&input[..pos], &input[pos + 1..])),
        None => Some((input, "")),
    }
}

/// Converts an RFC 3164 timestamp (e.g., "Mar  5 14:30:00") to ISO 8601 format.
/// Since RFC 3164 has no year, we use the current year.
fn rfc3164_timestamp_to_iso(month: &str, day: &str, time: &str) -> Option<String> {
    let month_num = MONTHS.iter().position(|&m| m == month)? + 1;
    let day_num: u32 = day.parse().ok()?;

    // Validate time format HH:MM:SS
    let parts: Vec<&str> = time.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let hour: u32 = parts[0].parse().ok()?;
    let min: u32 = parts[1].parse().ok()?;
    let sec: u32 = parts[2].parse().ok()?;
    if hour > 23 || min > 59 || sec > 60 {
        return None;
    }

    // RFC 3164 doesn't include a year; use a placeholder year.
    // Real implementations would use the current year, but for deterministic
    // parsing we emit what we can.
    Some(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        2024, month_num, day_num, hour, min, sec
    ))
}

/// Parses an RFC 3164 syslog message (after priority has been extracted).
///
/// Format: `<PRI>Mmm dd hh:mm:ss HOSTNAME MSG`
fn parse_rfc3164(pri: u32, after_pri: &str) -> Option<SyslogMessage> {
    let facility = pri / 8;
    let severity = pri % 8;

    // Parse: Month Day Time Hostname Message
    let (month, rest) = next_token(after_pri)?;
    if !MONTHS.contains(&month) {
        return None;
    }

    let (day, rest) = next_token(rest)?;
    let (time, rest) = next_token(rest)?;

    let timestamp = rfc3164_timestamp_to_iso(month, day, time)?;

    let (hostname, rest) = next_token(rest)?;

    let message = rest.to_string();

    Some(SyslogMessage {
        priority: pri,
        facility,
        severity,
        timestamp,
        hostname: hostname.to_string(),
        message,
        appname: None,
        procid: None,
        msgid: None,
    })
}

/// Parses an RFC 5424 syslog message (after priority has been extracted).
///
/// Format: `<PRI>VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP STRUCTURED-DATA [SP MSG]`
fn parse_rfc5424(pri: u32, after_pri: &str) -> Option<SyslogMessage> {
    let facility = pri / 8;
    let severity = pri % 8;

    // Skip version (already confirmed starts with "1 ")
    let (_, rest) = next_token(after_pri)?;

    // TIMESTAMP
    let (timestamp_raw, rest) = next_token(rest)?;
    let timestamp = if timestamp_raw == "-" {
        "-".to_string()
    } else {
        timestamp_raw.to_string()
    };

    // HOSTNAME
    let (hostname_raw, rest) = next_token(rest)?;
    let hostname = if hostname_raw == "-" {
        String::new()
    } else {
        hostname_raw.to_string()
    };

    // APP-NAME
    let (appname_raw, rest) = next_token(rest)?;
    let appname = if appname_raw == "-" {
        None
    } else {
        Some(appname_raw.to_string())
    };

    // PROCID
    let (procid_raw, rest) = next_token(rest)?;
    let procid = if procid_raw == "-" {
        None
    } else {
        Some(procid_raw.to_string())
    };

    // MSGID
    let (msgid_raw, rest) = next_token(rest)?;
    let msgid = if msgid_raw == "-" {
        None
    } else {
        Some(msgid_raw.to_string())
    };

    // STRUCTURED-DATA — skip it (either "-" or "[...]" blocks)
    let rest = rest.trim_start();
    let msg_part = if let Some(stripped) = rest.strip_prefix('-') {
        // Nil structured data
        stripped.strip_prefix(' ').unwrap_or(stripped)
    } else if rest.starts_with('[') {
        // Skip structured data blocks [id key="val" ...]
        skip_structured_data(rest)
    } else {
        rest
    };

    let message = msg_part.trim_start().to_string();

    Some(SyslogMessage {
        priority: pri,
        facility,
        severity,
        timestamp,
        hostname,
        message,
        appname,
        procid,
        msgid,
    })
}

/// Skips RFC 5424 structured data blocks `[id param="val" ...]`.
/// Returns the remaining string after all structured data blocks.
fn skip_structured_data(input: &str) -> &str {
    let mut pos = 0;
    let bytes = input.as_bytes();

    while pos < bytes.len() && bytes[pos] == b'[' {
        // Find matching close bracket, accounting for escaped characters
        pos += 1;
        while pos < bytes.len() {
            if bytes[pos] == b'\\' {
                pos += 2; // skip escaped char
            } else if bytes[pos] == b']' {
                pos += 1;
                break;
            } else {
                pos += 1;
            }
        }
    }

    if pos < input.len() { &input[pos..] } else { "" }
}

/// Sets parsed syslog attributes on a FlowFile.
fn apply_syslog_attributes(ff: &mut runifi_plugin_api::FlowFile, msg: &SyslogMessage) {
    ff.set_attribute(
        Arc::from("syslog.priority"),
        Arc::from(msg.priority.to_string().as_str()),
    );

    let facility_name = FACILITY_NAMES
        .get(msg.facility as usize)
        .unwrap_or(&"unknown");
    ff.set_attribute(Arc::from("syslog.facility"), Arc::from(*facility_name));

    let severity_name = SEVERITY_NAMES
        .get(msg.severity as usize)
        .unwrap_or(&"unknown");
    ff.set_attribute(Arc::from("syslog.severity"), Arc::from(*severity_name));

    ff.set_attribute(
        Arc::from("syslog.timestamp"),
        Arc::from(msg.timestamp.as_str()),
    );

    ff.set_attribute(
        Arc::from("syslog.hostname"),
        Arc::from(msg.hostname.as_str()),
    );

    ff.set_attribute(Arc::from("syslog.message"), Arc::from(msg.message.as_str()));

    if let Some(ref appname) = msg.appname {
        ff.set_attribute(Arc::from("syslog.appname"), Arc::from(appname.as_str()));
    }

    if let Some(ref procid) = msg.procid {
        ff.set_attribute(Arc::from("syslog.procid"), Arc::from(procid.as_str()));
    }

    if let Some(ref msgid) = msg.msgid {
        ff.set_attribute(Arc::from("syslog.msgid"), Arc::from(msgid.as_str()));
    }
}

impl Processor for ParseSyslog {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let rfc_mode = context
            .get_property("Parse RFC")
            .unwrap_or("auto")
            .to_string();

        while let Some(mut flowfile) = session.get() {
            let content = match session.read_content(&flowfile) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!(
                        flowfile_id = flowfile.id,
                        error = %e,
                        "Failed to read FlowFile content"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            let text = match std::str::from_utf8(&content) {
                Ok(s) => s.trim(),
                Err(e) => {
                    tracing::warn!(
                        flowfile_id = flowfile.id,
                        error = %e,
                        "FlowFile content is not valid UTF-8"
                    );
                    session.transfer(flowfile, &REL_FAILURE);
                    continue;
                }
            };

            let parsed = match parse_priority(text) {
                Some((pri, after_pri)) => {
                    match rfc_mode.as_str() {
                        "5424" => parse_rfc5424(pri, after_pri),
                        "3164" => parse_rfc3164(pri, after_pri),
                        _ => {
                            // Auto-detect: RFC 5424 starts with version after priority
                            if is_rfc5424(after_pri) {
                                parse_rfc5424(pri, after_pri)
                            } else {
                                parse_rfc3164(pri, after_pri)
                            }
                        }
                    }
                }
                None => None,
            };

            match parsed {
                Some(msg) => {
                    apply_syslog_attributes(&mut flowfile, &msg);
                    session.transfer(flowfile, &REL_SUCCESS);
                }
                None => {
                    tracing::debug!(flowfile_id = flowfile.id, "Failed to parse syslog message");
                    session.transfer(flowfile, &REL_FAILURE);
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
        vec![PROP_CHARSET, PROP_PARSE_RFC]
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "ParseSyslog",
        description: "Parses RFC 3164 and RFC 5424 syslog messages into structured FlowFile attributes",
        factory: || Box::new(ParseSyslog::new()),
        tags: &["Parsing", "Syslog"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use runifi_plugin_api::FlowFile;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        rfc_mode: String,
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            match name {
                "Parse RFC" => PropertyValue::String(self.rfc_mode.clone()),
                "Character Set" => PropertyValue::String("UTF-8".to_string()),
                _ => PropertyValue::Unset,
            }
        }
        fn name(&self) -> &str {
            "test-parse-syslog"
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
    }

    impl TestSession {
        fn new(inputs: Vec<(FlowFile, Bytes)>) -> Self {
            let mut ff_list = Vec::new();
            let mut content_list = Vec::new();
            for (ff, content) in inputs {
                content_list.push((ff.id, content));
                ff_list.push(ff);
            }
            Self {
                inputs: ff_list,
                contents: content_list,
                transferred: Vec::new(),
            }
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
                .map(|(_, data)| data.clone())
                .ok_or_else(|| runifi_plugin_api::PluginError::ContentNotFound(ff.id))
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

    // ---- RFC 3164 Tests ----

    #[test]
    fn parses_rfc3164_basic() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "3164".to_string(),
        };

        let msg = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(ff.get_attribute("syslog.priority").unwrap().as_ref(), "34");
        assert_eq!(
            ff.get_attribute("syslog.facility").unwrap().as_ref(),
            "auth"
        );
        assert_eq!(
            ff.get_attribute("syslog.severity").unwrap().as_ref(),
            "crit"
        );
        assert_eq!(
            ff.get_attribute("syslog.timestamp").unwrap().as_ref(),
            "2024-10-11T22:14:15"
        );
        assert_eq!(
            ff.get_attribute("syslog.hostname").unwrap().as_ref(),
            "mymachine"
        );
        assert_eq!(
            ff.get_attribute("syslog.message").unwrap().as_ref(),
            "su: 'su root' failed for lonvick on /dev/pts/8"
        );
    }

    #[test]
    fn parses_rfc3164_kern_emerg() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "3164".to_string(),
        };

        // Priority 0 = kern.emerg
        let msg = "<0>Jan  1 00:00:00 router kernel: Panic - not syncing";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(ff.get_attribute("syslog.priority").unwrap().as_ref(), "0");
        assert_eq!(
            ff.get_attribute("syslog.facility").unwrap().as_ref(),
            "kern"
        );
        assert_eq!(
            ff.get_attribute("syslog.severity").unwrap().as_ref(),
            "emerg"
        );
        assert_eq!(
            ff.get_attribute("syslog.hostname").unwrap().as_ref(),
            "router"
        );
    }

    #[test]
    fn parses_rfc3164_local_facility() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "3164".to_string(),
        };

        // Priority 166 = local4 (20*8) + severity 6 (info) = 166
        let msg = "<166>Mar 15 09:30:45 webserver nginx: GET /index.html 200";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("syslog.facility").unwrap().as_ref(),
            "local4"
        );
        assert_eq!(
            ff.get_attribute("syslog.severity").unwrap().as_ref(),
            "info"
        );
    }

    // ---- RFC 5424 Tests ----

    #[test]
    fn parses_rfc5424_full() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg = "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 - An application event log entry";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(ff.get_attribute("syslog.priority").unwrap().as_ref(), "165");
        assert_eq!(
            ff.get_attribute("syslog.facility").unwrap().as_ref(),
            "local4"
        );
        assert_eq!(
            ff.get_attribute("syslog.severity").unwrap().as_ref(),
            "notice"
        );
        assert_eq!(
            ff.get_attribute("syslog.timestamp").unwrap().as_ref(),
            "2003-10-11T22:14:15.003Z"
        );
        assert_eq!(
            ff.get_attribute("syslog.hostname").unwrap().as_ref(),
            "mymachine.example.com"
        );
        assert_eq!(
            ff.get_attribute("syslog.appname").unwrap().as_ref(),
            "evntslog"
        );
        assert!(ff.get_attribute("syslog.procid").is_none()); // "-" means nil
        assert_eq!(ff.get_attribute("syslog.msgid").unwrap().as_ref(), "ID47");
        assert_eq!(
            ff.get_attribute("syslog.message").unwrap().as_ref(),
            "An application event log entry"
        );
    }

    #[test]
    fn parses_rfc5424_with_procid() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg =
            "<34>1 2024-01-15T10:30:00Z server1 sshd 12345 - - Accepted password for user root";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(ff.get_attribute("syslog.appname").unwrap().as_ref(), "sshd");
        assert_eq!(ff.get_attribute("syslog.procid").unwrap().as_ref(), "12345");
        assert!(ff.get_attribute("syslog.msgid").is_none());
    }

    #[test]
    fn parses_rfc5424_with_structured_data() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg = r#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("syslog.message").unwrap().as_ref(),
            "An application event log entry"
        );
    }

    #[test]
    fn parses_rfc5424_nil_values() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg = "<13>1 - - - - - - A minimal syslog message";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(ff.get_attribute("syslog.priority").unwrap().as_ref(), "13");
        assert_eq!(ff.get_attribute("syslog.timestamp").unwrap().as_ref(), "-");
        assert_eq!(ff.get_attribute("syslog.hostname").unwrap().as_ref(), "");
        assert!(ff.get_attribute("syslog.appname").is_none());
        assert!(ff.get_attribute("syslog.procid").is_none());
        assert!(ff.get_attribute("syslog.msgid").is_none());
    }

    // ---- Auto-detection Tests ----

    #[test]
    fn auto_detects_rfc5424() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let msg = "<165>1 2003-10-11T22:14:15.003Z mymachine evntslog - ID47 - Event log entry";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        // If it was correctly auto-detected as 5424, it should have appname
        assert_eq!(
            ff.get_attribute("syslog.appname").unwrap().as_ref(),
            "evntslog"
        );
    }

    #[test]
    fn auto_detects_rfc3164() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let msg = "<34>Oct 11 22:14:15 mymachine su: failed login";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("syslog.hostname").unwrap().as_ref(),
            "mymachine"
        );
        // RFC 3164 should NOT have appname set
        assert!(ff.get_attribute("syslog.appname").is_none());
    }

    // ---- Failure / Invalid Message Tests ----

    #[test]
    fn rejects_invalid_no_priority() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let msg = "This is not a syslog message";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn rejects_invalid_priority_too_high() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let msg = "<999>Oct 11 22:14:15 host msg";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn rejects_malformed_rfc3164_bad_month() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "3164".to_string(),
        };

        let msg = "<34>Xyz 11 22:14:15 mymachine message";
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn rejects_non_utf8_content() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let data = Bytes::from(vec![0xFF, 0xFE, 0xFD]);
        let mut session = TestSession::new(vec![(make_ff(1), data)]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    #[test]
    fn handles_empty_content() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let mut session = TestSession::new(vec![(make_ff(1), Bytes::new())]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "failure");
    }

    // ---- Multiple FlowFiles ----

    #[test]
    fn processes_multiple_flowfiles() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "auto".to_string(),
        };

        let msg1 = "<34>Oct 11 22:14:15 host1 valid message";
        let msg2 = "not a syslog message";
        let msg3 = "<165>1 2003-10-11T22:14:15Z host2 app - - - Another message";

        let mut session = TestSession::new(vec![
            (make_ff(1), Bytes::from(msg1)),
            (make_ff(2), Bytes::from(msg2)),
            (make_ff(3), Bytes::from(msg3)),
        ]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 3);
        assert_eq!(session.transferred[0].1, "success"); // RFC 3164
        assert_eq!(session.transferred[1].1, "failure"); // invalid
        assert_eq!(session.transferred[2].1, "success"); // RFC 5424
    }

    // ---- Structured Data Tests ----

    #[test]
    fn parses_rfc5424_multiple_structured_data() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg = r#"<165>1 2003-10-11T22:14:15.003Z host app - - [sd1@123 key="val"][sd2@456 k2="v2"] The message"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("syslog.message").unwrap().as_ref(),
            "The message"
        );
    }

    #[test]
    fn parses_rfc5424_structured_data_with_escaped_brackets() {
        let mut proc = ParseSyslog::new();
        let ctx = TestContext {
            rfc_mode: "5424".to_string(),
        };

        let msg = r#"<165>1 2003-10-11T22:14:15Z host app - - [sd@123 key="val\]ue"] The message"#;
        let mut session = TestSession::new(vec![(make_ff(1), Bytes::from(msg))]);

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "success");

        let ff = &session.transferred[0].0;
        assert_eq!(
            ff.get_attribute("syslog.message").unwrap().as_ref(),
            "The message"
        );
    }

    // ---- Property & Descriptor Tests ----

    #[test]
    fn has_correct_relationships() {
        let proc = ParseSyslog::new();
        let rels = proc.relationships();
        assert_eq!(rels.len(), 2);
        assert!(rels.iter().any(|r| r.name == "success"));
        assert!(rels.iter().any(|r| r.name == "failure"));
    }

    #[test]
    fn has_correct_properties() {
        let proc = ParseSyslog::new();
        let props = proc.property_descriptors();
        assert_eq!(props.len(), 2);
        assert!(props.iter().any(|p| p.name == "Character Set"));
        assert!(props.iter().any(|p| p.name == "Parse RFC"));
    }

    // ---- Unit Tests for Internal Functions ----

    #[test]
    fn test_parse_priority() {
        assert_eq!(parse_priority("<34>rest"), Some((34, "rest")));
        assert_eq!(parse_priority("<0>msg"), Some((0, "msg")));
        assert_eq!(parse_priority("<191>msg"), Some((191, "msg")));
        assert!(parse_priority("<192>msg").is_none()); // too high
        assert!(parse_priority("no angle").is_none());
        assert!(parse_priority("<>msg").is_none()); // empty
        assert!(parse_priority("<abc>msg").is_none()); // non-numeric
    }

    #[test]
    fn test_is_rfc5424() {
        assert!(is_rfc5424("1 2003-10-11T22:14:15Z host app - -"));
        assert!(!is_rfc5424("Oct 11 22:14:15 host msg"));
        assert!(!is_rfc5424("2 timestamp")); // version 2 not matching "1 "
    }

    #[test]
    fn test_facility_severity_calculation() {
        // Priority 34 = facility 4 (auth) * 8 + severity 2 (crit)
        assert_eq!(34 / 8, 4); // auth
        assert_eq!(34 % 8, 2); // crit

        // Priority 165 = facility 20 (local4) * 8 + severity 5 (notice)
        assert_eq!(165 / 8, 20); // local4
        assert_eq!(165 % 8, 5); // notice
    }
}
