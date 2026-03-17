use std::collections::HashSet;

use regex_lite::Regex;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::processor::{Processor, ProcessorDescriptor};
use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::relationship::Relationship;
use runifi_plugin_api::result::ProcessResult;
use runifi_plugin_api::session::ProcessSession;

const REL_UNMATCHED: Relationship =
    Relationship::new("unmatched", "FlowFiles not matching any routing rule");

const PROP_MATCHING_STRATEGY: PropertyDescriptor = PropertyDescriptor::new(
    "Matching Strategy",
    "How to match the attribute value: 'exact', 'contains', 'regex', 'starts_with', 'ends_with'",
)
.default_value("exact");

const PROP_ROUTING_STRATEGY: PropertyDescriptor = PropertyDescriptor::new(
    "Routing Strategy",
    "Route to all matching relationships ('route to all') or only the first match ('route to first')",
)
.default_value("route to all");

/// Static property names owned by this processor (not routing rules).
const OWN_PROPERTY_NAMES: &[&str] = &["Matching Strategy", "Routing Strategy"];

/// Routes FlowFiles based on dynamic property routing rules.
///
/// Each dynamic property defines a routing rule:
///   - Property name = relationship name
///   - Property value = condition in format `attribute_name=expected_value`
///
/// FlowFiles are evaluated against all rules. The matching strategy applies
/// to the value comparison. FlowFiles matching no rules go to "unmatched".
pub struct RouteOnAttribute;

impl RouteOnAttribute {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RouteOnAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for RouteOnAttribute {
    fn on_trigger(
        &mut self,
        context: &dyn ProcessContext,
        session: &mut dyn ProcessSession,
    ) -> ProcessResult {
        let strategy = context
            .get_property("Matching Strategy")
            .unwrap_or("exact")
            .to_string();
        let routing_strategy = context
            .get_property("Routing Strategy")
            .unwrap_or("route to all")
            .to_string();
        let route_to_first = routing_strategy == "route to first";

        // Build routing rules from dynamic properties.
        let own_names: HashSet<&str> = OWN_PROPERTY_NAMES.iter().copied().collect();
        let all_names = context.property_names();
        let rules: Vec<(String, String, String)> = all_names
            .into_iter()
            .filter(|n| !own_names.contains(n.as_str()))
            .filter_map(|n| {
                context.get_property(&n).as_str().map(|v| {
                    // Parse "attribute_name=expected_value" format.
                    if let Some((attr, val)) = v.split_once('=') {
                        (n, attr.to_string(), val.to_string())
                    } else {
                        // If no '=', check for attribute existence.
                        (n, v.to_string(), String::new())
                    }
                })
            })
            .collect();

        // Pre-compile regexes if strategy is "regex".
        let compiled_regexes: Vec<Option<Regex>> = if strategy == "regex" {
            rules
                .iter()
                .map(|(_, _, pattern)| {
                    if pattern.is_empty() {
                        None
                    } else {
                        match Regex::new(pattern) {
                            Ok(re) => Some(re),
                            Err(e) => {
                                tracing::error!(pattern = %pattern, error = %e, "Invalid regex pattern");
                                None
                            }
                        }
                    }
                })
                .collect()
        } else {
            vec![None; rules.len()]
        };

        while let Some(flowfile) = session.get() {
            // Collect all matching relationship names first.
            let mut matched_rels: Vec<&str> = Vec::new();
            for (idx, (rel_name, attr_name, pattern)) in rules.iter().enumerate() {
                let matches = if pattern.is_empty() {
                    flowfile.get_attribute(attr_name).is_some()
                } else {
                    flowfile
                        .get_attribute(attr_name)
                        .is_some_and(|v| match strategy.as_str() {
                            "contains" => v.contains(pattern.as_str()),
                            "starts_with" => v.starts_with(pattern.as_str()),
                            "ends_with" => v.ends_with(pattern.as_str()),
                            "regex" => compiled_regexes[idx]
                                .as_ref()
                                .is_some_and(|re| re.is_match(v.as_ref())),
                            _ => v.as_ref() == pattern.as_str(),
                        })
                };
                if matches {
                    matched_rels.push(rel_name);
                    if route_to_first {
                        break;
                    }
                }
            }

            if matched_rels.is_empty() {
                session.transfer(flowfile, &REL_UNMATCHED);
            } else if matched_rels.len() == 1 {
                let rel = Relationship {
                    name: Box::leak(matched_rels[0].to_string().into_boxed_str()),
                    description: "",
                    auto_terminated: false,
                };
                session.transfer(flowfile, &rel);
            } else {
                // Multiple matches: clone for all but the last, transfer original for the last.
                let last_idx = matched_rels.len() - 1;
                for (i, rel_name) in matched_rels.iter().enumerate() {
                    let rel = Relationship {
                        name: Box::leak(rel_name.to_string().into_boxed_str()),
                        description: "",
                        auto_terminated: false,
                    };
                    if i == last_idx {
                        session.transfer(flowfile, &rel);
                        break;
                    } else {
                        let clone = session.clone_flowfile(&flowfile);
                        session.transfer(clone, &rel);
                    }
                }
            }
        }

        session.commit();
        Ok(())
    }

    fn relationships(&self) -> Vec<Relationship> {
        vec![REL_UNMATCHED]
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![PROP_MATCHING_STRATEGY, PROP_ROUTING_STRATEGY]
    }

    fn supports_dynamic_properties(&self) -> bool {
        true
    }

    fn dynamic_property_creates_relationship(&self) -> bool {
        true
    }
}

inventory::submit! {
    ProcessorDescriptor {
        type_name: "RouteOnAttribute",
        description: "Routes FlowFiles based on dynamic property routing rules",
        factory: || Box::new(RouteOnAttribute::new()),
        tags: &["Routing"],
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
            "test-route"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    struct MultiFlowFileSession {
        inputs: Vec<FlowFile>,
        transferred: Vec<(FlowFile, &'static str)>,
    }

    impl ProcessSession for MultiFlowFileSession {
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
            Ok(Bytes::new())
        }
        fn write_content(&mut self, ff: FlowFile, _data: Bytes) -> ProcessResult<FlowFile> {
            Ok(ff)
        }
        fn create(&mut self) -> FlowFile {
            unimplemented!()
        }
        fn clone_flowfile(&mut self, ff: &FlowFile) -> FlowFile {
            FlowFile {
                id: ff.id + 1000,
                attributes: ff.attributes.clone(),
                content_claim: ff.content_claim.clone(),
                size: ff.size,
                created_at_nanos: ff.created_at_nanos,
                lineage_start_id: ff.lineage_start_id,
                penalized_until_nanos: ff.penalized_until_nanos,
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

    fn make_ff(id: u64, attr_name: &str, attr_value: &str) -> FlowFile {
        let mut ff = FlowFile {
            id,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: id,
            penalized_until_nanos: 0,
        };
        ff.set_attribute(Arc::from(attr_name), Arc::from(attr_value));
        ff
    }

    #[test]
    fn routes_exact_match_dynamic() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "exact".to_string()),
                // Dynamic property: route "sensor-data" when type=sensor
                ("sensor-data".to_string(), "type=sensor".to_string()),
                // Dynamic property: route "log-data" when type=log
                ("log-data".to_string(), "type=log".to_string()),
            ],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "type", "sensor"),
                make_ff(2, "type", "log"),
                make_ff(3, "type", "unknown"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 3);
        assert_eq!(session.transferred[0].1, "sensor-data");
        assert_eq!(session.transferred[1].1, "log-data");
        assert_eq!(session.transferred[2].1, "unmatched");
    }

    #[test]
    fn routes_contains() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "contains".to_string()),
                ("csv-files".to_string(), "filename=.csv".to_string()),
            ],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data.csv"),
                make_ff(2, "filename", "data.json"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "csv-files");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_regex() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "regex".to_string()),
                (
                    "data-files".to_string(),
                    r"filename=^data-\d+\.csv$".to_string(),
                ),
            ],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data-2024.csv"),
                make_ff(2, "filename", "log-2024.csv"),
                make_ff(3, "filename", "data-abc.csv"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "data-files");
        assert_eq!(session.transferred[1].1, "unmatched");
        assert_eq!(session.transferred[2].1, "unmatched");
    }

    #[test]
    fn routes_starts_with() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "starts_with".to_string()),
                ("data-prefix".to_string(), "filename=data-".to_string()),
            ],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data-2024.csv"),
                make_ff(2, "filename", "log-2024.csv"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "data-prefix");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_ends_with() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "ends_with".to_string()),
                ("csv-files".to_string(), "filename=.csv".to_string()),
            ],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![
                make_ff(1, "filename", "data.csv"),
                make_ff(2, "filename", "data.json"),
            ],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "csv-files");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn routes_attribute_existence() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![
                ("Matching Strategy".to_string(), "exact".to_string()),
                // No '=' in value — checks for attribute existence.
                ("has-priority".to_string(), "priority".to_string()),
            ],
        };

        let ff_with = make_ff(1, "priority", "high");
        let ff_without = FlowFile {
            id: 2,
            attributes: Vec::new(),
            content_claim: None,
            size: 0,
            created_at_nanos: 0,
            lineage_start_id: 2,
            penalized_until_nanos: 0,
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![ff_with, ff_without],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred[0].1, "has-priority");
        assert_eq!(session.transferred[1].1, "unmatched");
    }

    #[test]
    fn no_rules_all_unmatched() {
        let mut proc = RouteOnAttribute::new();
        let ctx = TestContext {
            properties: vec![("Matching Strategy".to_string(), "exact".to_string())],
        };

        let mut session = MultiFlowFileSession {
            inputs: vec![make_ff(1, "type", "sensor")],
            transferred: Vec::new(),
        };

        proc.on_trigger(&ctx, &mut session).unwrap();

        assert_eq!(session.transferred.len(), 1);
        assert_eq!(session.transferred[0].1, "unmatched");
    }

    #[test]
    fn supports_dynamic_properties_flag() {
        let proc = RouteOnAttribute::new();
        assert!(proc.supports_dynamic_properties());
        assert!(proc.dynamic_property_creates_relationship());
        assert!(!proc.supports_sensitive_dynamic_properties());
    }
}
