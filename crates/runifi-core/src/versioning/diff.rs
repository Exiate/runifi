//! Flow diff computation — compares two flow states and reports changes.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::engine::persistence::PersistedFlowState;

/// A complete diff between two flow states.
#[derive(Debug, Clone, Serialize)]
pub struct FlowDiff {
    /// Processors that were added (present in `current`, absent in `other`).
    pub processors_added: Vec<String>,
    /// Processors that were removed (absent in `current`, present in `other`).
    pub processors_removed: Vec<String>,
    /// Processors that changed (type, scheduling, or properties differ).
    pub processors_changed: Vec<FlowDiffEntry>,
    /// Connections that were added.
    pub connections_added: Vec<String>,
    /// Connections that were removed.
    pub connections_removed: Vec<String>,
    /// Services that were added.
    pub services_added: Vec<String>,
    /// Services that were removed.
    pub services_removed: Vec<String>,
    /// Services that changed.
    pub services_changed: Vec<FlowDiffEntry>,
}

/// A single changed component with before/after details.
#[derive(Debug, Clone, Serialize)]
pub struct FlowDiffEntry {
    pub name: String,
    pub changes: Vec<String>,
}

/// Compute the diff between two flow states.
///
/// Reports changes from `other` (the version being compared) to `current`
/// (the live state). "Added" means present in `current` but not in `other`.
pub fn compute_diff(current: &PersistedFlowState, other: &PersistedFlowState) -> FlowDiff {
    // -- Processors --
    let current_procs: BTreeMap<&str, _> = current
        .processors
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();
    let other_procs: BTreeMap<&str, _> = other
        .processors
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    let current_names: BTreeSet<&str> = current_procs.keys().copied().collect();
    let other_names: BTreeSet<&str> = other_procs.keys().copied().collect();

    let processors_added: Vec<String> = current_names
        .difference(&other_names)
        .map(|n| n.to_string())
        .collect();
    let processors_removed: Vec<String> = other_names
        .difference(&current_names)
        .map(|n| n.to_string())
        .collect();

    let mut processors_changed = Vec::new();
    for name in current_names.intersection(&other_names) {
        let cur = current_procs[name];
        let oth = other_procs[name];
        let mut changes = Vec::new();

        if cur.type_name != oth.type_name {
            changes.push(format!("type: '{}' -> '{}'", oth.type_name, cur.type_name));
        }
        if cur.scheduling.strategy != oth.scheduling.strategy
            || cur.scheduling.interval_ms != oth.scheduling.interval_ms
        {
            changes.push(format!(
                "scheduling: '{} {}ms' -> '{} {}ms'",
                oth.scheduling.strategy,
                oth.scheduling.interval_ms,
                cur.scheduling.strategy,
                cur.scheduling.interval_ms,
            ));
        }

        // Compare properties (sorted for determinism).
        let cur_props: BTreeMap<&str, &str> = cur
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let oth_props: BTreeMap<&str, &str> = oth
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let all_keys: BTreeSet<&str> = cur_props.keys().chain(oth_props.keys()).copied().collect();
        for key in &all_keys {
            match (cur_props.get(key), oth_props.get(key)) {
                (Some(cv), Some(ov)) if cv != ov => {
                    changes.push(format!("property '{}': '{}' -> '{}'", key, ov, cv));
                }
                (Some(cv), None) => {
                    changes.push(format!("property '{}': added = '{}'", key, cv));
                }
                (None, Some(ov)) => {
                    changes.push(format!("property '{}': removed (was '{}')", key, ov));
                }
                _ => {}
            }
        }

        if !changes.is_empty() {
            processors_changed.push(FlowDiffEntry {
                name: name.to_string(),
                changes,
            });
        }
    }

    // -- Connections --
    // Use a canonical key: "source->relationship->destination"
    let conn_key = |s: &str, r: &str, d: &str| format!("{} -[{}]-> {}", s, r, d);

    let current_conns: BTreeSet<String> = current
        .connections
        .iter()
        .map(|c| conn_key(&c.source, &c.relationship, &c.destination))
        .collect();
    let other_conns: BTreeSet<String> = other
        .connections
        .iter()
        .map(|c| conn_key(&c.source, &c.relationship, &c.destination))
        .collect();

    let connections_added: Vec<String> = current_conns.difference(&other_conns).cloned().collect();
    let connections_removed: Vec<String> =
        other_conns.difference(&current_conns).cloned().collect();

    // -- Services --
    let current_svcs: BTreeMap<&str, _> = current
        .services
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    let other_svcs: BTreeMap<&str, _> = other
        .services
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    let current_svc_names: BTreeSet<&str> = current_svcs.keys().copied().collect();
    let other_svc_names: BTreeSet<&str> = other_svcs.keys().copied().collect();

    let services_added: Vec<String> = current_svc_names
        .difference(&other_svc_names)
        .map(|n| n.to_string())
        .collect();
    let services_removed: Vec<String> = other_svc_names
        .difference(&current_svc_names)
        .map(|n| n.to_string())
        .collect();

    let mut services_changed = Vec::new();
    for name in current_svc_names.intersection(&other_svc_names) {
        let cur = current_svcs[name];
        let oth = other_svcs[name];
        let mut changes = Vec::new();

        if cur.type_name != oth.type_name {
            changes.push(format!("type: '{}' -> '{}'", oth.type_name, cur.type_name));
        }

        let cur_props: BTreeMap<&str, &str> = cur
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let oth_props: BTreeMap<&str, &str> = oth
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let all_keys: BTreeSet<&str> = cur_props.keys().chain(oth_props.keys()).copied().collect();
        for key in &all_keys {
            match (cur_props.get(key), oth_props.get(key)) {
                (Some(cv), Some(ov)) if cv != ov => {
                    changes.push(format!("property '{}': '{}' -> '{}'", key, ov, cv));
                }
                (Some(cv), None) => {
                    changes.push(format!("property '{}': added = '{}'", key, cv));
                }
                (None, Some(ov)) => {
                    changes.push(format!("property '{}': removed (was '{}')", key, ov));
                }
                _ => {}
            }
        }

        if !changes.is_empty() {
            services_changed.push(FlowDiffEntry {
                name: name.to_string(),
                changes,
            });
        }
    }

    FlowDiff {
        processors_added,
        processors_removed,
        processors_changed,
        connections_added,
        connections_removed,
        services_added,
        services_removed,
        services_changed,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::engine::persistence::{
        PersistedConnection, PersistedFlowState, PersistedProcessor, PersistedScheduling,
    };

    use super::*;

    fn make_state(procs: Vec<(&str, &str)>, conns: Vec<(&str, &str, &str)>) -> PersistedFlowState {
        PersistedFlowState {
            version: 1,
            flow_name: "test".to_string(),
            processors: procs
                .into_iter()
                .map(|(name, type_name)| PersistedProcessor {
                    name: name.to_string(),
                    type_name: type_name.to_string(),
                    scheduling: PersistedScheduling {
                        strategy: "timer".to_string(),
                        interval_ms: 1000,
                        expression: None,
                    },
                    properties: HashMap::new(),
                })
                .collect(),
            connections: conns
                .into_iter()
                .map(|(src, rel, dst)| PersistedConnection {
                    source: src.to_string(),
                    relationship: rel.to_string(),
                    destination: dst.to_string(),
                    back_pressure: None,
                    expiration: None,
                    priority: None,
                    priority_attribute: None,
                })
                .collect(),
            positions: HashMap::new(),
            services: vec![],
            labels: vec![],
            process_groups: vec![],
        }
    }

    #[test]
    fn test_identical_states_produce_empty_diff() {
        let state = make_state(
            vec![("gen", "GenerateFlowFile")],
            vec![("gen", "success", "log")],
        );
        let diff = compute_diff(&state, &state);
        assert!(diff.processors_added.is_empty());
        assert!(diff.processors_removed.is_empty());
        assert!(diff.processors_changed.is_empty());
        assert!(diff.connections_added.is_empty());
        assert!(diff.connections_removed.is_empty());
    }

    #[test]
    fn test_added_processor() {
        let old = make_state(vec![("gen", "GenerateFlowFile")], vec![]);
        let new = make_state(
            vec![("gen", "GenerateFlowFile"), ("log", "LogAttribute")],
            vec![],
        );
        let diff = compute_diff(&new, &old);
        assert_eq!(diff.processors_added, vec!["log"]);
        assert!(diff.processors_removed.is_empty());
    }

    #[test]
    fn test_removed_processor() {
        let old = make_state(
            vec![("gen", "GenerateFlowFile"), ("log", "LogAttribute")],
            vec![],
        );
        let new = make_state(vec![("gen", "GenerateFlowFile")], vec![]);
        let diff = compute_diff(&new, &old);
        assert!(diff.processors_added.is_empty());
        assert_eq!(diff.processors_removed, vec!["log"]);
    }

    #[test]
    fn test_changed_processor_type() {
        let old = make_state(vec![("proc", "GenerateFlowFile")], vec![]);
        let new = make_state(vec![("proc", "LogAttribute")], vec![]);
        let diff = compute_diff(&new, &old);
        assert_eq!(diff.processors_changed.len(), 1);
        assert_eq!(diff.processors_changed[0].name, "proc");
        assert!(diff.processors_changed[0].changes[0].contains("type:"));
    }

    #[test]
    fn test_changed_processor_properties() {
        let mut old = make_state(vec![("gen", "GenerateFlowFile")], vec![]);
        old.processors[0]
            .properties
            .insert("File Size".to_string(), "1024".to_string());

        let mut new = make_state(vec![("gen", "GenerateFlowFile")], vec![]);
        new.processors[0]
            .properties
            .insert("File Size".to_string(), "2048".to_string());

        let diff = compute_diff(&new, &old);
        assert_eq!(diff.processors_changed.len(), 1);
        assert!(diff.processors_changed[0].changes[0].contains("File Size"));
    }

    #[test]
    fn test_added_connection() {
        let old = make_state(vec![], vec![]);
        let new = make_state(vec![], vec![("gen", "success", "log")]);
        let diff = compute_diff(&new, &old);
        assert_eq!(diff.connections_added.len(), 1);
        assert!(diff.connections_removed.is_empty());
    }

    #[test]
    fn test_removed_connection() {
        let old = make_state(vec![], vec![("gen", "success", "log")]);
        let new = make_state(vec![], vec![]);
        let diff = compute_diff(&new, &old);
        assert!(diff.connections_added.is_empty());
        assert_eq!(diff.connections_removed.len(), 1);
    }
}
