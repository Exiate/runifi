use std::collections::HashMap;

/// Unique identifier for a process group.
pub type ProcessGroupId = String;

/// Information about a process group, visible to the API and engine handle.
///
/// Process Groups are hierarchical containers that organize processors,
/// connections, input/output ports, and child process groups. They provide
/// variable scoping (child groups inherit and can override parent variables)
/// and serve as the boundary for flow versioning.
#[derive(Debug, Clone)]
pub struct ProcessGroupInfo {
    /// Unique identifier for this process group.
    pub id: ProcessGroupId,
    /// Human-readable name.
    pub name: String,
    /// Input ports — receive FlowFiles from connections in the parent group.
    pub input_ports: Vec<PortInfo>,
    /// Output ports — send FlowFiles to connections in the parent group.
    pub output_ports: Vec<PortInfo>,
    /// Names of processors that belong to this group.
    pub processor_names: Vec<String>,
    /// IDs of connections that belong to this group.
    pub connection_ids: Vec<String>,
    /// IDs of child process groups.
    pub child_group_ids: Vec<ProcessGroupId>,
    /// ID of the parent process group, or `None` for top-level groups.
    pub parent_group_id: Option<ProcessGroupId>,
    /// Group-scoped variables. Child groups inherit parent variables
    /// and can override them with their own values.
    pub variables: HashMap<String, String>,
}

/// Information about an input or output port on a process group.
///
/// Ports are named connection endpoints that bridge the boundary between
/// a process group and its parent. An input port receives FlowFiles from
/// a connection in the parent group and makes them available to processors
/// inside the group. An output port receives FlowFiles from processors
/// inside the group and sends them to connections in the parent group.
#[derive(Debug, Clone)]
pub struct PortInfo {
    /// Unique identifier for this port.
    pub id: String,
    /// Human-readable name for this port.
    pub name: String,
    /// The type of port (input or output).
    pub port_type: PortType,
    /// The process group this port belongs to.
    pub group_id: ProcessGroupId,
}

/// The type of a port on a process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortType {
    /// Receives FlowFiles from the parent group.
    Input,
    /// Sends FlowFiles to the parent group.
    Output,
}

impl ProcessGroupInfo {
    /// Create a new process group with the given id and name.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            input_ports: Vec::new(),
            output_ports: Vec::new(),
            processor_names: Vec::new(),
            connection_ids: Vec::new(),
            child_group_ids: Vec::new(),
            parent_group_id: None,
            variables: HashMap::new(),
        }
    }

    /// Resolve a variable by name, checking this group's variables first,
    /// then walking up the parent chain. Returns `None` if not found.
    pub fn resolve_variable<'a>(
        &'a self,
        name: &str,
        all_groups: &'a [ProcessGroupInfo],
    ) -> Option<&'a str> {
        // Check own variables first.
        if let Some(value) = self.variables.get(name) {
            return Some(value.as_str());
        }

        // Walk up to parent.
        if let Some(ref parent_id) = self.parent_group_id
            && let Some(parent) = all_groups.iter().find(|g| g.id == *parent_id)
        {
            return parent.resolve_variable(name, all_groups);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_process_group() {
        let pg = ProcessGroupInfo::new("pg-1", "Test Group");
        assert_eq!(pg.id, "pg-1");
        assert_eq!(pg.name, "Test Group");
        assert!(pg.input_ports.is_empty());
        assert!(pg.output_ports.is_empty());
        assert!(pg.processor_names.is_empty());
        assert!(pg.connection_ids.is_empty());
        assert!(pg.child_group_ids.is_empty());
        assert!(pg.parent_group_id.is_none());
        assert!(pg.variables.is_empty());
    }

    #[test]
    fn test_variable_resolution_own_variable() {
        let mut pg = ProcessGroupInfo::new("pg-1", "Group 1");
        pg.variables.insert("key".to_string(), "value".to_string());

        let groups = vec![pg.clone()];
        assert_eq!(pg.resolve_variable("key", &groups), Some("value"));
        assert_eq!(pg.resolve_variable("missing", &groups), None);
    }

    #[test]
    fn test_variable_resolution_inherits_from_parent() {
        let mut parent = ProcessGroupInfo::new("parent", "Parent");
        parent
            .variables
            .insert("inherited".to_string(), "from-parent".to_string());
        parent
            .variables
            .insert("overridden".to_string(), "parent-value".to_string());

        let mut child = ProcessGroupInfo::new("child", "Child");
        child.parent_group_id = Some("parent".to_string());
        child
            .variables
            .insert("overridden".to_string(), "child-value".to_string());
        child
            .variables
            .insert("own".to_string(), "child-only".to_string());

        let groups = vec![parent, child.clone()];

        // Child's own variable.
        assert_eq!(child.resolve_variable("own", &groups), Some("child-only"));

        // Inherited from parent.
        assert_eq!(
            child.resolve_variable("inherited", &groups),
            Some("from-parent")
        );

        // Overridden by child.
        assert_eq!(
            child.resolve_variable("overridden", &groups),
            Some("child-value")
        );

        // Not found anywhere.
        assert_eq!(child.resolve_variable("missing", &groups), None);
    }

    #[test]
    fn test_port_info() {
        let port = PortInfo {
            id: "port-1".to_string(),
            name: "raw-data".to_string(),
            port_type: PortType::Input,
            group_id: "pg-1".to_string(),
        };
        assert_eq!(port.port_type, PortType::Input);
        assert_eq!(port.name, "raw-data");
    }
}
