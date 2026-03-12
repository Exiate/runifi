/// A named relationship that a processor can route FlowFiles to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationship {
    pub name: &'static str,
    pub description: &'static str,
    pub auto_terminated: bool,
}

impl Relationship {
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            auto_terminated: false,
        }
    }

    pub const fn auto_terminated(mut self) -> Self {
        self.auto_terminated = true;
        self
    }
}

pub const REL_SUCCESS: Relationship =
    Relationship::new("success", "FlowFiles processed successfully");
pub const REL_FAILURE: Relationship =
    Relationship::new("failure", "FlowFiles that failed processing");
pub const REL_ORIGINAL: Relationship = Relationship::new("original", "The original FlowFile");
