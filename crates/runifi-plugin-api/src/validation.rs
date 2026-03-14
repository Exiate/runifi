/// Result of validating a processor's configuration.
///
/// A non-empty list of `ValidationResult` values means the processor is in an
/// *Invalid* state and cannot be started until the issues are resolved.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// The subject of the validation error (e.g., property name, relationship name).
    pub subject: String,
    /// Human-readable description of the validation issue.
    pub message: String,
}

impl ValidationResult {
    pub fn new(subject: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}': {}", self.subject, self.message)
    }
}
