/// Describes a configurable property on a processor.
#[derive(Debug, Clone)]
pub struct PropertyDescriptor {
    pub name: &'static str,
    pub description: &'static str,
    pub required: bool,
    pub default_value: Option<&'static str>,
    pub sensitive: bool,
    pub allowed_values: Option<&'static [&'static str]>,
    pub expression_language_supported: bool,
}

impl PropertyDescriptor {
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            required: false,
            default_value: None,
            sensitive: false,
            allowed_values: None,
            expression_language_supported: false,
        }
    }

    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub const fn default_value(mut self, val: &'static str) -> Self {
        self.default_value = Some(val);
        self
    }

    pub const fn sensitive(mut self) -> Self {
        self.sensitive = true;
        self
    }

    pub const fn allowed_values(mut self, values: &'static [&'static str]) -> Self {
        self.allowed_values = Some(values);
        self
    }

    pub const fn expression_language_supported(mut self) -> Self {
        self.expression_language_supported = true;
        self
    }
}

/// A resolved property value at runtime.
#[derive(Debug, Clone)]
pub enum PropertyValue {
    /// A string value.
    String(String),
    /// Property was not set (and has no default).
    Unset,
}

impl PropertyValue {
    /// Returns the string value, or `None` if unset.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s.as_str()),
            PropertyValue::Unset => None,
        }
    }

    /// Returns the string value, or the provided default.
    pub fn unwrap_or<'a>(&'a self, default: &'a str) -> &'a str {
        match self {
            PropertyValue::String(s) => s.as_str(),
            PropertyValue::Unset => default,
        }
    }
}
