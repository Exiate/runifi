use std::fmt;
use std::ops::Deref;

use zeroize::{Zeroize, ZeroizeOnDrop};

/// A string wrapper that securely zeroes its memory on drop and masks its
/// value in `Debug` and `Display` output.
///
/// Use this for any property value that must not leak into logs, error
/// messages, or serialized responses. `Serialize` is intentionally **not**
/// implemented to prevent accidental exfiltration.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SensitiveString {
    inner: String,
}

impl SensitiveString {
    /// Create a new `SensitiveString` from the given value.
    pub fn new(value: String) -> Self {
        Self { inner: value }
    }

    /// Consume this wrapper and return the inner string.
    ///
    /// The caller assumes responsibility for protecting the value.
    pub fn into_inner(self) -> String {
        // We need to extract the inner value without triggering zeroize.
        // Clone the string before self is dropped (and zeroized).
        self.inner.clone()
    }
}

impl Deref for SensitiveString {
    type Target = str;

    fn deref(&self) -> &str {
        &self.inner
    }
}

impl fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"********\"")
    }
}

impl fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "********")
    }
}

impl From<String> for SensitiveString {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for SensitiveString {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

impl PartialEq for SensitiveString {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for SensitiveString {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_value() {
        let s = SensitiveString::new("my-secret-password".to_string());
        let debug_output = format!("{:?}", s);
        assert_eq!(debug_output, "\"********\"");
        assert!(!debug_output.contains("my-secret-password"));
    }

    #[test]
    fn display_masks_value() {
        let s = SensitiveString::new("my-secret-password".to_string());
        let display_output = format!("{}", s);
        assert_eq!(display_output, "********");
        assert!(!display_output.contains("my-secret-password"));
    }

    #[test]
    fn deref_exposes_value() {
        let s = SensitiveString::new("my-secret-password".to_string());
        let value: &str = &s;
        assert_eq!(value, "my-secret-password");
    }

    #[test]
    fn from_string() {
        let s: SensitiveString = "hello".to_string().into();
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn from_str_ref() {
        let s: SensitiveString = "hello".into();
        assert_eq!(&*s, "hello");
    }

    #[test]
    fn clone_produces_independent_copy() {
        let s = SensitiveString::new("secret".to_string());
        let cloned = s.clone();
        assert_eq!(&*s, &*cloned);
    }

    #[test]
    fn into_inner_returns_value() {
        let s = SensitiveString::new("the-secret".to_string());
        let value = s.into_inner();
        assert_eq!(value, "the-secret");
    }

    #[test]
    fn equality_works() {
        let a = SensitiveString::new("same".to_string());
        let b = SensitiveString::new("same".to_string());
        let c = SensitiveString::new("different".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn zeroize_clears_value() {
        let mut s = SensitiveString::new("secret-data".to_string());
        s.zeroize();
        // After zeroize, the inner string should be empty (zeroed).
        assert!(s.inner.is_empty());
    }
}
