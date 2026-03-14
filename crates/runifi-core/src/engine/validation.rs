use runifi_plugin_api::Processor;
use runifi_plugin_api::context::ProcessContext;
use runifi_plugin_api::property::PropertyValue;
use runifi_plugin_api::validation::ValidationResult;

use super::handle::PropertyDescriptorInfo;

/// Validate a processor's configuration by checking:
/// 1. Required properties without defaults are set
/// 2. Property values are within allowed_values (if specified)
/// 3. Custom processor validation via `Processor::validate()`
///
/// Returns an empty list if the processor is valid.
pub fn validate_processor(
    descriptors: &[PropertyDescriptorInfo],
    context: &dyn ProcessContext,
    processor: Option<&dyn Processor>,
) -> Vec<ValidationResult> {
    let mut errors = Vec::new();

    // Check required properties.
    for desc in descriptors {
        if desc.required && desc.default_value.is_none() {
            let value = context.get_property(&desc.name);
            if matches!(value, PropertyValue::Unset) {
                errors.push(ValidationResult::new(
                    &desc.name,
                    format!("Required property '{}' is not set", desc.name),
                ));
            }
        }
    }

    // Check allowed values.
    for desc in descriptors {
        if let Some(ref allowed) = desc.allowed_values {
            let value = context.get_property(&desc.name);
            if let PropertyValue::String(ref val) = value
                && !allowed.iter().any(|a| a == val)
            {
                errors.push(ValidationResult::new(
                    &desc.name,
                    format!(
                        "Invalid value '{}' for property '{}'. Allowed: {:?}",
                        val, desc.name, allowed
                    ),
                ));
            }
        }
    }

    // Delegate to custom processor validation.
    if let Some(proc) = processor {
        errors.extend(proc.validate(context));
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use runifi_plugin_api::property::PropertyValue;

    struct TestContext {
        props: Vec<(String, String)>,
    }

    impl TestContext {
        fn new(props: Vec<(&str, &str)>) -> Self {
            Self {
                props: props
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            }
        }
    }

    impl ProcessContext for TestContext {
        fn get_property(&self, name: &str) -> PropertyValue {
            self.props
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| PropertyValue::String(v.clone()))
                .unwrap_or(PropertyValue::Unset)
        }
        fn name(&self) -> &str {
            "test"
        }
        fn id(&self) -> &str {
            "test-id"
        }
        fn yield_duration_ms(&self) -> u64 {
            1000
        }
    }

    #[test]
    fn valid_when_no_descriptors() {
        let ctx = TestContext::new(vec![]);
        let errors = validate_processor(&[], &ctx, None);
        assert!(errors.is_empty());
    }

    #[test]
    fn required_property_missing() {
        let desc = PropertyDescriptorInfo {
            name: "path".to_string(),
            description: "File path".to_string(),
            required: true,
            default_value: None,
            sensitive: false,
            allowed_values: None,
        };
        let ctx = TestContext::new(vec![]);
        let errors = validate_processor(&[desc], &ctx, None);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("Required property"));
    }

    #[test]
    fn required_property_with_default_is_valid() {
        let desc = PropertyDescriptorInfo {
            name: "path".to_string(),
            description: "File path".to_string(),
            required: true,
            default_value: Some("/tmp".to_string()),
            sensitive: false,
            allowed_values: None,
        };
        let ctx = TestContext::new(vec![]);
        let errors = validate_processor(&[desc], &ctx, None);
        assert!(errors.is_empty());
    }

    #[test]
    fn invalid_allowed_value() {
        let desc = PropertyDescriptorInfo {
            name: "mode".to_string(),
            description: "Operating mode".to_string(),
            required: false,
            default_value: None,
            sensitive: false,
            allowed_values: Some(vec!["fast".to_string(), "slow".to_string()]),
        };
        let ctx = TestContext::new(vec![("mode", "invalid")]);
        let errors = validate_processor(&[desc], &ctx, None);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("Invalid value"));
    }

    #[test]
    fn valid_allowed_value() {
        let desc = PropertyDescriptorInfo {
            name: "mode".to_string(),
            description: "Operating mode".to_string(),
            required: false,
            default_value: None,
            sensitive: false,
            allowed_values: Some(vec!["fast".to_string(), "slow".to_string()]),
        };
        let ctx = TestContext::new(vec![("mode", "fast")]);
        let errors = validate_processor(&[desc], &ctx, None);
        assert!(errors.is_empty());
    }

    #[test]
    fn required_property_present_is_valid() {
        let desc = PropertyDescriptorInfo {
            name: "path".to_string(),
            description: "File path".to_string(),
            required: true,
            default_value: None,
            sensitive: false,
            allowed_values: None,
        };
        let ctx = TestContext::new(vec![("path", "/data")]);
        let errors = validate_processor(&[desc], &ctx, None);
        assert!(errors.is_empty());
    }
}
