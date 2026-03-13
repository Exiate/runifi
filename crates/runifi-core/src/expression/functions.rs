/// Built-in function implementations for the RuniFi Expression Language.
///
/// Functions operate on a subject string value and produce a new string value.
/// Boolean results are represented as `"true"` / `"false"` strings.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FunctionError {
    #[error("unknown function: {0}")]
    UnknownFunction(String),
    #[error("function '{name}' expected {expected} arguments, got {got}")]
    ArityMismatch {
        name: String,
        expected: String,
        got: usize,
    },
    #[error("function '{name}' argument error: {reason}")]
    ArgumentError { name: String, reason: String },
    #[error("invalid regex in '{name}': {reason}")]
    RegexError { name: String, reason: String },
}

/// Evaluate a built-in function.
///
/// `subject` is the current value being operated on.
/// `name` is the function name.
/// `args` are the evaluated argument values (already resolved to strings).
pub fn evaluate_function(
    subject: &str,
    name: &str,
    args: &[String],
) -> Result<String, FunctionError> {
    match name {
        // --- String functions ---
        "toUpper" => {
            check_arity(name, args, 0)?;
            Ok(subject.to_uppercase())
        }
        "toLower" => {
            check_arity(name, args, 0)?;
            Ok(subject.to_lowercase())
        }
        "trim" => {
            check_arity(name, args, 0)?;
            Ok(subject.trim().to_string())
        }
        "substring" => {
            check_arity_range(name, args, 1, 2)?;
            let start = parse_usize_arg(name, &args[0])?;
            let end = if args.len() == 2 {
                parse_usize_arg(name, &args[1])?
            } else {
                subject.len()
            };
            let start = start.min(subject.len());
            let end = end.min(subject.len()).max(start);
            Ok(subject[start..end].to_string())
        }
        "substringBefore" => {
            check_arity(name, args, 1)?;
            Ok(subject
                .find(args[0].as_str())
                .map_or_else(|| subject.to_string(), |i| subject[..i].to_string()))
        }
        "substringAfter" => {
            check_arity(name, args, 1)?;
            let needle = &args[0];
            Ok(subject.find(needle.as_str()).map_or_else(
                || subject.to_string(),
                |i| subject[i + needle.len()..].to_string(),
            ))
        }
        "substringBeforeLast" => {
            check_arity(name, args, 1)?;
            Ok(subject
                .rfind(args[0].as_str())
                .map_or_else(|| subject.to_string(), |i| subject[..i].to_string()))
        }
        "substringAfterLast" => {
            check_arity(name, args, 1)?;
            let needle = &args[0];
            Ok(subject.rfind(needle.as_str()).map_or_else(
                || subject.to_string(),
                |i| subject[i + needle.len()..].to_string(),
            ))
        }
        "append" => {
            check_arity(name, args, 1)?;
            Ok(format!("{}{}", subject, args[0]))
        }
        "prepend" => {
            check_arity(name, args, 1)?;
            Ok(format!("{}{}", args[0], subject))
        }
        "replace" => {
            check_arity(name, args, 2)?;
            Ok(subject.replace(args[0].as_str(), &args[1]))
        }
        "replaceAll" => {
            check_arity(name, args, 2)?;
            let regex =
                regex_lite::Regex::new(&args[0]).map_err(|e| FunctionError::RegexError {
                    name: name.to_string(),
                    reason: e.to_string(),
                })?;
            Ok(regex.replace_all(subject, args[1].as_str()).into_owned())
        }
        "length" => {
            check_arity(name, args, 0)?;
            Ok(subject.len().to_string())
        }

        // --- Boolean functions ---
        "equals" => {
            check_arity(name, args, 1)?;
            Ok((subject == args[0]).to_string())
        }
        "equalsIgnoreCase" => {
            check_arity(name, args, 1)?;
            Ok(subject.eq_ignore_ascii_case(&args[0]).to_string())
        }
        "contains" => {
            check_arity(name, args, 1)?;
            Ok(subject.contains(args[0].as_str()).to_string())
        }
        "startsWith" => {
            check_arity(name, args, 1)?;
            Ok(subject.starts_with(args[0].as_str()).to_string())
        }
        "endsWith" => {
            check_arity(name, args, 1)?;
            Ok(subject.ends_with(args[0].as_str()).to_string())
        }
        "isEmpty" => {
            check_arity(name, args, 0)?;
            Ok(subject.is_empty().to_string())
        }
        "isNull" => {
            // isNull is handled specially in the evaluator for missing attributes.
            // When called on a resolved value, it's always false.
            check_arity(name, args, 0)?;
            Ok("false".to_string())
        }
        "not" => {
            check_arity(name, args, 0)?;
            Ok((!is_truthy(subject)).to_string())
        }
        "and" => {
            check_arity(name, args, 1)?;
            Ok((is_truthy(subject) && is_truthy(&args[0])).to_string())
        }
        "or" => {
            check_arity(name, args, 1)?;
            Ok((is_truthy(subject) || is_truthy(&args[0])).to_string())
        }

        // --- Type coercion ---
        "toString" => {
            check_arity(name, args, 0)?;
            Ok(subject.to_string())
        }
        "toNumber" => {
            check_arity(name, args, 0)?;
            // Attempt to parse as a number, return the canonical form.
            match subject.parse::<f64>() {
                Ok(n) => {
                    if n.fract() == 0.0 && n.is_finite() {
                        Ok((n as i64).to_string())
                    } else {
                        Ok(n.to_string())
                    }
                }
                Err(_) => Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("cannot convert '{}' to number", subject),
                }),
            }
        }

        // --- Special ---
        "literal" => {
            // `literal` is handled as a subjectless function in the evaluator.
            // If called as a chained function, it just returns the argument.
            check_arity(name, args, 1)?;
            Ok(args[0].clone())
        }
        "ifElse" => {
            check_arity(name, args, 2)?;
            if is_truthy(subject) {
                Ok(args[0].clone())
            } else {
                Ok(args[1].clone())
            }
        }

        _ => Err(FunctionError::UnknownFunction(name.to_string())),
    }
}

/// Check a boolean-like string value.
fn is_truthy(s: &str) -> bool {
    s == "true"
}

fn check_arity(name: &str, args: &[String], expected: usize) -> Result<(), FunctionError> {
    if args.len() != expected {
        Err(FunctionError::ArityMismatch {
            name: name.to_string(),
            expected: expected.to_string(),
            got: args.len(),
        })
    } else {
        Ok(())
    }
}

fn check_arity_range(
    name: &str,
    args: &[String],
    min: usize,
    max: usize,
) -> Result<(), FunctionError> {
    if args.len() < min || args.len() > max {
        Err(FunctionError::ArityMismatch {
            name: name.to_string(),
            expected: format!("{}-{}", min, max),
            got: args.len(),
        })
    } else {
        Ok(())
    }
}

fn parse_usize_arg(name: &str, arg: &str) -> Result<usize, FunctionError> {
    // Handle float-formatted numbers like "0.0" → 0
    if let Ok(n) = arg.parse::<f64>()
        && n >= 0.0
        && n.is_finite()
    {
        return Ok(n as usize);
    }
    arg.parse::<usize>()
        .map_err(|_| FunctionError::ArgumentError {
            name: name.to_string(),
            reason: format!("expected non-negative integer, got '{}'", arg),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_upper() {
        assert_eq!(evaluate_function("hello", "toUpper", &[]).unwrap(), "HELLO");
    }

    #[test]
    fn test_to_lower() {
        assert_eq!(evaluate_function("HELLO", "toLower", &[]).unwrap(), "hello");
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            evaluate_function("  hello  ", "trim", &[]).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_substring() {
        assert_eq!(
            evaluate_function("hello", "substring", &["1".into()]).unwrap(),
            "ello"
        );
        assert_eq!(
            evaluate_function("hello", "substring", &["1".into(), "3".into()]).unwrap(),
            "el"
        );
    }

    #[test]
    fn test_substring_clamped() {
        assert_eq!(
            evaluate_function("hi", "substring", &["0".into(), "100".into()]).unwrap(),
            "hi"
        );
        assert_eq!(
            evaluate_function("hi", "substring", &["50".into()]).unwrap(),
            ""
        );
    }

    #[test]
    fn test_substring_before_after() {
        assert_eq!(
            evaluate_function("hello.world.txt", "substringBefore", &[".".into()]).unwrap(),
            "hello"
        );
        assert_eq!(
            evaluate_function("hello.world.txt", "substringAfter", &[".".into()]).unwrap(),
            "world.txt"
        );
        assert_eq!(
            evaluate_function("hello.world.txt", "substringBeforeLast", &[".".into()]).unwrap(),
            "hello.world"
        );
        assert_eq!(
            evaluate_function("hello.world.txt", "substringAfterLast", &[".".into()]).unwrap(),
            "txt"
        );
    }

    #[test]
    fn test_substring_not_found() {
        assert_eq!(
            evaluate_function("hello", "substringBefore", &["x".into()]).unwrap(),
            "hello"
        );
        assert_eq!(
            evaluate_function("hello", "substringAfterLast", &["x".into()]).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_append_prepend() {
        assert_eq!(
            evaluate_function("hello", "append", &[" world".into()]).unwrap(),
            "hello world"
        );
        assert_eq!(
            evaluate_function("world", "prepend", &["hello ".into()]).unwrap(),
            "hello world"
        );
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            evaluate_function("hello world", "replace", &["world".into(), "rust".into()]).unwrap(),
            "hello rust"
        );
    }

    #[test]
    fn test_replace_all_regex() {
        assert_eq!(
            evaluate_function(
                "abc123def456",
                "replaceAll",
                &["[0-9]+".into(), "NUM".into()]
            )
            .unwrap(),
            "abcNUMdefNUM"
        );
    }

    #[test]
    fn test_length() {
        assert_eq!(evaluate_function("hello", "length", &[]).unwrap(), "5");
        assert_eq!(evaluate_function("", "length", &[]).unwrap(), "0");
    }

    #[test]
    fn test_equals() {
        assert_eq!(
            evaluate_function("hello", "equals", &["hello".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("hello", "equals", &["world".into()]).unwrap(),
            "false"
        );
    }

    #[test]
    fn test_equals_ignore_case() {
        assert_eq!(
            evaluate_function("Hello", "equalsIgnoreCase", &["hello".into()]).unwrap(),
            "true"
        );
    }

    #[test]
    fn test_contains() {
        assert_eq!(
            evaluate_function("hello world", "contains", &["world".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("hello", "contains", &["xyz".into()]).unwrap(),
            "false"
        );
    }

    #[test]
    fn test_starts_ends_with() {
        assert_eq!(
            evaluate_function("hello", "startsWith", &["he".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("hello", "endsWith", &["lo".into()]).unwrap(),
            "true"
        );
    }

    #[test]
    fn test_is_empty() {
        assert_eq!(evaluate_function("", "isEmpty", &[]).unwrap(), "true");
        assert_eq!(evaluate_function("x", "isEmpty", &[]).unwrap(), "false");
    }

    #[test]
    fn test_not_and_or() {
        assert_eq!(evaluate_function("true", "not", &[]).unwrap(), "false");
        assert_eq!(evaluate_function("false", "not", &[]).unwrap(), "true");
        assert_eq!(
            evaluate_function("true", "and", &["true".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("true", "and", &["false".into()]).unwrap(),
            "false"
        );
        assert_eq!(
            evaluate_function("false", "or", &["true".into()]).unwrap(),
            "true"
        );
    }

    #[test]
    fn test_to_number() {
        assert_eq!(evaluate_function("42", "toNumber", &[]).unwrap(), "42");
        assert_eq!(evaluate_function("3.14", "toNumber", &[]).unwrap(), "3.14");
        assert!(evaluate_function("abc", "toNumber", &[]).is_err());
    }

    #[test]
    fn test_if_else() {
        assert_eq!(
            evaluate_function("true", "ifElse", &["yes".into(), "no".into()]).unwrap(),
            "yes"
        );
        assert_eq!(
            evaluate_function("false", "ifElse", &["yes".into(), "no".into()]).unwrap(),
            "no"
        );
    }

    #[test]
    fn test_unknown_function() {
        assert!(evaluate_function("x", "nonexistent", &[]).is_err());
    }

    #[test]
    fn test_arity_mismatch() {
        assert!(evaluate_function("x", "toUpper", &["extra".into()]).is_err());
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            evaluate_function("Héllo Wörld", "toUpper", &[]).unwrap(),
            "HÉLLO WÖRLD"
        );
        assert_eq!(
            evaluate_function("Héllo", "length", &[]).unwrap(),
            "6" // byte length
        );
    }

    #[test]
    fn test_empty_string_functions() {
        assert_eq!(evaluate_function("", "toUpper", &[]).unwrap(), "");
        assert_eq!(evaluate_function("", "toLower", &[]).unwrap(), "");
        assert_eq!(evaluate_function("", "trim", &[]).unwrap(), "");
        assert_eq!(evaluate_function("", "append", &["x".into()]).unwrap(), "x");
        assert_eq!(
            evaluate_function("", "substring", &["0".into()]).unwrap(),
            ""
        );
    }
}
