/// Built-in function implementations for the RuniFi Expression Language.
///
/// Functions operate on a subject string value and produce a new string value.
/// Boolean results are represented as `"true"` / `"false"` strings.
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::DateTime;
use md5::Md5;
use sha2::{Digest, Sha256, Sha512};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use uuid::Uuid;

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
            let char_count = subject.chars().count();
            let start = start.min(char_count);
            let end = if args.len() == 2 {
                parse_usize_arg(name, &args[1])?.min(char_count).max(start)
            } else {
                char_count
            };
            Ok(subject.chars().skip(start).take(end - start).collect())
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
            Ok(subject.chars().count().to_string())
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

        // --- Encoding / Hash functions ---
        "urlEncode" => {
            check_arity(name, args, 0)?;
            Ok(urlencoding::encode(subject).into_owned())
        }
        "urlDecode" => {
            check_arity(name, args, 0)?;
            urlencoding::decode(subject)
                .map(|s| s.into_owned())
                .map_err(|e| FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("invalid URL encoding: {}", e),
                })
        }
        "base64Encode" => {
            check_arity(name, args, 0)?;
            Ok(BASE64.encode(subject.as_bytes()))
        }
        "base64Decode" => {
            check_arity(name, args, 0)?;
            let bytes =
                BASE64
                    .decode(subject.as_bytes())
                    .map_err(|e| FunctionError::ArgumentError {
                        name: name.to_string(),
                        reason: format!("invalid base64: {}", e),
                    })?;
            String::from_utf8(bytes).map_err(|e| FunctionError::ArgumentError {
                name: name.to_string(),
                reason: format!("decoded bytes are not valid UTF-8: {}", e),
            })
        }
        "hash" => {
            check_arity(name, args, 1)?;
            match args[0].as_str() {
                "MD5" | "md5" => {
                    let mut hasher = Md5::new();
                    hasher.update(subject.as_bytes());
                    Ok(hex::encode(hasher.finalize()))
                }
                "SHA-256" | "sha-256" | "SHA256" | "sha256" => {
                    let mut hasher = Sha256::new();
                    hasher.update(subject.as_bytes());
                    Ok(hex::encode(hasher.finalize()))
                }
                "SHA-512" | "sha-512" | "SHA512" | "sha512" => {
                    let mut hasher = Sha512::new();
                    hasher.update(subject.as_bytes());
                    Ok(hex::encode(hasher.finalize()))
                }
                other => Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!(
                        "unsupported hash algorithm: '{}'. Supported: MD5, SHA-256, SHA-512",
                        other
                    ),
                }),
            }
        }

        // --- Date/Time functions ---
        "now" => {
            check_arity(name, args, 0)?;
            Ok(chrono::Utc::now().timestamp_millis().to_string())
        }
        "format" => {
            check_arity(name, args, 1)?;
            let millis = subject
                .parse::<i64>()
                .map_err(|_| FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("expected epoch millis, got '{}'", subject),
                })?;
            let dt = DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("invalid timestamp millis: {}", millis),
                }
            })?;
            Ok(dt.format(&args[0]).to_string())
        }
        "toDate" => {
            check_arity(name, args, 1)?;
            let dt = chrono::NaiveDateTime::parse_from_str(subject, &args[0]).map_err(|e| {
                FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!(
                        "cannot parse '{}' with pattern '{}': {}",
                        subject, args[0], e
                    ),
                }
            })?;
            Ok(dt.and_utc().timestamp_millis().to_string())
        }

        // --- Math functions ---
        "plus" => {
            check_arity(name, args, 1)?;
            let a = parse_f64_arg(name, subject)?;
            let b = parse_f64_arg(name, &args[0])?;
            Ok(format_number(a + b))
        }
        "minus" => {
            check_arity(name, args, 1)?;
            let a = parse_f64_arg(name, subject)?;
            let b = parse_f64_arg(name, &args[0])?;
            Ok(format_number(a - b))
        }
        "multiply" => {
            check_arity(name, args, 1)?;
            let a = parse_f64_arg(name, subject)?;
            let b = parse_f64_arg(name, &args[0])?;
            Ok(format_number(a * b))
        }
        "divide" => {
            check_arity(name, args, 1)?;
            let a = parse_f64_arg(name, subject)?;
            let b = parse_f64_arg(name, &args[0])?;
            if b == 0.0 {
                return Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: "division by zero".to_string(),
                });
            }
            Ok(format_number(a / b))
        }
        "mod" => {
            check_arity(name, args, 1)?;
            let a = parse_f64_arg(name, subject)?;
            let b = parse_f64_arg(name, &args[0])?;
            if b == 0.0 {
                return Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: "modulo by zero".to_string(),
                });
            }
            Ok(format_number(a % b))
        }
        "toRadix" => {
            check_arity_range(name, args, 1, 2)?;
            let num = subject
                .parse::<i64>()
                .map_err(|_| FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("expected integer, got '{}'", subject),
                })?;
            let radix = parse_usize_arg(name, &args[0])?;
            if !(2..=36).contains(&radix) {
                return Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!("radix must be 2-36, got {}", radix),
                });
            }
            let result = format_radix(num, radix as u32);
            if args.len() == 2 {
                let min_width = parse_usize_arg(name, &args[1])?;
                if result.len() < min_width {
                    Ok(format!("{:0>width$}", result, width = min_width))
                } else {
                    Ok(result)
                }
            } else {
                Ok(result)
            }
        }
        "math" => {
            check_arity(name, args, 1)?;
            let val = parse_f64_arg(name, subject)?;
            match args[0].as_str() {
                "abs" => Ok(format_number(val.abs())),
                "ceil" | "ceiling" => Ok(format_number(val.ceil())),
                "floor" => Ok(format_number(val.floor())),
                "round" => Ok(format_number(val.round())),
                other => Err(FunctionError::ArgumentError {
                    name: name.to_string(),
                    reason: format!(
                        "unknown math operation: '{}'. Supported: abs, ceil, floor, round",
                        other
                    ),
                }),
            }
        }

        // --- Additional String functions ---
        "indexOf" => {
            check_arity(name, args, 1)?;
            Ok(subject
                .find(args[0].as_str())
                .map_or("-1".to_string(), |i| i.to_string()))
        }
        "lastIndexOf" => {
            check_arity(name, args, 1)?;
            Ok(subject
                .rfind(args[0].as_str())
                .map_or("-1".to_string(), |i| i.to_string()))
        }
        "padLeft" => {
            check_arity(name, args, 2)?;
            let target_len = parse_usize_arg(name, &args[0])?;
            let pad_char = args[1].chars().next().unwrap_or(' ');
            let current_len = subject.chars().count();
            if current_len >= target_len {
                Ok(subject.to_string())
            } else {
                let padding: String =
                    std::iter::repeat_n(pad_char, target_len - current_len).collect();
                Ok(format!("{}{}", padding, subject))
            }
        }
        "padRight" => {
            check_arity(name, args, 2)?;
            let target_len = parse_usize_arg(name, &args[0])?;
            let pad_char = args[1].chars().next().unwrap_or(' ');
            let current_len = subject.chars().count();
            if current_len >= target_len {
                Ok(subject.to_string())
            } else {
                let padding: String =
                    std::iter::repeat_n(pad_char, target_len - current_len).collect();
                Ok(format!("{}{}", subject, padding))
            }
        }
        "find" => {
            check_arity(name, args, 1)?;
            let regex =
                regex_lite::Regex::new(&args[0]).map_err(|e| FunctionError::RegexError {
                    name: name.to_string(),
                    reason: e.to_string(),
                })?;
            Ok(regex
                .find(subject)
                .map_or(String::new(), |m| m.as_str().to_string()))
        }
        "matches" => {
            check_arity(name, args, 1)?;
            let regex =
                regex_lite::Regex::new(&args[0]).map_err(|e| FunctionError::RegexError {
                    name: name.to_string(),
                    reason: e.to_string(),
                })?;
            Ok(regex.is_match(subject).to_string())
        }
        "in" => {
            if args.is_empty() {
                return Err(FunctionError::ArityMismatch {
                    name: name.to_string(),
                    expected: "1+".to_string(),
                    got: 0,
                });
            }
            Ok(args.iter().any(|a| a == subject).to_string())
        }
        "count" => {
            check_arity(name, args, 1)?;
            Ok(subject.matches(args[0].as_str()).count().to_string())
        }
        "join" => {
            check_arity(name, args, 1)?;
            Ok(subject.to_string())
        }
        "getDelimitedField" => {
            check_arity_range(name, args, 1, 2)?;
            let index = parse_usize_arg(name, &args[0])?;
            let delimiter = if args.len() >= 2 {
                args[1].as_str()
            } else {
                ","
            };
            let fields: Vec<&str> = subject.split(delimiter).collect();
            if index == 0 || index > fields.len() {
                Ok(String::new())
            } else {
                Ok(fields[index - 1].trim().to_string())
            }
        }

        // --- Escape functions ---
        "escapeJson" => {
            check_arity(name, args, 0)?;
            Ok(escape_json(subject))
        }
        "unescapeJson" => {
            check_arity(name, args, 0)?;
            Ok(unescape_json(subject))
        }
        "escapeHtml3" | "escapeHtml4" => {
            check_arity(name, args, 0)?;
            Ok(escape_html(subject))
        }
        "unescapeHtml3" | "unescapeHtml4" => {
            check_arity(name, args, 0)?;
            Ok(unescape_html(subject))
        }
        "escapeXml" => {
            check_arity(name, args, 0)?;
            Ok(escape_xml(subject))
        }
        "unescapeXml" => {
            check_arity(name, args, 0)?;
            Ok(unescape_xml(subject))
        }
        "escapeCsv" => {
            check_arity(name, args, 0)?;
            Ok(escape_csv(subject))
        }
        "unescapeCsv" => {
            check_arity(name, args, 0)?;
            Ok(unescape_csv(subject))
        }

        // --- System functions ---
        "hostname" => {
            check_arity(name, args, 0)?;
            Ok(std::fs::read_to_string("/etc/hostname")
                .map(|s| s.trim().to_string())
                .unwrap_or_default())
        }
        "ip" => {
            check_arity(name, args, 0)?;
            Ok(get_local_ip())
        }
        "UUID" => {
            check_arity(name, args, 0)?;
            Ok(Uuid::now_v7().to_string())
        }
        "nextInt" => {
            check_arity(name, args, 0)?;
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            Ok(COUNTER.fetch_add(1, Ordering::Relaxed).to_string())
        }
        "thread" => {
            check_arity(name, args, 0)?;
            Ok(std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_string())
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

fn parse_f64_arg(name: &str, arg: &str) -> Result<f64, FunctionError> {
    arg.parse::<f64>()
        .map_err(|_| FunctionError::ArgumentError {
            name: name.to_string(),
            reason: format!("expected number, got '{}'", arg),
        })
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        (n as i64).to_string()
    } else {
        n.to_string()
    }
}

fn format_radix(mut n: i64, radix: u32) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let negative = n < 0;
    if negative {
        n = -n;
    }
    let mut digits = Vec::new();
    let mut val = n as u64;
    while val > 0 {
        let digit = (val % radix as u64) as u32;
        digits.push(char::from_digit(digit, radix).unwrap());
        val /= radix as u64;
    }
    digits.reverse();
    let s: String = digits.into_iter().collect();
    if negative { format!("-{}", s) } else { s }
}

fn escape_json(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c < '\x20' => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

fn unescape_json(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16)
                        && let Some(ch) = char::from_u32(code)
                    {
                        result.push(ch);
                    }
                }
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn escape_html(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '"' => result.push_str("&quot;"),
            '\'' => result.push_str("&#39;"),
            _ => result.push(c),
        }
    }
    result
}

fn unescape_html(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#x27;", "'")
        .replace("&apos;", "'")
}

fn escape_xml(s: &str) -> String {
    escape_html(s)
}

fn unescape_xml(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn unescape_csv(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        s.to_string()
    }
}

fn get_local_ip() -> String {
    use std::net::UdpSocket;
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect("8.8.8.8:80")?;
            socket.local_addr()
        })
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
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
            "5" // character count, not byte length
        );
    }

    #[test]
    fn test_substring_unicode() {
        // "Héllo" — 'é' is 2 bytes but 1 character. substring must use char indices.
        assert_eq!(
            evaluate_function("Héllo", "substring", &["1".into(), "3".into()]).unwrap(),
            "él"
        );
        assert_eq!(
            evaluate_function("Héllo", "substring", &["0".into(), "1".into()]).unwrap(),
            "H"
        );
        // CJK characters (3 bytes each)
        assert_eq!(
            evaluate_function("\u{4e16}\u{754c}", "substring", &["0".into(), "1".into()]).unwrap(),
            "\u{4e16}"
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

    // --- Encoding / Hash tests ---

    #[test]
    fn test_url_encode() {
        assert_eq!(
            evaluate_function("hello world", "urlEncode", &[]).unwrap(),
            "hello%20world"
        );
        assert_eq!(
            evaluate_function("a=1&b=2", "urlEncode", &[]).unwrap(),
            "a%3D1%26b%3D2"
        );
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(
            evaluate_function("hello%20world", "urlDecode", &[]).unwrap(),
            "hello world"
        );
        assert_eq!(
            evaluate_function("a%3D1%26b%3D2", "urlDecode", &[]).unwrap(),
            "a=1&b=2"
        );
    }

    #[test]
    fn test_url_decode_invalid() {
        // urlencoding is lenient with most inputs; test a truncated percent-encoding
        assert!(evaluate_function("%C3%28", "urlDecode", &[]).is_err());
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(
            evaluate_function("hello", "base64Encode", &[]).unwrap(),
            "aGVsbG8="
        );
    }

    #[test]
    fn test_base64_decode() {
        assert_eq!(
            evaluate_function("aGVsbG8=", "base64Decode", &[]).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_base64_decode_invalid() {
        assert!(evaluate_function("!!!invalid!!!", "base64Decode", &[]).is_err());
    }

    #[test]
    fn test_base64_roundtrip() {
        let original = "Hello, World! 123 Special: @#$%";
        let encoded = evaluate_function(original, "base64Encode", &[]).unwrap();
        let decoded = evaluate_function(&encoded, "base64Decode", &[]).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_hash_md5() {
        assert_eq!(
            evaluate_function("hello", "hash", &["MD5".into()]).unwrap(),
            "5d41402abc4b2a76b9719d911017c592"
        );
    }

    #[test]
    fn test_hash_sha256() {
        assert_eq!(
            evaluate_function("hello", "hash", &["SHA-256".into()]).unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        // Also test lowercase variant
        assert_eq!(
            evaluate_function("hello", "hash", &["sha256".into()]).unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_hash_sha512() {
        let result = evaluate_function("hello", "hash", &["SHA-512".into()]).unwrap();
        assert_eq!(result.len(), 128); // SHA-512 produces 64 bytes = 128 hex chars
        assert!(result.starts_with("9b71d224bd62f3785d96d46ad3ea3d73"));
    }

    #[test]
    fn test_hash_unsupported() {
        assert!(evaluate_function("hello", "hash", &["SHA-1".into()]).is_err());
    }

    // --- Date/Time tests ---

    #[test]
    fn test_now() {
        let result = evaluate_function("", "now", &[]).unwrap();
        let millis: i64 = result.parse().unwrap();
        // Should be a reasonable timestamp (after 2020)
        assert!(millis > 1_577_836_800_000);
    }

    #[test]
    fn test_format_date() {
        // 2024-01-15 12:00:00 UTC in millis
        let millis = "1705320000000";
        let result = evaluate_function(millis, "format", &["%Y-%m-%d".into()]).unwrap();
        assert_eq!(result, "2024-01-15");
    }

    #[test]
    fn test_format_date_invalid() {
        assert!(evaluate_function("not_a_number", "format", &["%Y".into()]).is_err());
    }

    #[test]
    fn test_to_date() {
        let result = evaluate_function(
            "2024-01-15 12:00:00",
            "toDate",
            &["%Y-%m-%d %H:%M:%S".into()],
        )
        .unwrap();
        let millis: i64 = result.parse().unwrap();
        assert_eq!(millis, 1705320000000);
    }

    #[test]
    fn test_to_date_invalid() {
        assert!(evaluate_function("invalid", "toDate", &["%Y-%m-%d".into()]).is_err());
    }

    // --- Math tests ---

    #[test]
    fn test_plus() {
        assert_eq!(
            evaluate_function("10", "plus", &["5".into()]).unwrap(),
            "15"
        );
        assert_eq!(
            evaluate_function("1.5", "plus", &["2.3".into()]).unwrap(),
            "3.8"
        );
    }

    #[test]
    fn test_minus() {
        assert_eq!(
            evaluate_function("10", "minus", &["3".into()]).unwrap(),
            "7"
        );
        assert_eq!(
            evaluate_function("1.5", "minus", &["0.5".into()]).unwrap(),
            "1"
        );
    }

    #[test]
    fn test_multiply() {
        assert_eq!(
            evaluate_function("6", "multiply", &["7".into()]).unwrap(),
            "42"
        );
        assert_eq!(
            evaluate_function("2.5", "multiply", &["4".into()]).unwrap(),
            "10"
        );
    }

    #[test]
    fn test_divide() {
        assert_eq!(
            evaluate_function("10", "divide", &["3".into()]).unwrap(),
            "3.3333333333333335"
        );
        assert_eq!(
            evaluate_function("10", "divide", &["2".into()]).unwrap(),
            "5"
        );
    }

    #[test]
    fn test_divide_by_zero() {
        assert!(evaluate_function("10", "divide", &["0".into()]).is_err());
    }

    #[test]
    fn test_mod() {
        assert_eq!(evaluate_function("10", "mod", &["3".into()]).unwrap(), "1");
        assert_eq!(evaluate_function("10", "mod", &["5".into()]).unwrap(), "0");
    }

    #[test]
    fn test_mod_by_zero() {
        assert!(evaluate_function("10", "mod", &["0".into()]).is_err());
    }

    #[test]
    fn test_to_radix() {
        assert_eq!(
            evaluate_function("255", "toRadix", &["16".into()]).unwrap(),
            "ff"
        );
        assert_eq!(
            evaluate_function("10", "toRadix", &["2".into()]).unwrap(),
            "1010"
        );
        assert_eq!(
            evaluate_function("0", "toRadix", &["16".into()]).unwrap(),
            "0"
        );
    }

    #[test]
    fn test_to_radix_with_padding() {
        assert_eq!(
            evaluate_function("255", "toRadix", &["16".into(), "4".into()]).unwrap(),
            "00ff"
        );
        assert_eq!(
            evaluate_function("10", "toRadix", &["2".into(), "8".into()]).unwrap(),
            "00001010"
        );
    }

    #[test]
    fn test_to_radix_invalid() {
        assert!(evaluate_function("10", "toRadix", &["1".into()]).is_err());
        assert!(evaluate_function("10", "toRadix", &["37".into()]).is_err());
    }

    #[test]
    fn test_math_abs() {
        assert_eq!(
            evaluate_function("-5", "math", &["abs".into()]).unwrap(),
            "5"
        );
        assert_eq!(
            evaluate_function("5", "math", &["abs".into()]).unwrap(),
            "5"
        );
    }

    #[test]
    fn test_math_ceil() {
        assert_eq!(
            evaluate_function("3.2", "math", &["ceil".into()]).unwrap(),
            "4"
        );
        assert_eq!(
            evaluate_function("3.2", "math", &["ceiling".into()]).unwrap(),
            "4"
        );
    }

    #[test]
    fn test_math_floor() {
        assert_eq!(
            evaluate_function("3.8", "math", &["floor".into()]).unwrap(),
            "3"
        );
    }

    #[test]
    fn test_math_round() {
        assert_eq!(
            evaluate_function("3.5", "math", &["round".into()]).unwrap(),
            "4"
        );
        assert_eq!(
            evaluate_function("3.4", "math", &["round".into()]).unwrap(),
            "3"
        );
    }

    #[test]
    fn test_math_unknown_op() {
        assert!(evaluate_function("3", "math", &["sqrt".into()]).is_err());
    }

    #[test]
    fn test_math_not_a_number() {
        assert!(evaluate_function("abc", "plus", &["1".into()]).is_err());
    }

    // --- Additional String function tests ---

    #[test]
    fn test_index_of() {
        assert_eq!(
            evaluate_function("hello world", "indexOf", &["world".into()]).unwrap(),
            "6"
        );
        assert_eq!(
            evaluate_function("hello", "indexOf", &["xyz".into()]).unwrap(),
            "-1"
        );
    }

    #[test]
    fn test_last_index_of() {
        assert_eq!(
            evaluate_function("hello.world.txt", "lastIndexOf", &[".".into()]).unwrap(),
            "11"
        );
        assert_eq!(
            evaluate_function("hello", "lastIndexOf", &["x".into()]).unwrap(),
            "-1"
        );
    }

    #[test]
    fn test_pad_left() {
        assert_eq!(
            evaluate_function("42", "padLeft", &["5".into(), "0".into()]).unwrap(),
            "00042"
        );
        assert_eq!(
            evaluate_function("hello", "padLeft", &["3".into(), "0".into()]).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_pad_right() {
        assert_eq!(
            evaluate_function("42", "padRight", &["5".into(), "0".into()]).unwrap(),
            "42000"
        );
    }

    #[test]
    fn test_find() {
        assert_eq!(
            evaluate_function("abc123def", "find", &["[0-9]+".into()]).unwrap(),
            "123"
        );
        assert_eq!(
            evaluate_function("abcdef", "find", &["[0-9]+".into()]).unwrap(),
            ""
        );
    }

    #[test]
    fn test_matches() {
        assert_eq!(
            evaluate_function("abc123", "matches", &["[0-9]+".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("abcdef", "matches", &["^[0-9]+$".into()]).unwrap(),
            "false"
        );
    }

    #[test]
    fn test_in() {
        assert_eq!(
            evaluate_function("b", "in", &["a".into(), "b".into(), "c".into()]).unwrap(),
            "true"
        );
        assert_eq!(
            evaluate_function("d", "in", &["a".into(), "b".into(), "c".into()]).unwrap(),
            "false"
        );
    }

    #[test]
    fn test_in_no_args() {
        assert!(evaluate_function("x", "in", &[]).is_err());
    }

    #[test]
    fn test_count() {
        assert_eq!(
            evaluate_function("banana", "count", &["an".into()]).unwrap(),
            "2"
        );
        assert_eq!(
            evaluate_function("hello", "count", &["x".into()]).unwrap(),
            "0"
        );
    }

    #[test]
    fn test_join() {
        assert_eq!(
            evaluate_function("hello", "join", &[",".into()]).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_get_delimited_field() {
        assert_eq!(
            evaluate_function("a,b,c", "getDelimitedField", &["2".into()]).unwrap(),
            "b"
        );
        assert_eq!(
            evaluate_function("a|b|c", "getDelimitedField", &["3".into(), "|".into()]).unwrap(),
            "c"
        );
        // Out of bounds returns empty
        assert_eq!(
            evaluate_function("a,b", "getDelimitedField", &["5".into()]).unwrap(),
            ""
        );
        // Index 0 returns empty (1-based)
        assert_eq!(
            evaluate_function("a,b", "getDelimitedField", &["0".into()]).unwrap(),
            ""
        );
    }

    #[test]
    fn test_get_delimited_field_trims() {
        assert_eq!(
            evaluate_function("a, b , c", "getDelimitedField", &["2".into()]).unwrap(),
            "b"
        );
    }

    // --- Escape function tests ---

    #[test]
    fn test_escape_json() {
        assert_eq!(
            evaluate_function("hello \"world\"", "escapeJson", &[]).unwrap(),
            "hello \\\"world\\\""
        );
        assert_eq!(
            evaluate_function("line1\nline2", "escapeJson", &[]).unwrap(),
            "line1\\nline2"
        );
        assert_eq!(
            evaluate_function("tab\there", "escapeJson", &[]).unwrap(),
            "tab\\there"
        );
    }

    #[test]
    fn test_unescape_json() {
        assert_eq!(
            evaluate_function("hello \\\"world\\\"", "unescapeJson", &[]).unwrap(),
            "hello \"world\""
        );
        assert_eq!(
            evaluate_function("line1\\nline2", "unescapeJson", &[]).unwrap(),
            "line1\nline2"
        );
    }

    #[test]
    fn test_json_escape_roundtrip() {
        let original = "He said \"hello\"\nNew line\ttab\\backslash";
        let escaped = evaluate_function(original, "escapeJson", &[]).unwrap();
        let unescaped = evaluate_function(&escaped, "unescapeJson", &[]).unwrap();
        assert_eq!(unescaped, original);
    }

    #[test]
    fn test_escape_html() {
        assert_eq!(
            evaluate_function("<script>alert('xss')</script>", "escapeHtml3", &[]).unwrap(),
            "&lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;"
        );
        assert_eq!(
            evaluate_function("a & b", "escapeHtml4", &[]).unwrap(),
            "a &amp; b"
        );
    }

    #[test]
    fn test_unescape_html() {
        assert_eq!(
            evaluate_function("&lt;b&gt;bold&lt;/b&gt;", "unescapeHtml3", &[]).unwrap(),
            "<b>bold</b>"
        );
        assert_eq!(
            evaluate_function("a &amp; b", "unescapeHtml4", &[]).unwrap(),
            "a & b"
        );
    }

    #[test]
    fn test_escape_xml() {
        assert_eq!(
            evaluate_function("<tag attr=\"val\">", "escapeXml", &[]).unwrap(),
            "&lt;tag attr=&quot;val&quot;&gt;"
        );
    }

    #[test]
    fn test_unescape_xml() {
        assert_eq!(
            evaluate_function("&lt;tag&gt;", "unescapeXml", &[]).unwrap(),
            "<tag>"
        );
        assert_eq!(
            evaluate_function("&apos;quote&apos;", "unescapeXml", &[]).unwrap(),
            "'quote'"
        );
    }

    #[test]
    fn test_escape_csv() {
        assert_eq!(
            evaluate_function("hello", "escapeCsv", &[]).unwrap(),
            "hello"
        );
        assert_eq!(
            evaluate_function("hello,world", "escapeCsv", &[]).unwrap(),
            "\"hello,world\""
        );
        assert_eq!(
            evaluate_function("say \"hi\"", "escapeCsv", &[]).unwrap(),
            "\"say \"\"hi\"\"\""
        );
    }

    #[test]
    fn test_unescape_csv() {
        assert_eq!(
            evaluate_function("hello", "unescapeCsv", &[]).unwrap(),
            "hello"
        );
        assert_eq!(
            evaluate_function("\"hello,world\"", "unescapeCsv", &[]).unwrap(),
            "hello,world"
        );
        assert_eq!(
            evaluate_function("\"say \"\"hi\"\"\"", "unescapeCsv", &[]).unwrap(),
            "say \"hi\""
        );
    }

    // --- System function tests ---

    #[test]
    fn test_hostname() {
        // Just verify it returns something without erroring
        let result = evaluate_function("", "hostname", &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ip() {
        let result = evaluate_function("", "ip", &[]).unwrap();
        // Should be a valid IP address
        assert!(
            result.parse::<std::net::IpAddr>().is_ok(),
            "expected valid IP, got: {}",
            result
        );
    }

    #[test]
    fn test_uuid() {
        let result = evaluate_function("", "UUID", &[]).unwrap();
        assert!(result.len() == 36); // UUID v7 format: 8-4-4-4-12
        assert!(uuid::Uuid::parse_str(&result).is_ok());
    }

    #[test]
    fn test_uuid_unique() {
        let a = evaluate_function("", "UUID", &[]).unwrap();
        let b = evaluate_function("", "UUID", &[]).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn test_next_int() {
        let a = evaluate_function("", "nextInt", &[]).unwrap();
        let b = evaluate_function("", "nextInt", &[]).unwrap();
        let a_val: u64 = a.parse().unwrap();
        let b_val: u64 = b.parse().unwrap();
        assert_eq!(b_val, a_val + 1);
    }

    #[test]
    fn test_thread() {
        let result = evaluate_function("", "thread", &[]).unwrap();
        // In test context, thread name should not be empty
        assert!(!result.is_empty());
    }

    // --- Helper function tests ---

    #[test]
    fn test_format_number_integer() {
        assert_eq!(format_number(42.0), "42");
        assert_eq!(format_number(-3.0), "-3");
        assert_eq!(format_number(0.0), "0");
    }

    #[test]
    fn test_format_number_float() {
        assert_eq!(format_number(3.14), "3.14");
        assert_eq!(format_number(-1.5), "-1.5");
    }

    #[test]
    fn test_format_radix_basic() {
        assert_eq!(format_radix(255, 16), "ff");
        assert_eq!(format_radix(10, 2), "1010");
        assert_eq!(format_radix(0, 10), "0");
        assert_eq!(format_radix(-10, 10), "-10");
    }
}
