/// AST evaluator for the RuniFi Expression Language.
///
/// Walks the AST with FlowFile attributes to produce a final string result.
use std::sync::Arc;

use thiserror::Error;

use super::ast::{Argument, DynamicExpr, Expression, Segment, Subject};
use super::functions::{FunctionError, evaluate_function};

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("function error: {0}")]
    Function(#[from] FunctionError),
}

/// Evaluate a compiled expression against FlowFile attributes.
///
/// Returns the final string result with all expressions resolved.
pub fn evaluate(expr: &Expression, attrs: &[(Arc<str>, Arc<str>)]) -> Result<String, EvalError> {
    let mut result = String::new();
    for segment in &expr.segments {
        match segment {
            Segment::Literal(s) => result.push_str(s),
            Segment::Dynamic(dynamic) => {
                let val = eval_dynamic(dynamic, attrs)?;
                result.push_str(&val);
            }
        }
    }
    Ok(result)
}

/// Evaluate a dynamic expression.
fn eval_dynamic(expr: &DynamicExpr, attrs: &[(Arc<str>, Arc<str>)]) -> Result<String, EvalError> {
    // Resolve the subject.
    let (mut value, is_null) = eval_subject(&expr.subject, attrs)?;

    // Apply chained functions.
    for func in &expr.chain {
        // Special handling for isNull — check the null flag before the first function.
        if func.name == "isNull" {
            value = is_null.to_string();
            continue;
        }

        // If the attribute was null, treat as empty string for all other functions.
        let resolved_args = eval_args(&func.args, attrs)?;
        value = evaluate_function(&value, &func.name, &resolved_args)?;
    }

    Ok(value)
}

/// Evaluate the subject, returning (value, is_null).
fn eval_subject(
    subject: &Subject,
    attrs: &[(Arc<str>, Arc<str>)],
) -> Result<(String, bool), EvalError> {
    match subject {
        Subject::AttributeRef(name) => {
            match find_attr(attrs, name) {
                Some(v) => Ok((v.to_string(), false)),
                None => Ok((String::new(), true)), // Missing attribute → empty, is_null=true
            }
        }
        Subject::StringLiteral(s) => Ok((s.clone(), false)),
        Subject::SubjectlessFunction(func) => {
            let resolved_args = eval_args(&func.args, attrs)?;
            let val = evaluate_function("", &func.name, &resolved_args)?;
            Ok((val, false))
        }
    }
}

/// Evaluate function arguments.
fn eval_args(args: &[Argument], attrs: &[(Arc<str>, Arc<str>)]) -> Result<Vec<String>, EvalError> {
    args.iter().map(|arg| eval_argument(arg, attrs)).collect()
}

/// Evaluate a single argument.
fn eval_argument(arg: &Argument, attrs: &[(Arc<str>, Arc<str>)]) -> Result<String, EvalError> {
    match arg {
        Argument::StringLiteral(s) => Ok(s.clone()),
        Argument::NumberLiteral(n) => {
            if n.fract() == 0.0 && n.is_finite() {
                Ok((*n as i64).to_string())
            } else {
                Ok(n.to_string())
            }
        }
        Argument::Expression(expr) => eval_dynamic(expr, attrs),
    }
}

/// Find an attribute value by name.
fn find_attr<'a>(attrs: &'a [(Arc<str>, Arc<str>)], name: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k.as_ref() == name)
        .map(|(_, v)| v.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::parser::parse;

    fn attrs(pairs: &[(&str, &str)]) -> Vec<(Arc<str>, Arc<str>)> {
        pairs
            .iter()
            .map(|(k, v)| (Arc::from(*k), Arc::from(*v)))
            .collect()
    }

    fn eval(input: &str, a: &[(Arc<str>, Arc<str>)]) -> String {
        let expr = parse(input).unwrap();
        evaluate(&expr, a).unwrap()
    }

    #[test]
    fn test_plain_string() {
        assert_eq!(eval("hello world", &[]), "hello world");
    }

    #[test]
    fn test_attribute_ref() {
        let a = attrs(&[("filename", "test.txt")]);
        assert_eq!(eval("${filename}", &a), "test.txt");
    }

    #[test]
    fn test_missing_attribute() {
        assert_eq!(eval("${missing}", &[]), "");
    }

    #[test]
    fn test_function_chain() {
        let a = attrs(&[("filename", "Test.TXT")]);
        assert_eq!(eval("${filename:toLower()}", &a), "test.txt");
    }

    #[test]
    fn test_substring_after_last() {
        let a = attrs(&[("filename", "archive.tar.gz")]);
        assert_eq!(eval("${filename:substringAfterLast('.')}", &a), "gz");
    }

    #[test]
    fn test_multi_function_chain() {
        let a = attrs(&[("filename", "Test.LOG")]);
        assert_eq!(eval("${filename:toLower():contains('log')}", &a), "true");
    }

    #[test]
    fn test_literal_function() {
        assert_eq!(
            eval("${literal('hello'):append(' world')}", &[]),
            "hello world"
        );
    }

    #[test]
    fn test_if_else() {
        let a = attrs(&[("filename", "test.txt")]);
        assert_eq!(
            eval("${filename:equals('test.txt'):ifElse('yes','no')}", &a),
            "yes"
        );
        assert_eq!(
            eval("${filename:equals('other'):ifElse('yes','no')}", &a),
            "no"
        );
    }

    #[test]
    fn test_nested_expression() {
        let a = attrs(&[("attr1", "hello"), ("attr2", "hello")]);
        assert_eq!(eval("${attr1:equals(${attr2})}", &a), "true");

        let a2 = attrs(&[("attr1", "hello"), ("attr2", "world")]);
        assert_eq!(eval("${attr1:equals(${attr2})}", &a2), "false");
    }

    #[test]
    fn test_mixed_literal_and_expression() {
        let a = attrs(&[("name", "world")]);
        assert_eq!(eval("hello ${name}!", &a), "hello world!");
    }

    #[test]
    fn test_escaped_dollar() {
        assert_eq!(eval("cost: $$5", &[]), "cost: $5");
    }

    #[test]
    fn test_is_null() {
        assert_eq!(eval("${missing:isNull()}", &[]), "true");
        let a = attrs(&[("exists", "val")]);
        assert_eq!(eval("${exists:isNull()}", &a), "false");
    }

    #[test]
    fn test_is_empty_on_missing() {
        assert_eq!(eval("${missing:isEmpty()}", &[]), "true");
    }

    #[test]
    fn test_replace_all_regex() {
        let a = attrs(&[("data", "abc123def456")]);
        assert_eq!(
            eval("${data:replaceAll('[0-9]+', 'NUM')}", &a),
            "abcNUMdefNUM"
        );
    }

    #[test]
    fn test_not_and_or() {
        let a = attrs(&[("flag", "true")]);
        assert_eq!(eval("${flag:not()}", &a), "false");
        assert_eq!(eval("${flag:and('true')}", &a), "true");
        assert_eq!(eval("${flag:or('false')}", &a), "true");
    }

    #[test]
    fn test_substring_numeric_args() {
        let a = attrs(&[("data", "hello world")]);
        assert_eq!(eval("${data:substring(0, 5)}", &a), "hello");
        assert_eq!(eval("${data:substring(6)}", &a), "world");
    }

    #[test]
    fn test_to_number() {
        let a = attrs(&[("num", "42")]);
        assert_eq!(eval("${num:toNumber()}", &a), "42");
    }

    #[test]
    fn test_multiple_expressions() {
        let a = attrs(&[("first", "Hello"), ("last", "World")]);
        assert_eq!(eval("${first} ${last}", &a), "Hello World");
    }

    #[test]
    fn test_no_expression_passthrough() {
        assert_eq!(eval("no expressions here", &[]), "no expressions here");
    }
}
