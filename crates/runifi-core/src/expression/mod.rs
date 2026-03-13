/// RuniFi Expression Language — NiFi-compatible `${...}` syntax.
///
/// # Usage
///
/// ```
/// use std::sync::Arc;
/// use runifi_core::expression::Expression;
///
/// // Compile once at flow load time.
/// let expr = Expression::compile("${filename:toLower():substringAfterLast('.')}").unwrap();
///
/// // Evaluate per FlowFile.
/// let attrs: Vec<(Arc<str>, Arc<str>)> = vec![
///     (Arc::from("filename"), Arc::from("Report.PDF")),
/// ];
/// let result = expr.evaluate(&attrs).unwrap();
/// assert_eq!(result, "pdf");
/// ```
pub mod ast;
pub mod evaluator;
pub mod functions;
pub mod lexer;
pub mod parser;

use std::sync::Arc;

use thiserror::Error;

/// Errors that can occur during expression compilation or evaluation.
#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("compile error: {0}")]
    Compile(#[from] parser::ParseError),
    #[error("eval error: {0}")]
    Eval(#[from] evaluator::EvalError),
}

/// A compiled expression that can be evaluated against FlowFile attributes.
///
/// Designed for compile-once, evaluate-many usage. Properties without `${`
/// are stored as plain literals with zero evaluation overhead.
#[derive(Debug, Clone)]
pub struct Expression {
    /// The compiled AST.
    compiled: ast::Expression,
    /// Fast-path: if the entire expression is a single literal, store it here.
    /// Avoids AST traversal for non-dynamic properties.
    literal_value: Option<String>,
}

impl Expression {
    /// Compile an expression string into a reusable `Expression`.
    ///
    /// Returns an error with position information if the expression has syntax errors.
    pub fn compile(input: &str) -> Result<Self, ExpressionError> {
        // Short-circuit: if there's no `${`, it's a plain literal.
        if !input.contains("${") {
            // Still handle `$$` → `$` escape.
            let literal = input.replace("$$", "$");
            return Ok(Self {
                compiled: ast::Expression {
                    segments: vec![ast::Segment::Literal(literal.clone())],
                },
                literal_value: Some(literal),
            });
        }

        let compiled = parser::parse(input)?;

        // Check if parsing produced a single literal (e.g., only `$$` escapes).
        let literal_value = if compiled.segments.len() == 1 {
            match &compiled.segments[0] {
                ast::Segment::Literal(s) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        Ok(Self {
            compiled,
            literal_value,
        })
    }

    /// Evaluate this expression against FlowFile attributes.
    ///
    /// For plain literal expressions, returns immediately with zero overhead.
    pub fn evaluate(&self, attrs: &[(Arc<str>, Arc<str>)]) -> Result<String, ExpressionError> {
        // Fast path: plain literal.
        if let Some(ref lit) = self.literal_value {
            return Ok(lit.clone());
        }

        Ok(evaluator::evaluate(&self.compiled, attrs)?)
    }

    /// Returns `true` if this expression is a plain literal with no dynamic parts.
    pub fn is_literal(&self) -> bool {
        self.literal_value.is_some()
    }
}

/// Resolve a property value through the expression language.
///
/// This is a convenience function for the engine to resolve a single property.
/// For properties that need repeated evaluation, use `Expression::compile` + `evaluate`.
pub fn resolve_property(
    value: &str,
    attrs: &[(Arc<str>, Arc<str>)],
) -> Result<String, ExpressionError> {
    // Fast path: no expression syntax.
    if !value.contains("${") && !value.contains("$$") {
        return Ok(value.to_string());
    }
    let expr = Expression::compile(value)?;
    expr.evaluate(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> Vec<(Arc<str>, Arc<str>)> {
        pairs
            .iter()
            .map(|(k, v)| (Arc::from(*k), Arc::from(*v)))
            .collect()
    }

    // --- Short-circuit / fast path tests ---

    #[test]
    fn test_literal_is_literal() {
        let expr = Expression::compile("plain text").unwrap();
        assert!(expr.is_literal());
        assert_eq!(expr.evaluate(&[]).unwrap(), "plain text");
    }

    #[test]
    fn test_dynamic_is_not_literal() {
        let expr = Expression::compile("${filename}").unwrap();
        assert!(!expr.is_literal());
    }

    #[test]
    fn test_escaped_dollar_literal() {
        let expr = Expression::compile("cost $$5").unwrap();
        assert!(expr.is_literal());
        assert_eq!(expr.evaluate(&[]).unwrap(), "cost $5");
    }

    // --- Full integration tests ---

    #[test]
    fn test_resolve_property_plain() {
        assert_eq!(resolve_property("hello", &[]).unwrap(), "hello");
    }

    #[test]
    fn test_resolve_property_expression() {
        let a = attrs(&[("filename", "test.txt")]);
        assert_eq!(resolve_property("${filename}", &a).unwrap(), "test.txt");
    }

    #[test]
    fn test_full_chain() {
        let a = attrs(&[("filename", "Report.PDF")]);
        let expr = Expression::compile("${filename:toLower():substringAfterLast('.')}").unwrap();
        assert_eq!(expr.evaluate(&a).unwrap(), "pdf");
    }

    #[test]
    fn test_compile_error() {
        assert!(Expression::compile("${").is_err());
    }

    #[test]
    fn test_eval_error_unknown_function() {
        let expr = Expression::compile("${x:unknownFunc()}").unwrap();
        assert!(expr.evaluate(&[]).is_err());
    }

    #[test]
    fn test_multiple_expressions_in_string() {
        let a = attrs(&[("dir", "/tmp"), ("filename", "data.csv")]);
        assert_eq!(
            resolve_property("${dir}/${filename}", &a).unwrap(),
            "/tmp/data.csv"
        );
    }

    #[test]
    fn test_complex_chain() {
        let a = attrs(&[("filename", "My Document.PDF")]);
        let expr =
            Expression::compile("${filename:replace(' ', '_'):toLower():substringAfterLast('.')}")
                .unwrap();
        assert_eq!(expr.evaluate(&a).unwrap(), "pdf");
    }

    #[test]
    fn test_nested_expression_in_function() {
        let a = attrs(&[("ext", "pdf"), ("filename", "report.pdf")]);
        let expr = Expression::compile("${filename:endsWith(${ext})}").unwrap();
        assert_eq!(expr.evaluate(&a).unwrap(), "true");
    }

    #[test]
    fn test_conditional_expression() {
        let a = attrs(&[("filename", "data.log")]);
        let expr = Expression::compile(
            "${filename:toLower():contains('log'):ifElse('LOG_FILE', 'OTHER')}",
        )
        .unwrap();
        assert_eq!(expr.evaluate(&a).unwrap(), "LOG_FILE");
    }

    #[test]
    fn test_empty_input() {
        let expr = Expression::compile("").unwrap();
        assert!(expr.is_literal());
        assert_eq!(expr.evaluate(&[]).unwrap(), "");
    }

    #[test]
    fn test_only_dollar_signs() {
        let expr = Expression::compile("$$$$").unwrap();
        assert_eq!(expr.evaluate(&[]).unwrap(), "$$");
    }

    #[test]
    fn test_attribute_with_dots() {
        let a = attrs(&[("mime.type", "text/plain")]);
        assert_eq!(resolve_property("${mime.type}", &a).unwrap(), "text/plain");
    }

    #[test]
    fn test_attribute_with_hyphens() {
        let a = attrs(&[("content-type", "application/json")]);
        assert_eq!(
            resolve_property("${content-type}", &a).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_replace_all_with_regex() {
        let a = attrs(&[("data", "Hello World 123")]);
        assert_eq!(
            resolve_property(r"${data:replaceAll('\d+', 'NUM')}", &a).unwrap(),
            "Hello World NUM"
        );
    }
}
