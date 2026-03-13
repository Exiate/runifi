/// Parser for the RuniFi Expression Language.
///
/// Converts a token stream from the lexer into an AST.
use thiserror::Error;

use super::ast::{Argument, DynamicExpr, Expression, FunctionCall, Segment, Subject};
use super::lexer::{LexError, RawSegment, Token, tokenize};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("lex error: {0}")]
    Lex(#[from] LexError),
    #[error("unexpected token {token:?} at position {pos}")]
    UnexpectedToken { token: Token, pos: usize },
    #[error("unexpected end of expression")]
    UnexpectedEnd,
    #[error("expected token {expected:?}, got {got:?}")]
    Expected { expected: &'static str, got: Token },
}

/// Parse a full expression string into an AST.
pub fn parse(input: &str) -> Result<Expression, ParseError> {
    let raw_segments = tokenize(input)?;
    let mut segments = Vec::new();

    for raw in raw_segments {
        match raw {
            RawSegment::Literal(s) => {
                segments.push(Segment::Literal(s));
            }
            RawSegment::Expression(tokens) => {
                let mut pos = 0;
                let expr = parse_dynamic_expr(&tokens, &mut pos)?;
                segments.push(Segment::Dynamic(expr));
            }
        }
    }

    Ok(Expression { segments })
}

/// Parse a dynamic expression from tokens. Expects tokens to end with ExprClose.
fn parse_dynamic_expr(tokens: &[Token], pos: &mut usize) -> Result<DynamicExpr, ParseError> {
    let subject = parse_subject(tokens, pos)?;
    let mut chain = Vec::new();

    // Parse chained function calls: `:funcName(args)`
    while *pos < tokens.len() {
        match &tokens[*pos] {
            Token::Colon => {
                *pos += 1; // skip `:`
                let func = parse_function_call(tokens, pos)?;
                chain.push(func);
            }
            Token::ExprClose => {
                *pos += 1;
                break;
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "':' or '}'",
                    got: other.clone(),
                });
            }
        }
    }

    Ok(DynamicExpr { subject, chain })
}

/// Parse the subject of an expression.
fn parse_subject(tokens: &[Token], pos: &mut usize) -> Result<Subject, ParseError> {
    if *pos >= tokens.len() {
        return Err(ParseError::UnexpectedEnd);
    }

    match &tokens[*pos] {
        Token::Ident(name) => {
            let name = name.clone();
            *pos += 1;

            // Check if this is a subjectless function call (identifier followed by `(`).
            if *pos < tokens.len() && tokens[*pos] == Token::LParen {
                let args = parse_args(tokens, pos)?;
                Ok(Subject::SubjectlessFunction(FunctionCall { name, args }))
            } else {
                Ok(Subject::AttributeRef(name))
            }
        }
        Token::StringLit(s) => {
            let s = s.clone();
            *pos += 1;
            Ok(Subject::StringLiteral(s))
        }
        other => Err(ParseError::Expected {
            expected: "identifier or string literal",
            got: other.clone(),
        }),
    }
}

/// Parse a function call: `funcName(args)` or `funcName`.
fn parse_function_call(tokens: &[Token], pos: &mut usize) -> Result<FunctionCall, ParseError> {
    if *pos >= tokens.len() {
        return Err(ParseError::UnexpectedEnd);
    }

    let name = match &tokens[*pos] {
        Token::Ident(n) => {
            let n = n.clone();
            *pos += 1;
            n
        }
        other => {
            return Err(ParseError::Expected {
                expected: "function name",
                got: other.clone(),
            });
        }
    };

    // Check for argument list.
    let args = if *pos < tokens.len() && tokens[*pos] == Token::LParen {
        parse_args(tokens, pos)?
    } else {
        // No-arg function without parens (e.g., `:isEmpty`).
        Vec::new()
    };

    Ok(FunctionCall { name, args })
}

/// Parse argument list: `(arg1, arg2, ...)`.
fn parse_args(tokens: &[Token], pos: &mut usize) -> Result<Vec<Argument>, ParseError> {
    assert_eq!(tokens[*pos], Token::LParen);
    *pos += 1; // skip `(`

    let mut args = Vec::new();

    // Handle empty arg list.
    if *pos < tokens.len() && tokens[*pos] == Token::RParen {
        *pos += 1;
        return Ok(args);
    }

    loop {
        if *pos >= tokens.len() {
            return Err(ParseError::UnexpectedEnd);
        }

        let arg = parse_argument(tokens, pos)?;
        args.push(arg);

        if *pos >= tokens.len() {
            return Err(ParseError::UnexpectedEnd);
        }

        match &tokens[*pos] {
            Token::Comma => {
                *pos += 1; // skip `,`
            }
            Token::RParen => {
                *pos += 1; // skip `)`
                break;
            }
            other => {
                return Err(ParseError::Expected {
                    expected: "',' or ')'",
                    got: other.clone(),
                });
            }
        }
    }

    Ok(args)
}

/// Parse a single argument.
fn parse_argument(tokens: &[Token], pos: &mut usize) -> Result<Argument, ParseError> {
    if *pos >= tokens.len() {
        return Err(ParseError::UnexpectedEnd);
    }

    match &tokens[*pos] {
        Token::StringLit(s) => {
            let s = s.clone();
            *pos += 1;
            Ok(Argument::StringLiteral(s))
        }
        Token::NumberLit(n) => {
            let n = *n;
            *pos += 1;
            Ok(Argument::NumberLiteral(n))
        }
        Token::ExprOpen => {
            // Nested expression: `${...}`.
            *pos += 1; // skip ExprOpen
            let expr = parse_dynamic_expr(tokens, pos)?;
            Ok(Argument::Expression(expr))
        }
        other => Err(ParseError::Expected {
            expected: "string, number, or expression",
            got: other.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_literal() {
        let expr = parse("hello world").unwrap();
        assert_eq!(expr.segments, vec![Segment::Literal("hello world".into())]);
    }

    #[test]
    fn test_parse_attribute_ref() {
        let expr = parse("${filename}").unwrap();
        assert_eq!(
            expr.segments,
            vec![Segment::Dynamic(DynamicExpr {
                subject: Subject::AttributeRef("filename".into()),
                chain: vec![],
            })]
        );
    }

    #[test]
    fn test_parse_function_chain() {
        let expr = parse("${filename:toLower():contains('log')}").unwrap();
        let seg = &expr.segments[0];
        match seg {
            Segment::Dynamic(d) => {
                assert_eq!(d.subject, Subject::AttributeRef("filename".into()));
                assert_eq!(d.chain.len(), 2);
                assert_eq!(d.chain[0].name, "toLower");
                assert_eq!(d.chain[0].args, vec![]);
                assert_eq!(d.chain[1].name, "contains");
                assert_eq!(d.chain[1].args, vec![Argument::StringLiteral("log".into())]);
            }
            _ => panic!("expected dynamic segment"),
        }
    }

    #[test]
    fn test_parse_subjectless_function() {
        let expr = parse("${literal('hello'):append(' world')}").unwrap();
        let seg = &expr.segments[0];
        match seg {
            Segment::Dynamic(d) => {
                assert_eq!(
                    d.subject,
                    Subject::SubjectlessFunction(FunctionCall {
                        name: "literal".into(),
                        args: vec![Argument::StringLiteral("hello".into())],
                    })
                );
                assert_eq!(d.chain.len(), 1);
                assert_eq!(d.chain[0].name, "append");
            }
            _ => panic!("expected dynamic segment"),
        }
    }

    #[test]
    fn test_parse_nested_expression() {
        let expr = parse("${attr1:equals(${attr2})}").unwrap();
        let seg = &expr.segments[0];
        match seg {
            Segment::Dynamic(d) => {
                assert_eq!(d.subject, Subject::AttributeRef("attr1".into()));
                assert_eq!(d.chain.len(), 1);
                assert_eq!(d.chain[0].name, "equals");
                assert_eq!(
                    d.chain[0].args,
                    vec![Argument::Expression(DynamicExpr {
                        subject: Subject::AttributeRef("attr2".into()),
                        chain: vec![],
                    })]
                );
            }
            _ => panic!("expected dynamic segment"),
        }
    }

    #[test]
    fn test_parse_mixed() {
        let expr = parse("prefix-${filename:toLower()}-suffix").unwrap();
        assert_eq!(expr.segments.len(), 3);
        assert!(matches!(&expr.segments[0], Segment::Literal(s) if s == "prefix-"));
        assert!(matches!(&expr.segments[1], Segment::Dynamic(_)));
        assert!(matches!(&expr.segments[2], Segment::Literal(s) if s == "-suffix"));
    }

    #[test]
    fn test_parse_if_else() {
        let expr = parse("${filename:equals('test.txt'):ifElse('yes','no')}").unwrap();
        let seg = &expr.segments[0];
        match seg {
            Segment::Dynamic(d) => {
                assert_eq!(d.chain.len(), 2);
                assert_eq!(d.chain[1].name, "ifElse");
                assert_eq!(d.chain[1].args.len(), 2);
            }
            _ => panic!("expected dynamic segment"),
        }
    }

    #[test]
    fn test_parse_error_missing_close() {
        assert!(parse("${filename").is_err());
    }

    #[test]
    fn test_parse_dollar_escape() {
        let expr = parse("$$").unwrap();
        assert_eq!(expr.segments, vec![Segment::Literal("$".into())]);
    }

    #[test]
    fn test_parse_substring_two_args() {
        let expr = parse("${filename:substring(0, 5)}").unwrap();
        let seg = &expr.segments[0];
        match seg {
            Segment::Dynamic(d) => {
                assert_eq!(d.chain[0].name, "substring");
                assert_eq!(d.chain[0].args.len(), 2);
            }
            _ => panic!("expected dynamic segment"),
        }
    }
}
