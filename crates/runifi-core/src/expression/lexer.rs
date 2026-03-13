/// Tokenizer for the RuniFi Expression Language.
///
/// Splits input into literal text and `${...}` expression regions,
/// then tokenizes expression contents.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LexError {
    #[error("unexpected end of input at position {pos}")]
    UnexpectedEof { pos: usize },
    #[error("unterminated string literal at position {pos}")]
    UnterminatedString { pos: usize },
    #[error("unexpected character '{ch}' at position {pos}")]
    UnexpectedChar { ch: char, pos: usize },
    #[error("unterminated expression at position {pos}")]
    UnterminatedExpression { pos: usize },
}

/// Token types within a `${...}` expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// An identifier (attribute name or function name).
    Ident(String),
    /// A string literal (contents between single quotes).
    StringLit(String),
    /// A numeric literal.
    NumberLit(f64),
    /// `:` — function chain separator.
    Colon,
    /// `(` — function argument list open.
    LParen,
    /// `)` — function argument list close.
    RParen,
    /// `,` — argument separator.
    Comma,
    /// `${` — nested expression open.
    ExprOpen,
    /// `}` — expression close.
    ExprClose,
}

/// A top-level segment: either literal text or a token stream from `${...}`.
#[derive(Debug, Clone, PartialEq)]
pub enum RawSegment {
    Literal(String),
    Expression(Vec<Token>),
}

/// Tokenize a full expression string into raw segments.
///
/// Handles `$$` escape for literal `$` and splits on `${...}`.
pub fn tokenize(input: &str) -> Result<Vec<RawSegment>, LexError> {
    let mut segments = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut literal = String::new();

    while i < len {
        if chars[i] == '$' {
            if i + 1 < len && chars[i + 1] == '$' {
                // Escaped dollar sign.
                literal.push('$');
                i += 2;
            } else if i + 1 < len && chars[i + 1] == '{' {
                // Start of expression.
                if !literal.is_empty() {
                    segments.push(RawSegment::Literal(std::mem::take(&mut literal)));
                }
                i += 2; // skip `${`
                let tokens = tokenize_expression(&chars, &mut i)?;
                segments.push(RawSegment::Expression(tokens));
            } else {
                literal.push('$');
                i += 1;
            }
        } else {
            literal.push(chars[i]);
            i += 1;
        }
    }

    if !literal.is_empty() {
        segments.push(RawSegment::Literal(literal));
    }

    Ok(segments)
}

/// Tokenize the contents of a `${...}` block. `pos` starts right after `${`
/// and is advanced past the closing `}`.
fn tokenize_expression(chars: &[char], pos: &mut usize) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    let len = chars.len();

    while *pos < len {
        skip_whitespace(chars, pos);
        if *pos >= len {
            return Err(LexError::UnterminatedExpression { pos: *pos });
        }

        match chars[*pos] {
            '}' => {
                tokens.push(Token::ExprClose);
                *pos += 1;
                return Ok(tokens);
            }
            ':' => {
                tokens.push(Token::Colon);
                *pos += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                *pos += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                *pos += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                *pos += 1;
            }
            '\'' => {
                let s = read_string_literal(chars, pos)?;
                tokens.push(Token::StringLit(s));
            }
            '$' => {
                // Nested expression: `${...}`
                if *pos + 1 < len && chars[*pos + 1] == '{' {
                    tokens.push(Token::ExprOpen);
                    *pos += 2;
                    // Recursively tokenize — but we flatten into the same token stream
                    // with ExprOpen/ExprClose markers.
                    let inner = tokenize_expression(chars, pos)?;
                    tokens.extend(inner);
                } else {
                    return Err(LexError::UnexpectedChar { ch: '$', pos: *pos });
                }
            }
            c if c.is_ascii_digit()
                || (c == '-' && *pos + 1 < len && chars[*pos + 1].is_ascii_digit()) =>
            {
                let n = read_number(chars, pos)?;
                tokens.push(Token::NumberLit(n));
            }
            c if is_ident_start(c) => {
                let ident = read_ident(chars, pos);
                tokens.push(Token::Ident(ident));
            }
            c => {
                return Err(LexError::UnexpectedChar { ch: c, pos: *pos });
            }
        }
    }

    Err(LexError::UnterminatedExpression { pos: *pos })
}

fn skip_whitespace(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'
}

fn read_ident(chars: &[char], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < chars.len() && is_ident_char(chars[*pos]) {
        *pos += 1;
    }
    chars[start..*pos].iter().collect()
}

fn read_string_literal(chars: &[char], pos: &mut usize) -> Result<String, LexError> {
    let start = *pos;
    *pos += 1; // skip opening quote
    let mut s = String::new();
    while *pos < chars.len() {
        if chars[*pos] == '\'' {
            if *pos + 1 < chars.len() && chars[*pos + 1] == '\'' {
                // Escaped single quote.
                s.push('\'');
                *pos += 2;
            } else {
                *pos += 1; // skip closing quote
                return Ok(s);
            }
        } else {
            s.push(chars[*pos]);
            *pos += 1;
        }
    }
    Err(LexError::UnterminatedString { pos: start })
}

fn read_number(chars: &[char], pos: &mut usize) -> Result<f64, LexError> {
    let start = *pos;
    if chars[*pos] == '-' {
        *pos += 1;
    }
    while *pos < chars.len() && chars[*pos].is_ascii_digit() {
        *pos += 1;
    }
    if *pos < chars.len() && chars[*pos] == '.' {
        *pos += 1;
        while *pos < chars.len() && chars[*pos].is_ascii_digit() {
            *pos += 1;
        }
    }
    let num_str: String = chars[start..*pos].iter().collect();
    num_str
        .parse::<f64>()
        .map_err(|_| LexError::UnexpectedChar {
            ch: chars[start],
            pos: start,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_literal() {
        let result = tokenize("hello world").unwrap();
        assert_eq!(result, vec![RawSegment::Literal("hello world".into())]);
    }

    #[test]
    fn test_escaped_dollar() {
        let result = tokenize("price is $$5").unwrap();
        assert_eq!(result, vec![RawSegment::Literal("price is $5".into())]);
    }

    #[test]
    fn test_simple_expression() {
        let result = tokenize("${filename}").unwrap();
        assert_eq!(
            result,
            vec![RawSegment::Expression(vec![
                Token::Ident("filename".into()),
                Token::ExprClose,
            ])]
        );
    }

    #[test]
    fn test_expression_with_function() {
        let result = tokenize("${filename:toLower()}").unwrap();
        assert_eq!(
            result,
            vec![RawSegment::Expression(vec![
                Token::Ident("filename".into()),
                Token::Colon,
                Token::Ident("toLower".into()),
                Token::LParen,
                Token::RParen,
                Token::ExprClose,
            ])]
        );
    }

    #[test]
    fn test_mixed_literal_and_expression() {
        let result = tokenize("file: ${filename}.txt").unwrap();
        assert_eq!(
            result,
            vec![
                RawSegment::Literal("file: ".into()),
                RawSegment::Expression(vec![Token::Ident("filename".into()), Token::ExprClose,]),
                RawSegment::Literal(".txt".into()),
            ]
        );
    }

    #[test]
    fn test_string_literal_in_function() {
        let result = tokenize("${filename:substringAfterLast('.')}").unwrap();
        assert_eq!(
            result,
            vec![RawSegment::Expression(vec![
                Token::Ident("filename".into()),
                Token::Colon,
                Token::Ident("substringAfterLast".into()),
                Token::LParen,
                Token::StringLit(".".into()),
                Token::RParen,
                Token::ExprClose,
            ])]
        );
    }

    #[test]
    fn test_nested_expression() {
        let result = tokenize("${attr1:equals(${attr2})}").unwrap();
        assert_eq!(
            result,
            vec![RawSegment::Expression(vec![
                Token::Ident("attr1".into()),
                Token::Colon,
                Token::Ident("equals".into()),
                Token::LParen,
                Token::ExprOpen,
                Token::Ident("attr2".into()),
                Token::ExprClose,
                Token::RParen,
                Token::ExprClose,
            ])]
        );
    }

    #[test]
    fn test_unterminated_expression() {
        assert!(tokenize("${filename").is_err());
    }

    #[test]
    fn test_unterminated_string() {
        assert!(tokenize("${filename:equals('test)}").is_err());
    }

    #[test]
    fn test_number_literal() {
        let result = tokenize("${filename:substring(0, 5)}").unwrap();
        assert!(
            result[0]
                == RawSegment::Expression(vec![
                    Token::Ident("filename".into()),
                    Token::Colon,
                    Token::Ident("substring".into()),
                    Token::LParen,
                    Token::NumberLit(0.0),
                    Token::Comma,
                    Token::NumberLit(5.0),
                    Token::RParen,
                    Token::ExprClose,
                ])
        );
    }
}
