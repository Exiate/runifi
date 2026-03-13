//! AST node types for the RuniFi Expression Language.
//!
//! Expressions use NiFi-compatible `${...}` syntax with function chaining.

/// A compiled expression, potentially containing multiple segments.
#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    pub segments: Vec<Segment>,
}

/// A segment is either literal text or a dynamic expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    /// Plain text (no interpolation).
    Literal(String),
    /// A `${...}` expression.
    Dynamic(DynamicExpr),
}

/// A dynamic expression: a subject with zero or more chained function calls.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicExpr {
    pub subject: Subject,
    pub chain: Vec<FunctionCall>,
}

/// The subject of an expression — what the functions operate on.
#[derive(Debug, Clone, PartialEq)]
pub enum Subject {
    /// Reference to a FlowFile attribute: `${filename}`.
    AttributeRef(String),
    /// A string literal: `${'hello'}` or used as a subjectless start.
    StringLiteral(String),
    /// A subjectless function call: `${literal('hello')}`.
    SubjectlessFunction(FunctionCall),
}

/// A function call with name and arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Argument>,
}

/// A function argument — can be a literal string, a number, or a nested expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Argument {
    /// A string literal: `'hello'`.
    StringLiteral(String),
    /// A numeric literal: `42`, `3.14`.
    NumberLiteral(f64),
    /// A nested expression: `${attr2}` used as an argument.
    Expression(DynamicExpr),
}
