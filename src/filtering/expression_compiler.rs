use anyhow::{Result, anyhow};
use geozero::mvt;
use regex::Regex;
use serde_json::Value;
use std::{collections::HashSet, fmt::Display};

use super::data::Operator;

/// A compiled expression that can be efficiently evaluated
#[derive(Debug, Clone)]
pub enum CompiledExpression {
    // Comparison operations
    Equal(Box<CompiledExpression>, Box<CompiledExpression>),
    NotEqual(Box<CompiledExpression>, Box<CompiledExpression>),
    LessThan(Box<CompiledExpression>, Box<CompiledExpression>),
    GreaterThan(Box<CompiledExpression>, Box<CompiledExpression>),
    LessThanOrEqual(Box<CompiledExpression>, Box<CompiledExpression>),
    GreaterThanOrEqual(Box<CompiledExpression>, Box<CompiledExpression>),

    // Logical operations
    Any(Vec<CompiledExpression>),
    All(Vec<CompiledExpression>),
    None(Vec<CompiledExpression>),
    Not(Box<CompiledExpression>),

    // Membership operations
    In(Box<CompiledExpression>, HashSet<ExpressionValue>),

    // String operations
    StartsWith(Box<CompiledExpression>, String),
    EndsWith(Box<CompiledExpression>, String),
    RegexMatch(Box<CompiledExpression>, Regex),
    RegexCapture(Box<CompiledExpression>, Regex, usize),

    // Value operations
    Boolean(Box<CompiledExpression>),
    Literal(ExpressionValue),

    // Context operations
    Tag(String), // Get feature property by name
    Key,         // Current tag key being processed
    Type,        // Feature geometry type
}

/// Runtime values that expressions can evaluate to
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExpressionValue {
    String(String),
    Number(i64),
    Float(String), // Store as string to maintain precision and enable hashing
    Boolean(bool),
    Null,
    Array(Vec<ExpressionValue>),
}

impl ExpressionValue {
    /// Convert from serde_json::Value
    pub fn from_json_value(value: &Value) -> Self {
        match value {
            Value::String(s) => ExpressionValue::String(s.clone()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ExpressionValue::Number(i)
                } else {
                    ExpressionValue::Float(n.to_string())
                }
            }
            Value::Bool(b) => ExpressionValue::Boolean(*b),
            Value::Null => ExpressionValue::Null,
            Value::Array(arr) => {
                ExpressionValue::Array(arr.iter().map(ExpressionValue::from_json_value).collect())
            }
            _ => ExpressionValue::String(value.to_string()),
        }
    }

    /// Convert from mvt::tile::Value
    pub fn from_mvt_value(value: &mvt::tile::Value) -> Self {
        if let Some(s) = &value.string_value {
            ExpressionValue::String(s.to_string())
        } else if let Some(i) = value.int_value {
            ExpressionValue::Number(i)
        } else if let Some(u) = value.uint_value {
            ExpressionValue::Number(u as i64)
        } else if let Some(i) = value.sint_value {
            ExpressionValue::Number(i)
        } else if let Some(f) = value.float_value {
            ExpressionValue::Float(f.to_string())
        } else if let Some(d) = value.double_value {
            ExpressionValue::Float(d.to_string())
        } else if let Some(b) = value.bool_value {
            ExpressionValue::Boolean(b)
        } else {
            ExpressionValue::Null
        }
    }

    /// Convert to boolean for logical operations
    pub fn to_bool(&self) -> bool {
        match self {
            ExpressionValue::Boolean(b) => *b,
            ExpressionValue::String(s) => !s.is_empty(),
            ExpressionValue::Number(n) => *n != 0,
            ExpressionValue::Float(f) => f != "0" && f != "0.0",
            ExpressionValue::Null => false,
            ExpressionValue::Array(arr) => !arr.is_empty(),
        }
    }
}

impl Display for ExpressionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionValue::String(s) => write!(f, "{}", s),
            ExpressionValue::Number(n) => write!(f, "{}", n),
            ExpressionValue::Float(s) => write!(f, "{}", s),
            ExpressionValue::Boolean(b) => write!(f, "{}", b),
            ExpressionValue::Null => write!(f, "null"),
            ExpressionValue::Array(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                write!(f, "[{}]", elements.join(", "))
            }
        }
    }
}

/// Compiles JSON expressions into optimized executable forms
pub struct ExpressionCompiler;

impl ExpressionCompiler {
    /// Compile a JSON expression into a CompiledExpression
    pub fn compile(expr: &Value) -> Result<CompiledExpression> {
        match expr {
            Value::Array(arr) => {
                if arr.is_empty() {
                    return Err(anyhow!("Expression array cannot be empty"));
                }

                let op_str = arr[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("First element must be operator string"))?;

                let operator = Operator::from_str(op_str)?;
                let args = &arr[1..];

                Self::compile_operator(operator, args)
            }
            // Handle all literal values
            Value::String(s) => Ok(CompiledExpression::Literal(ExpressionValue::String(
                s.clone(),
            ))),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(CompiledExpression::Literal(ExpressionValue::Number(i)))
                } else {
                    Ok(CompiledExpression::Literal(ExpressionValue::Float(
                        n.to_string(),
                    )))
                }
            }
            Value::Bool(b) => Ok(CompiledExpression::Literal(ExpressionValue::Boolean(*b))),
            Value::Null => Ok(CompiledExpression::Literal(ExpressionValue::Null)),
            Value::Object(_) => Err(anyhow!("Object expressions are not supported")),
        }
    }

    fn compile_operator(operator: Operator, args: &[Value]) -> Result<CompiledExpression> {
        match operator {
            // Comparison operations
            Operator::Equal => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::Equal(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }
            Operator::NotEqual => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::NotEqual(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }
            Operator::LessThan => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::LessThan(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }
            Operator::GreaterThan => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::GreaterThan(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }
            Operator::LessThanOrEqual => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::LessThanOrEqual(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }
            Operator::GreaterThanOrEqual => {
                Self::ensure_arg_count(args, 2)?;
                Ok(CompiledExpression::GreaterThanOrEqual(
                    Box::new(Self::compile(&args[0])?),
                    Box::new(Self::compile(&args[1])?),
                ))
            }

            // Logical operations
            Operator::Any => {
                let compiled_args: Result<Vec<_>> = args.iter().map(Self::compile).collect();
                Ok(CompiledExpression::Any(compiled_args?))
            }
            Operator::All => {
                let compiled_args: Result<Vec<_>> = args.iter().map(Self::compile).collect();
                Ok(CompiledExpression::All(compiled_args?))
            }
            Operator::None => {
                let compiled_args: Result<Vec<_>> = args.iter().map(Self::compile).collect();
                Ok(CompiledExpression::None(compiled_args?))
            }
            Operator::Not => {
                Self::ensure_arg_count(args, 1)?;
                Ok(CompiledExpression::Not(Box::new(Self::compile(&args[0])?)))
            }

            // Membership operations
            Operator::In => {
                Self::ensure_arg_count(args, 2)?;
                let expr = Self::compile(&args[0])?;
                let values = Self::compile(&args[1])?;
                let values =
                    if let CompiledExpression::Literal(ExpressionValue::Array(arr)) = values {
                        arr.into_iter().collect()
                    } else {
                        return Err(anyhow!("In operator requires an array of values"));
                    };
                Ok(CompiledExpression::In(Box::new(expr), values))
            }

            // String operations
            Operator::StartsWith => {
                Self::ensure_arg_count(args, 2)?;
                let expr = Self::compile(&args[0])?;
                let prefix = args[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("StartsWith requires string argument"))?;
                Ok(CompiledExpression::StartsWith(
                    Box::new(expr),
                    prefix.to_string(),
                ))
            }
            Operator::EndsWith => {
                Self::ensure_arg_count(args, 2)?;
                let expr = Self::compile(&args[0])?;
                let suffix = args[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("EndsWith requires string argument"))?;
                Ok(CompiledExpression::EndsWith(
                    Box::new(expr),
                    suffix.to_string(),
                ))
            }
            Operator::RegexMatch => {
                Self::ensure_arg_count(args, 2)?;
                let expr = Self::compile(&args[0])?;
                let pattern = args[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("RegexMatch requires string pattern"))?;
                let regex = Regex::new(pattern)
                    .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;
                Ok(CompiledExpression::RegexMatch(Box::new(expr), regex))
            }
            Operator::RegexCapture => {
                Self::ensure_min_arg_count(args, 3)?;
                let expr = Self::compile(&args[0])?;
                let pattern = args[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("RegexCapture requires string pattern"))?;
                let group = args[2]
                    .as_u64()
                    .ok_or_else(|| anyhow!("RegexCapture requires numeric group index"))?
                    as usize;
                let regex = Regex::new(pattern)
                    .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;
                Ok(CompiledExpression::RegexCapture(
                    Box::new(expr),
                    regex,
                    group,
                ))
            }

            // Value operations
            Operator::Boolean => {
                Self::ensure_arg_count(args, 1)?;
                Ok(CompiledExpression::Boolean(Box::new(Self::compile(
                    &args[0],
                )?)))
            }
            Operator::Literal => {
                Self::ensure_arg_count(args, 1)?;
                Ok(CompiledExpression::Literal(
                    ExpressionValue::from_json_value(&args[0]),
                ))
            }

            // Context operations
            Operator::Tag => {
                Self::ensure_arg_count(args, 1)?;
                let tag_name = args[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("Tag operator requires string argument"))?;
                Ok(CompiledExpression::Tag(tag_name.to_string()))
            }
            Operator::Key => {
                Self::ensure_arg_count(args, 0)?;
                Ok(CompiledExpression::Key)
            }
            Operator::Type => {
                Self::ensure_arg_count(args, 0)?;
                Ok(CompiledExpression::Type)
            }
        }
    }

    fn ensure_arg_count(args: &[Value], expected: usize) -> Result<()> {
        if args.len() != expected {
            return Err(anyhow!(
                "Expected {} arguments, got {}",
                expected,
                args.len()
            ));
        }
        Ok(())
    }

    fn ensure_min_arg_count(args: &[Value], min: usize) -> Result<()> {
        if args.len() < min {
            return Err(anyhow!(
                "Expected at least {} arguments, got {}",
                min,
                args.len()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compile_simple_equality() {
        let expr = json!(["==", ["tag", "kind"], "park"]);
        let compiled = ExpressionCompiler::compile(&expr).unwrap();

        match compiled {
            CompiledExpression::Equal(_, _) => {}
            _ => panic!("Expected Equal expression"),
        }
    }

    #[test]
    fn test_compile_logical_any() {
        let expr = json!([
            "any",
            ["==", ["tag", "kind"], "park"],
            ["==", ["tag", "kind"], "school"]
        ]);

        let compiled = ExpressionCompiler::compile(&expr).unwrap();

        match compiled {
            CompiledExpression::Any(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected Any expression"),
        }
    }

    #[test]
    fn test_compile_membership_in() {
        let expr = json!([
            "in",
            ["tag", "kind"],
            ["literal", ["park", "school", "hospital"]]
        ]);
        let compiled = ExpressionCompiler::compile(&expr).unwrap();

        match compiled {
            CompiledExpression::In(_, values) => {
                assert_eq!(values.len(), 3);
                assert!(values.contains(&ExpressionValue::String("park".to_string())));
            }
            _ => panic!("Expected In expression"),
        }
    }

    #[test]
    fn test_compile_regex_match() {
        let expr = json!(["regex-match", ["key"], "^name:.*"]);
        let compiled = ExpressionCompiler::compile(&expr).unwrap();

        match compiled {
            CompiledExpression::RegexMatch(_, _) => {}
            _ => panic!("Expected RegexMatch expression"),
        }
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let expr = json!(["regex-match", ["key"], "["]);
        let result = ExpressionCompiler::compile(&expr);
        assert!(result.is_err());
    }

    #[test]
    fn test_expression_value_conversions() {
        let val = ExpressionValue::String("test".to_string());
        assert_eq!(val.to_string(), "test");
        assert!(val.to_bool());

        let val = ExpressionValue::Number(0);
        assert!(!val.to_bool());

        let val = ExpressionValue::Boolean(true);
        assert!(val.to_bool());
    }
}
