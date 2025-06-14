use anyhow::Result;
use geozero::mvt::tile::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

use super::expression_compiler::{CompiledExpression, ExpressionValue};

/// Context for expression evaluation
#[derive(Debug)]
pub struct EvaluationContext {
    /// Layer name
    pub layer_name: String,
    /// Feature properties/tags
    pub properties: HashMap<String, Value>,
    /// Current tag key being processed (for key-based operations)
    pub current_key: Option<String>,
    /// Feature geometry type
    pub geometry_type: Option<String>,
}

impl EvaluationContext {
    pub fn new(layer_name: &str, properties: HashMap<String, Value>) -> Self {
        Self {
            layer_name: layer_name.to_string(),
            properties,
            current_key: None,
            geometry_type: None,
        }
    }

    pub fn with_current_key(mut self, key: &str) -> Self {
        self.current_key = Some(key.to_string());
        self
    }

    pub fn with_geometry_type(mut self, geometry_type: &str) -> Self {
        self.geometry_type = Some(geometry_type.to_string());
        self
    }
}

/// Executes compiled expressions against feature data
pub struct ExpressionExecutor;

impl ExpressionExecutor {
    /// Evaluate a compiled expression in the given context
    pub fn evaluate(
        expr: &CompiledExpression,
        context: &EvaluationContext,
    ) -> Result<ExpressionValue> {
        match expr {
            // Comparison operations
            CompiledExpression::Equal(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                Ok(ExpressionValue::Boolean(
                    Self::compare_values(&left_val, &right_val) == Ordering::Equal,
                ))
            }
            CompiledExpression::NotEqual(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                Ok(ExpressionValue::Boolean(
                    Self::compare_values(&left_val, &right_val) != Ordering::Equal,
                ))
            }
            CompiledExpression::LessThan(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                Ok(ExpressionValue::Boolean(
                    Self::compare_values(&left_val, &right_val) == Ordering::Less,
                ))
            }
            CompiledExpression::GreaterThan(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                Ok(ExpressionValue::Boolean(
                    Self::compare_values(&left_val, &right_val) == Ordering::Greater,
                ))
            }
            CompiledExpression::LessThanOrEqual(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                let cmp = Self::compare_values(&left_val, &right_val);
                Ok(ExpressionValue::Boolean(
                    cmp == Ordering::Less || cmp == Ordering::Equal,
                ))
            }
            CompiledExpression::GreaterThanOrEqual(left, right) => {
                let left_val = Self::evaluate(left, context)?;
                let right_val = Self::evaluate(right, context)?;
                let cmp = Self::compare_values(&left_val, &right_val);
                Ok(ExpressionValue::Boolean(
                    cmp == Ordering::Greater || cmp == Ordering::Equal,
                ))
            }

            // Logical operations
            CompiledExpression::Any(exprs) => {
                for expr in exprs {
                    let result = Self::evaluate(expr, context)?;
                    if result.to_bool() {
                        return Ok(ExpressionValue::Boolean(true));
                    }
                }
                Ok(ExpressionValue::Boolean(false))
            }
            CompiledExpression::All(exprs) => {
                for expr in exprs {
                    let result = Self::evaluate(expr, context)?;
                    if !result.to_bool() {
                        return Ok(ExpressionValue::Boolean(false));
                    }
                }
                Ok(ExpressionValue::Boolean(true))
            }
            CompiledExpression::None(exprs) => {
                for expr in exprs {
                    let result = Self::evaluate(expr, context)?;
                    if result.to_bool() {
                        return Ok(ExpressionValue::Boolean(false));
                    }
                }
                Ok(ExpressionValue::Boolean(true))
            }
            CompiledExpression::Not(expr) => {
                let result = Self::evaluate(expr, context)?;
                Ok(ExpressionValue::Boolean(!result.to_bool()))
            }

            // Membership operations
            CompiledExpression::In(expr, values) => {
                let val = Self::evaluate(expr, context)?;
                Ok(ExpressionValue::Boolean(values.contains(&val)))
            }

            // String operations
            CompiledExpression::StartsWith(expr, prefix) => {
                let val = Self::evaluate(expr, context)?;
                let str_val = val.to_string();
                Ok(ExpressionValue::Boolean(str_val.starts_with(prefix)))
            }
            CompiledExpression::EndsWith(expr, suffix) => {
                let val = Self::evaluate(expr, context)?;
                let str_val = val.to_string();
                Ok(ExpressionValue::Boolean(str_val.ends_with(suffix)))
            }
            CompiledExpression::RegexMatch(expr, regex) => {
                let val = Self::evaluate(expr, context)?;
                let str_val = val.to_string();
                Ok(ExpressionValue::Boolean(regex.is_match(&str_val)))
            }
            CompiledExpression::RegexCapture(expr, regex, group_idx) => {
                let val = Self::evaluate(expr, context)?;
                let str_val = val.to_string();
                if let Some(captures) = regex.captures(&str_val) {
                    if let Some(group) = captures.get(*group_idx) {
                        Ok(ExpressionValue::String(group.as_str().to_string()))
                    } else {
                        Ok(ExpressionValue::Null)
                    }
                } else {
                    Ok(ExpressionValue::Null)
                }
            }

            // Value operations
            CompiledExpression::Boolean(expr) => {
                let val = Self::evaluate(expr, context)?;
                Ok(ExpressionValue::Boolean(val.to_bool()))
            }
            CompiledExpression::Literal(value) => Ok(value.clone()),

            // Context operations
            CompiledExpression::Tag(tag_name) => {
                if let Some(value) = context.properties.get(tag_name.as_str()) {
                    Ok(ExpressionValue::from_mvt_value(value))
                } else {
                    Ok(ExpressionValue::Null)
                }
            }
            CompiledExpression::Key => {
                if let Some(key) = &context.current_key {
                    Ok(ExpressionValue::String(key.clone()))
                } else {
                    Ok(ExpressionValue::Null)
                }
            }
            CompiledExpression::Type => {
                if let Some(geom_type) = &context.geometry_type {
                    Ok(ExpressionValue::String(geom_type.clone()))
                } else {
                    Ok(ExpressionValue::Null)
                }
            }
        }
    }

    /// Compare two expression values with proper type coercion
    fn compare_values(left: &ExpressionValue, right: &ExpressionValue) -> Ordering {
        match (left, right) {
            // Null comparisons
            (ExpressionValue::Null, ExpressionValue::Null) => Ordering::Equal,
            (ExpressionValue::Null, _) => Ordering::Less,
            (_, ExpressionValue::Null) => Ordering::Greater,

            // Boolean comparisons
            (ExpressionValue::Boolean(a), ExpressionValue::Boolean(b)) => a.cmp(b),

            // Numeric comparisons
            (ExpressionValue::Number(a), ExpressionValue::Number(b)) => a.cmp(b),
            (ExpressionValue::Float(a), ExpressionValue::Float(b)) => {
                // Parse and compare as f64
                let a_float: f64 = a.parse().unwrap_or(0.0);
                let b_float: f64 = b.parse().unwrap_or(0.0);
                a_float.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
            }
            (ExpressionValue::Number(a), ExpressionValue::Float(b)) => {
                let a_float = *a as f64;
                let b_float: f64 = b.parse().unwrap_or(0.0);
                a_float.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
            }
            (ExpressionValue::Float(a), ExpressionValue::Number(b)) => {
                let a_float: f64 = a.parse().unwrap_or(0.0);
                let b_float = *b as f64;
                a_float.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
            }

            // String comparisons
            (ExpressionValue::String(a), ExpressionValue::String(b)) => a.cmp(b),

            // Mixed type comparisons - convert to strings
            _ => left.to_string().cmp(&right.to_string()),
        }
    }

    /// Evaluate expression and return boolean result
    pub fn evaluate_bool(expr: &CompiledExpression, context: &EvaluationContext) -> Result<bool> {
        let result = Self::evaluate(expr, context)?;
        Ok(result.to_bool())
    }
}

#[cfg(test)]
mod tests {
    use super::super::expression_compiler::ExpressionCompiler;
    use super::*;
    use geozero::mvt::tile::Value;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_test_context() -> EvaluationContext {
        let name: Value = Value {
            string_value: Some("Central Park".to_string()),
            float_value: None,
            double_value: None,
            int_value: None,
            uint_value: None,
            sint_value: None,
            bool_value: None,
        };
        let kind: Value = Value {
            string_value: Some("park".to_string()),
            float_value: None,
            double_value: None,
            int_value: None,
            uint_value: None,
            sint_value: None,
            bool_value: None,
        };
        let area: Value = Value {
            string_value: None,
            float_value: None,
            double_value: Some(3.41),
            int_value: None,
            uint_value: None,
            sint_value: None,
            bool_value: None,
        };
        let public: Value = Value {
            string_value: None,
            float_value: None,
            double_value: None,
            int_value: None,
            uint_value: None,
            sint_value: None,
            bool_value: Some(true),
        };
        let capacity: Value = Value {
            string_value: None,
            float_value: None,
            double_value: None,
            int_value: None,
            uint_value: None,
            sint_value: Some(1000),
            bool_value: None,
        };

        let mut properties = HashMap::new();
        properties.insert("name".to_string(), name);
        properties.insert("kind".to_string(), kind);
        properties.insert("area".to_string(), area);
        properties.insert("public".to_string(), public);
        properties.insert("capacity".to_string(), capacity);

        EvaluationContext::new("test", properties)
            .with_geometry_type("Polygon")
            .with_current_key("name:en")
    }

    #[test]
    fn test_simple_equality_filter() {
        let context = create_test_context();
        let expr_json = json!(["==", ["tag", "kind"], "park"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();

        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_inequality_filter() {
        let context = create_test_context();
        let expr_json = json!(["!=", ["tag", "kind"], "school"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();

        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_numeric_comparison() {
        let context = create_test_context();
        let expr_json = json!([">", ["tag", "capacity"], 500]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();

        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        let expr_json = json!(["<", ["tag", "area"], 5.0]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();

        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_logical_operations() {
        let context = create_test_context();

        // Test ANY - should be true if any condition matches
        let expr_json = json!([
            "any",
            ["==", ["tag", "kind"], "school"],
            ["==", ["tag", "kind"], "park"]
        ]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test ALL - should be true if all conditions match
        let expr_json = json!([
            "all",
            ["==", ["tag", "kind"], "park"],
            [">", ["tag", "capacity"], 100]
        ]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test NOT - should invert the result
        let expr_json = json!(["!", ["==", ["tag", "kind"], "school"]]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_membership_operations() {
        let context = create_test_context();

        // Test IN operation
        let expr_json = json!([
            "in",
            ["tag", "kind"],
            ["literal", ["park", "school", "hospital"]]
        ]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test NOT IN operation
        let expr_json = json!([
            "!",
            ["in", ["tag", "kind"], ["literal", ["school", "hospital"]]]
        ]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test IN operation with null
        let expr_json = json!(["in", null, ["literal", [null, "school", "hospital"]]]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_string_operations() {
        let context = create_test_context();

        // Test starts-with
        let expr_json = json!(["starts-with", ["tag", "name"], "Central"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test ends-with
        let expr_json = json!(["ends-with", ["tag", "name"], "Park"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test regex match
        let expr_json = json!(["regex-match", ["tag", "name"], "^Central.*Park$"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_context_operations() {
        let context = create_test_context();

        // Test key operation
        let expr_json = json!(["starts-with", ["key"], "name:"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);

        // Test type operation
        let expr_json = json!(["==", ["type"], "Polygon"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_boolean_type_conversion() {
        let context = create_test_context();

        // Test boolean tag
        let expr_json = json!(["boolean", ["tag", "public"]]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_missing_tag_handling() {
        let context = create_test_context();

        // Test accessing non-existent tag
        let expr_json = json!(["==", ["tag", "nonexistent"], "value"]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(!result); // Should be false since null != "value"
    }

    #[test]
    fn test_complex_filter_example() {
        let context = create_test_context();

        // Complex filter: public parks with capacity > 500 or area > 2.0
        let expr_json = json!([
            "all",
            ["==", ["tag", "kind"], "park"],
            ["boolean", ["tag", "public"]],
            [
                "any",
                [">", ["tag", "capacity"], 500],
                [">", ["tag", "area"], 2.0]
            ]
        ]);

        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        assert!(result);
    }

    #[test]
    fn test_regex_capture() {
        let context = create_test_context();

        // Test regex capture - extract first word from name
        let expr_json = json!(["regex-capture", ["tag", "name"], r"^(\w+)", 1]);
        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate(&compiled, &context).unwrap();

        assert_eq!(result, ExpressionValue::String("Central".to_string()));
    }

    #[test]
    fn test_complex_regex_capture_filter() {
        let mut context = create_test_context();

        // Complex filter: keys starting with "name" but excluding null and "ja" language codes
        let expr_json = json!([
            "all",
            ["starts-with", ["key"], "name"],
            [
                "not",
                [
                    "in",
                    ["regex-capture", ["key"], "^name:?(.*)$", 1],
                    ["literal", ["", "ja"]]
                ]
            ]
        ]);

        let compiled = ExpressionCompiler::compile(&expr_json).unwrap();
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();

        // Should be true because:
        // 1. Key "name:en" starts with "name" ✓
        // 2. Regex capture extracts "en" from "name:en"
        // 3. "en" is not in [null, "ja"] ✓
        assert!(result);

        context = context.with_current_key("name:ja");
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        // Should be false because:
        // 1. Key "name:ja" starts with "name" ✓
        // 2. Regex capture extracts "ja" from "name:ja"
        // 3. "ja" is in [null, "ja"] ✗
        assert!(!result);

        context = context.with_current_key("name");
        let result = ExpressionExecutor::evaluate_bool(&compiled, &context).unwrap();
        // Should be false because:
        // 1. Key "name" does starts with "name" ✓
        // 2. Regex capture extracts null from "name"
        // 3. null is in [null, "ja"] ✗
        assert!(!result);
    }
}
