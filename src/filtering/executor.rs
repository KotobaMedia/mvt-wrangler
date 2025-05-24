pub use anyhow::{Result, anyhow};
pub use geo::Intersects;
pub use geo_types::Geometry;
pub use geozero::{
    ToGeo,
    mvt::tile::{Feature, Value},
};
pub use regex::Regex;
pub use serde_json::Value as JsonValue;
pub use std::collections::HashMap;

use super::data::{Expression, FilterCollection, LayerFilter, Operator};

/// Context for evaluating expressions against a feature
pub struct EvaluationContext<'a> {
    /// The layer name this feature belongs to
    pub layer_name: &'a str,
    /// The feature's geometry type (Point, LineString, etc.)
    pub geometry_type: &'a str,
    /// The feature's tags as key-value pairs
    pub tags: &'a HashMap<String, String>,
    /// Current tag key being evaluated (for tag-level filters)
    pub current_key: Option<&'a str>,
    /// Current tag value being evaluated (for tag-level filters)
    pub current_value: Option<&'a str>,
}

/// Executes dynamic filtering on vector tile features
pub struct FilterExecutor {
    filters: FilterCollection,
    /// Cached compiled regexes for performance
    regex_cache: HashMap<String, Regex>,
}

impl FilterExecutor {
    /// Create a new filter executor from a filter collection
    pub fn new(filters: FilterCollection) -> Self {
        Self {
            filters,
            regex_cache: HashMap::new(),
        }
    }

    /// Apply filters to a feature, returning whether to keep it and which tags to keep
    pub fn apply_feature_filters(
        &mut self,
        feature: &Feature,
        feature_geometry: &Geometry<f64>,
        layer_name: &str,
        layer_keys: &[String],
        layer_values: &[Value],
    ) -> Result<(bool, Vec<u32>)> {
        // Convert feature tags to a more accessible format
        let tags = self.extract_feature_tags(feature, layer_keys, layer_values)?;

        // Determine geometry type
        let geometry_type = match feature_geometry {
            Geometry::Point(_) => Ok("Point"),
            Geometry::LineString(_) => Ok("LineString"),
            Geometry::Polygon(_) => Ok("Polygon"),
            Geometry::MultiPoint(_) => Ok("Point"),
            Geometry::MultiLineString(_) => Ok("LineString"),
            Geometry::MultiPolygon(_) => Ok("Polygon"),
            _ => Err(anyhow!("Unsupported geometry type: {:?}", feature_geometry)),
        }?;

        let context = EvaluationContext {
            layer_name,
            geometry_type,
            tags: &tags,
            current_key: None,
            current_value: None,
        };

        // Find applicable layer filter
        let layer_filter = self.find_layer_filter(layer_name, feature_geometry)?;

        let keep_feature = if let Some(layer_filter) = layer_filter {
            // Apply feature-level filter
            if let Some(ref feature_expr) = layer_filter.feature {
                let should_remove = self.evaluate_expression(feature_expr, &context)?;
                !should_remove // Keep if expression evaluates to false
            } else {
                true // No feature filter, keep the feature
            }
        } else {
            true // No applicable filter, keep the feature
        };

        if !keep_feature {
            return Ok((false, Vec::new()));
        }

        // Apply tag-level filters
        let filtered_tags = if let Some(layer_filter) = layer_filter {
            if let Some(ref tag_expr) = layer_filter.tag {
                self.filter_tags(feature, layer_keys, layer_values, tag_expr, &context)?
            } else {
                feature.tags.clone() // No tag filter, keep all tags
            }
        } else {
            feature.tags.clone() // No applicable filter, keep all tags
        };

        Ok((true, filtered_tags))
    }

    /// Find the applicable layer filter for a feature
    fn find_layer_filter(
        &self,
        layer_name: &str,
        feature_geometry: &Geometry<f64>,
    ) -> Result<Option<&LayerFilter>> {
        for filter_feature in &self.filters.features {
            // Check spatial intersection
            let filter_geometry = self.parse_geojson_geometry(&filter_feature.geometry)?;
            if !feature_geometry.intersects(&filter_geometry) {
                continue;
            }

            // Check for layer-specific filter
            if let Some(layer_filter) = filter_feature.properties.layers.get(layer_name) {
                return Ok(Some(layer_filter));
            }

            // Check for wildcard filter
            if let Some(layer_filter) = filter_feature.properties.layers.get("*") {
                return Ok(Some(layer_filter));
            }
        }
        Ok(None)
    }

    /// Extract feature tags as a HashMap for easier access
    fn extract_feature_tags(
        &self,
        feature: &Feature,
        layer_keys: &[String],
        layer_values: &[Value],
    ) -> Result<HashMap<String, String>> {
        let mut tags = HashMap::new();

        for tags_pair in feature.tags.chunks_exact(2) {
            let key_index = tags_pair[0] as usize;
            let value_index = tags_pair[1] as usize;

            if key_index >= layer_keys.len() || value_index >= layer_values.len() {
                continue; // Skip invalid indices
            }

            let key = &layer_keys[key_index];
            let value = self.value_to_string(&layer_values[value_index]);

            tags.insert(key.clone(), value);
        }

        Ok(tags)
    }

    /// Filter tags based on tag expression
    fn filter_tags(
        &mut self,
        feature: &Feature,
        layer_keys: &[String],
        layer_values: &[Value],
        tag_expr: &Expression,
        base_context: &EvaluationContext,
    ) -> Result<Vec<u32>> {
        let mut filtered_tags = Vec::new();

        for tags_pair in feature.tags.chunks_exact(2) {
            let key_index = tags_pair[0] as usize;
            let value_index = tags_pair[1] as usize;

            if key_index >= layer_keys.len() || value_index >= layer_values.len() {
                continue;
            }

            let key = &layer_keys[key_index];
            let value = self.value_to_string(&layer_values[value_index]);

            // Create context for this specific tag
            let tag_context = EvaluationContext {
                current_key: Some(key),
                current_value: Some(&value),
                ..*base_context
            };

            // Evaluate tag filter - if true, remove the tag
            let should_remove = self.evaluate_expression(tag_expr, &tag_context)?;

            if !should_remove {
                filtered_tags.push(tags_pair[0]);
                filtered_tags.push(tags_pair[1]);
            }
        }

        Ok(filtered_tags)
    }

    /// Evaluate a filter expression
    fn evaluate_expression(
        &mut self,
        expr: &Expression,
        context: &EvaluationContext,
    ) -> Result<bool> {
        match expr {
            JsonValue::Array(arr) => {
                if arr.is_empty() {
                    return Err(anyhow!("Empty expression array"));
                }

                let operator_str = arr[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("First element must be operator string"))?;
                let operator = Operator::from_str(operator_str)?;

                self.evaluate_operator(&operator, &arr[1..], context)
            }
            JsonValue::Bool(b) => Ok(*b),
            _ => Err(anyhow!("Expression must be array or boolean")),
        }
    }

    /// Evaluate a specific operator with its operands
    fn evaluate_operator(
        &mut self,
        operator: &Operator,
        operands: &[JsonValue],
        context: &EvaluationContext,
    ) -> Result<bool> {
        match operator {
            // Comparison operators
            Operator::Equal => {
                if operands.len() != 2 {
                    return Err(anyhow!("== requires exactly 2 operands"));
                }
                let left = self.evaluate_operand(&operands[0], context)?;
                let right = self.evaluate_operand(&operands[1], context)?;
                Ok(left == right)
            }
            Operator::NotEqual => {
                if operands.len() != 2 {
                    return Err(anyhow!("!= requires exactly 2 operands"));
                }
                let left = self.evaluate_operand(&operands[0], context)?;
                let right = self.evaluate_operand(&operands[1], context)?;
                Ok(left != right)
            }

            // Logical operators
            Operator::Any => {
                for operand in operands {
                    if self.evaluate_expression(operand, context)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Operator::All => {
                for operand in operands {
                    if !self.evaluate_expression(operand, context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Operator::None => {
                for operand in operands {
                    if self.evaluate_expression(operand, context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Operator::Not => {
                if operands.len() != 1 {
                    return Err(anyhow!("not requires exactly 1 operand"));
                }
                Ok(!self.evaluate_expression(&operands[0], context)?)
            }

            // Membership operators
            Operator::In => {
                if operands.len() != 2 {
                    return Err(anyhow!("in requires exactly 2 operands"));
                }
                let value = self.evaluate_operand(&operands[0], context)?;
                let array = self.evaluate_array_operand(&operands[1], context)?;
                Ok(array.contains(&value))
            }
            Operator::NotIn => {
                if operands.len() != 2 {
                    return Err(anyhow!("not-in requires exactly 2 operands"));
                }
                let value = self.evaluate_operand(&operands[0], context)?;
                let array = self.evaluate_array_operand(&operands[1], context)?;
                Ok(!array.contains(&value))
            }

            // String operators
            Operator::StartsWith => {
                if operands.len() != 2 {
                    return Err(anyhow!("starts-with requires exactly 2 operands"));
                }
                let string = self.evaluate_operand(&operands[0], context)?;
                let prefix = self.evaluate_operand(&operands[1], context)?;
                Ok(string.starts_with(&prefix))
            }
            Operator::EndsWith => {
                if operands.len() != 2 {
                    return Err(anyhow!("ends-with requires exactly 2 operands"));
                }
                let string = self.evaluate_operand(&operands[0], context)?;
                let suffix = self.evaluate_operand(&operands[1], context)?;
                Ok(string.ends_with(&suffix))
            }
            Operator::RegexMatch => {
                if operands.len() != 2 {
                    return Err(anyhow!("regex-match requires exactly 2 operands"));
                }
                let string = self.evaluate_operand(&operands[0], context)?;
                let pattern = self.evaluate_operand(&operands[1], context)?;
                let regex = self.get_regex(&pattern)?;
                Ok(regex.is_match(&string))
            }

            _ => Err(anyhow!("Operator {:?} not yet implemented", operator)),
        }
    }

    /// Evaluate an operand to a string value
    fn evaluate_operand(&self, operand: &JsonValue, context: &EvaluationContext) -> Result<String> {
        match operand {
            JsonValue::String(s) => Ok(s.clone()),
            JsonValue::Number(n) => Ok(n.to_string()),
            JsonValue::Bool(b) => Ok(b.to_string()),
            JsonValue::Array(arr) => {
                if arr.is_empty() {
                    return Err(anyhow!("Empty array operand"));
                }

                let operator_str = arr[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("Array operand must start with operator string"))?;

                match operator_str {
                    "tag" => {
                        if arr.len() != 2 {
                            return Err(anyhow!("tag operator requires exactly 1 argument"));
                        }
                        let key = arr[1]
                            .as_str()
                            .ok_or_else(|| anyhow!("tag key must be string"))?;
                        Ok(context.tags.get(key).cloned().unwrap_or_default())
                    }
                    "key" => {
                        if arr.len() != 1 {
                            return Err(anyhow!("key operator requires no arguments"));
                        }
                        Ok(context.current_key.unwrap_or("").to_string())
                    }
                    "$type" => {
                        if arr.len() != 1 {
                            return Err(anyhow!("$type operator requires no arguments"));
                        }
                        Ok(context.geometry_type.to_string())
                    }
                    "literal" => {
                        if arr.len() != 2 {
                            return Err(anyhow!("literal operator requires exactly 1 argument"));
                        }
                        match &arr[1] {
                            JsonValue::String(s) => Ok(s.clone()),
                            JsonValue::Number(n) => Ok(n.to_string()),
                            JsonValue::Bool(b) => Ok(b.to_string()),
                            _ => Err(anyhow!("literal value must be string, number, or boolean")),
                        }
                    }
                    _ => Err(anyhow!("Unknown operand operator: {}", operator_str)),
                }
            }
            _ => Err(anyhow!("Unsupported operand type")),
        }
    }

    /// Evaluate an operand that should be an array of strings
    fn evaluate_array_operand(
        &self,
        operand: &JsonValue,
        context: &EvaluationContext,
    ) -> Result<Vec<String>> {
        match operand {
            JsonValue::Array(arr) => {
                if arr.is_empty() {
                    return Err(anyhow!("Empty array operand"));
                }

                let operator_str = arr[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("Array operand must start with operator string"))?;

                match operator_str {
                    "literal" => {
                        if arr.len() != 2 {
                            return Err(anyhow!("literal operator requires exactly 1 argument"));
                        }
                        match &arr[1] {
                            JsonValue::Array(literal_arr) => {
                                let mut result = Vec::new();
                                for item in literal_arr {
                                    match item {
                                        JsonValue::String(s) => result.push(s.clone()),
                                        JsonValue::Number(n) => result.push(n.to_string()),
                                        JsonValue::Bool(b) => result.push(b.to_string()),
                                        _ => {
                                            return Err(anyhow!(
                                                "literal array items must be string, number, or boolean"
                                            ));
                                        }
                                    }
                                }
                                Ok(result)
                            }
                            _ => Err(anyhow!(
                                "literal operator for array must have array argument"
                            )),
                        }
                    }
                    _ => Err(anyhow!(
                        "Unsupported array operand operator: {}",
                        operator_str
                    )),
                }
            }
            _ => Err(anyhow!("Array operand must be array")),
        }
    }

    /// Get or compile a regex pattern
    fn get_regex(&mut self, pattern: &str) -> Result<&Regex> {
        if !self.regex_cache.contains_key(pattern) {
            let regex = Regex::new(pattern)
                .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;
            self.regex_cache.insert(pattern.to_string(), regex);
        }
        Ok(self.regex_cache.get(pattern).unwrap())
    }

    /// Convert MVT Value to string
    fn value_to_string(&self, value: &Value) -> String {
        match value {
            Value::StringValue(s) => s.clone(),
            Value::FloatValue(f) => f.to_string(),
            Value::DoubleValue(d) => d.to_string(),
            Value::IntValue(i) => i.to_string(),
            Value::UintValue(u) => u.to_string(),
            Value::SintValue(s) => s.to_string(),
            Value::BoolValue(b) => b.to_string(),
        }
    }

    /// Parse GeoJSON geometry from our data structure
    fn parse_geojson_geometry(&self, geom: &super::data::GeoJsonGeometry) -> Result<Geometry<f64>> {
        // This is a simplified implementation - in a real scenario you'd want
        // a proper GeoJSON geometry parser
        match geom.geometry_type.as_str() {
            "Point" => {
                if let JsonValue::Array(coords) = &geom.coordinates {
                    if coords.len() >= 2 {
                        let x = coords[0]
                            .as_f64()
                            .ok_or_else(|| anyhow!("Invalid x coordinate"))?;
                        let y = coords[1]
                            .as_f64()
                            .ok_or_else(|| anyhow!("Invalid y coordinate"))?;
                        return Ok(Geometry::Point((x, y).into()));
                    }
                }
                Err(anyhow!("Invalid Point coordinates"))
            }
            "Polygon" => {
                // For now, just create a simple bounding box from the coordinates
                // In a real implementation, you'd properly parse the polygon
                if let JsonValue::Array(rings) = &geom.coordinates {
                    if let Some(JsonValue::Array(exterior)) = rings.first() {
                        if exterior.len() >= 4 {
                            // Create a simple polygon from the coordinates
                            let mut coords = Vec::new();
                            for coord_pair in exterior {
                                if let JsonValue::Array(pair) = coord_pair {
                                    if pair.len() >= 2 {
                                        let x = pair[0]
                                            .as_f64()
                                            .ok_or_else(|| anyhow!("Invalid x coordinate"))?;
                                        let y = pair[1]
                                            .as_f64()
                                            .ok_or_else(|| anyhow!("Invalid y coordinate"))?;
                                        coords.push((x, y).into());
                                    }
                                }
                            }
                            if coords.len() >= 4 {
                                use geo_types::Polygon;
                                let polygon = Polygon::new(coords.into(), vec![]);
                                return Ok(Geometry::Polygon(polygon));
                            }
                        }
                    }
                }
                Err(anyhow!("Invalid Polygon coordinates"))
            }
            _ => Err(anyhow!("Unsupported geometry type: {}", geom.geometry_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_evaluate_basic_expression() {
        let filters = FilterCollection {
            feature_type: "FeatureCollection".to_string(),
            features: vec![],
        };
        let mut executor = FilterExecutor::new(filters);

        let context = EvaluationContext {
            layer_name: "test",
            geometry_type: "Point",
            tags: &[("kind".to_string(), "park".to_string())]
                .iter()
                .cloned()
                .collect(),
            current_key: None,
            current_value: None,
        };

        // Test simple equality
        let expr = json!(["==", ["tag", "kind"], ["literal", "park"]]);
        let result = executor.evaluate_expression(&expr, &context).unwrap();
        assert!(result);

        // Test inequality
        let expr = json!(["==", ["tag", "kind"], ["literal", "school"]]);
        let result = executor.evaluate_expression(&expr, &context).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_evaluate_membership_expression() {
        let filters = FilterCollection {
            feature_type: "FeatureCollection".to_string(),
            features: vec![],
        };
        let mut executor = FilterExecutor::new(filters);

        let context = EvaluationContext {
            layer_name: "test",
            geometry_type: "Point",
            tags: &[("kind".to_string(), "park".to_string())]
                .iter()
                .cloned()
                .collect(),
            current_key: None,
            current_value: None,
        };

        // Test membership
        let expr = json!(["in", ["tag", "kind"], ["literal", ["park", "school"]]]);
        let result = executor.evaluate_expression(&expr, &context).unwrap();
        assert!(result);

        // Test non-membership
        let expr = json!(["in", ["tag", "kind"], ["literal", ["building", "road"]]]);
        let result = executor.evaluate_expression(&expr, &context).unwrap();
        assert!(!result);
    }
}
