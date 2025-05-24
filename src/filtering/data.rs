use super::expression_compiler::{CompiledExpression, ExpressionCompiler};
use anyhow::{Result, anyhow};
use geojson::Geometry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Represents a GeoJSON filtering specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCollection {
    #[serde(rename = "type")]
    pub feature_type: String, // Should be "FeatureCollection"
    pub features: Vec<FilterFeature>,
}

/// A single filter feature with geometry and layer rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterFeature {
    #[serde(rename = "type")]
    pub feature_type: String, // Should be "Feature"
    pub geometry: Geometry,
    pub properties: FilterProperties,
}

/// Properties containing filter rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterProperties {
    pub id: Option<String>,
    pub description: Option<String>,
    pub layers: HashMap<String, LayerFilter>,
}

/// Filter rules for a specific layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerFilter {
    /// Expression to remove entire features
    pub feature: Option<Expression>,
    /// Expression to remove specific tags
    pub tag: Option<Expression>,
}

impl LayerFilter {
    /// Compile the filter expressions for efficient evaluation
    pub fn compile(&self) -> Result<CompiledLayerFilter> {
        let feature = if let Some(ref expr) = self.feature {
            Some(ExpressionCompiler::compile(expr)?)
        } else {
            None
        };

        let tag = if let Some(ref expr) = self.tag {
            Some(ExpressionCompiler::compile(expr)?)
        } else {
            None
        };

        Ok(CompiledLayerFilter { feature, tag })
    }
}

/// Compiled version of LayerFilter for efficient evaluation
#[derive(Debug, Clone)]
pub struct CompiledLayerFilter {
    pub feature: Option<CompiledExpression>,
    pub tag: Option<CompiledExpression>,
}

/// Represents a filter expression using JSON array syntax
pub type Expression = Value;

/// Supported operators in filter expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,

    // Logical
    Any,
    All,
    None,
    Not,

    // Membership
    In,

    // String operations
    StartsWith,
    EndsWith,
    RegexMatch,
    RegexCapture,

    // Casting
    Boolean,
    Literal,

    // Context
    Tag,  // feature property lookup
    Key,  // current tag key
    Type, // feature geometry type (type)
}

impl Operator {
    /// Parse operator from string
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "==" => Ok(Operator::Equal),
            "!=" => Ok(Operator::NotEqual),
            "<" => Ok(Operator::LessThan),
            ">" => Ok(Operator::GreaterThan),
            "<=" => Ok(Operator::LessThanOrEqual),
            ">=" => Ok(Operator::GreaterThanOrEqual),
            "any" => Ok(Operator::Any),
            "all" => Ok(Operator::All),
            "none" => Ok(Operator::None),
            "not" => Ok(Operator::Not),
            "!" => Ok(Operator::Not),
            "in" => Ok(Operator::In),
            "starts-with" => Ok(Operator::StartsWith),
            "ends-with" => Ok(Operator::EndsWith),
            "regex-match" => Ok(Operator::RegexMatch),
            "regex-capture" => Ok(Operator::RegexCapture),
            "boolean" => Ok(Operator::Boolean),
            "literal" => Ok(Operator::Literal),
            "tag" => Ok(Operator::Tag),
            "key" => Ok(Operator::Key),
            "type" => Ok(Operator::Type),
            _ => Err(anyhow!("Unknown operator: {}", s)),
        }
    }
}

impl FilterCollection {
    /// Compile the entire filter collection for efficient evaluation
    pub fn compile(&self) -> Result<CompiledFilterCollection> {
        let mut compiled_features = Vec::new();

        for feature in &self.features {
            compiled_features.push(feature.compile()?);
        }

        Ok(CompiledFilterCollection {
            features: compiled_features,
        })
    }
}

/// Compiled version of FilterCollection for efficient evaluation
#[derive(Debug, Clone)]
pub struct CompiledFilterCollection {
    pub features: Vec<CompiledFilterFeature>,
}

/// Compiled version of FilterFeature for efficient evaluation
#[derive(Debug, Clone)]
pub struct CompiledFilterFeature {
    pub geometry: geo_types::Geometry<f64>,
    pub layers: HashMap<String, CompiledLayerFilter>,
}

impl FilterFeature {
    /// Compile the filter feature for efficient evaluation
    pub fn compile(&self) -> Result<CompiledFilterFeature> {
        let compiled_layers = self.compile_layers()?;

        Ok(CompiledFilterFeature {
            geometry: self.geometry.clone().try_into()?,
            layers: compiled_layers,
        })
    }

    /// Compile all layer filters for efficient evaluation
    pub fn compile_layers(&self) -> Result<HashMap<String, CompiledLayerFilter>> {
        let mut compiled_layers = HashMap::new();

        for (layer_name, layer_filter) in &self.properties.layers {
            let compiled = layer_filter.compile()?;
            compiled_layers.insert(layer_name.clone(), compiled);
        }

        Ok(compiled_layers)
    }
}

impl CompiledFilterFeature {
    /// Check if this feature should be removed based on its feature filters
    /// Returns true if the feature should be removed (filtered out)
    pub fn should_remove_feature(
        &self,
        context: &super::executor::EvaluationContext,
    ) -> Result<bool> {
        // Check if there's a layer filter for this specific layer
        if let Some(layer_filter) = self.layers.get(&context.layer_name) {
            if let Some(ref feature_expr) = layer_filter.feature {
                return super::executor::ExpressionExecutor::evaluate_bool(feature_expr, context);
            }
        }

        // Check if there's a wildcard layer filter
        if let Some(layer_filter) = self.layers.get("*") {
            if let Some(ref feature_expr) = layer_filter.feature {
                return super::executor::ExpressionExecutor::evaluate_bool(feature_expr, context);
            }
        }

        // No matching filter found, don't remove the feature
        Ok(false)
    }

    /// Check if a specific tag should be removed
    /// Returns true if the tag should be removed (filtered out)
    pub fn should_remove_tag(&self, context: &super::executor::EvaluationContext) -> Result<bool> {
        // Check if there's a layer filter for this specific layer
        if let Some(layer_filter) = self.layers.get(&context.layer_name) {
            if let Some(ref tag_expr) = layer_filter.tag {
                return super::executor::ExpressionExecutor::evaluate_bool(tag_expr, context);
            }
        }

        // Check if there's a wildcard layer filter
        if let Some(layer_filter) = self.layers.get("*") {
            if let Some(ref tag_expr) = layer_filter.tag {
                return super::executor::ExpressionExecutor::evaluate_bool(tag_expr, context);
            }
        }

        // No matching filter found, don't remove the tag
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geojson::Value;
    use serde_json::json;

    #[test]
    fn test_parse_basic_filter() {
        let json_str = r#"
        {
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "geometry": {
                "type": "Polygon",
                "coordinates": [[
                  [-180,-90], [-180,90], [180,90], [180,-90], [-180,-90]
                ]]
              },
              "properties": {
                "id": "global-park-school",
                "description": "Remove park/school features worldwide",
                "layers": {
                  "*": {
                    "feature": [
                      "in",
                      ["tag","kind"],
                      ["literal", ["park","school"]]
                    ]
                  }
                }
              }
            }
          ]
        }
        "#;

        let filter: FilterCollection = serde_json::from_str(json_str).unwrap();
        assert_eq!(filter.feature_type, "FeatureCollection");
        assert_eq!(filter.features.len(), 1);

        let feature = &filter.features[0];
        assert_eq!(
            feature.properties.id.as_ref().unwrap(),
            "global-park-school"
        );
        assert!(feature.properties.layers.contains_key("*"));
    }

    #[test]
    fn test_validate_operators() {
        assert!(Operator::from_str("==").is_ok());
        assert!(Operator::from_str("in").is_ok());
        assert!(Operator::from_str("starts-with").is_ok());
        assert!(Operator::from_str("type").is_ok());
        assert!(Operator::from_str("invalid-op").is_err());
    }

    #[test]
    fn test_layer_filter_with_both_feature_and_tag() {
        let json_str = r#"
        {
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "geometry": {
                "type": "Point",
                "coordinates": [0, 0]
              },
              "properties": {
                "layers": {
                  "buildings": {
                    "feature": ["==", ["tag", "demolished"], true],
                    "tag": ["starts-with", ["key"], "temp:"]
                  }
                }
              }
            }
          ]
        }
        "#;

        let filter: FilterCollection = serde_json::from_str(json_str).unwrap();
        let feature = &filter.features[0];
        let buildings_filter = &feature.properties.layers["buildings"];

        assert!(buildings_filter.feature.is_some());
        assert!(buildings_filter.tag.is_some());
    }

    #[test]
    fn test_complex_expressions() {
        let json_str = r#"
        {
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "geometry": {
                "type": "Polygon",
                "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]
              },
              "properties": {
                "layers": {
                  "*": {
                    "feature": [
                      "any",
                      ["==", ["type"], "Point"],
                      ["==", ["type"], "LineString"]
                    ],
                    "tag": [
                      "regex-capture",
                      ["key"],
                      "^name:?(.*)$",
                      1
                    ]
                  }
                }
              }
            }
          ]
        }
        "#;

        let filter: FilterCollection = serde_json::from_str(json_str).unwrap();
        assert_eq!(filter.features.len(), 1);

        let layer_filter = &filter.features[0].properties.layers["*"];
        assert!(layer_filter.feature.is_some());
        assert!(layer_filter.tag.is_some());
    }

    #[test]
    fn test_serialize_to_json() {
        let filter = FilterCollection {
            feature_type: "FeatureCollection".to_string(),
            features: vec![FilterFeature {
                feature_type: "Feature".to_string(),
                geometry: Geometry::new(Value::Point(vec![0.0, 0.0])),
                properties: FilterProperties {
                    id: Some("test-filter".to_string()),
                    description: Some("Test filter".to_string()),
                    layers: {
                        let mut map = HashMap::new();
                        map.insert(
                            "*".to_string(),
                            LayerFilter {
                                feature: Some(json!(["==", ["tag", "test"], true])),
                                tag: None,
                            },
                        );
                        map
                    },
                },
            }],
        };

        let json_output = serde_json::to_string(&filter).unwrap();
        assert!(json_output.contains("FeatureCollection"));
        assert!(json_output.contains("test-filter"));

        // Verify we can parse it back
        let parsed: FilterCollection = serde_json::from_str(&json_output).unwrap();
        assert_eq!(
            parsed.features[0].properties.id,
            Some("test-filter".to_string())
        );
    }
}
