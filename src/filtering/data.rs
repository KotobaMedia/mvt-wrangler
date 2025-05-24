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
    NotIn,

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
    Type, // feature geometry type ($type)
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
            "in" => Ok(Operator::In),
            "not-in" => Ok(Operator::NotIn),
            "starts-with" => Ok(Operator::StartsWith),
            "ends-with" => Ok(Operator::EndsWith),
            "regex-match" => Ok(Operator::RegexMatch),
            "regex-capture" => Ok(Operator::RegexCapture),
            "boolean" => Ok(Operator::Boolean),
            "literal" => Ok(Operator::Literal),
            "tag" => Ok(Operator::Tag),
            "key" => Ok(Operator::Key),
            "$type" => Ok(Operator::Type),
            _ => Err(anyhow!("Unknown operator: {}", s)),
        }
    }
}

impl FilterCollection {
    /// Parse a filter collection from JSON string
    pub fn from_json(json: &str) -> Result<Self> {
        let collection: FilterCollection = serde_json::from_str(json)?;
        collection.validate()?;
        Ok(collection)
    }

    /// Validate the filter collection structure
    pub fn validate(&self) -> Result<()> {
        if self.feature_type != "FeatureCollection" {
            return Err(anyhow!("Root type must be 'FeatureCollection'"));
        }

        for feature in &self.features {
            feature.validate()?;
        }

        Ok(())
    }

    /// Serialize to JSON string
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl FilterFeature {
    /// Validate the filter feature structure
    pub fn validate(&self) -> Result<()> {
        if self.feature_type != "Feature" {
            return Err(anyhow!("Feature type must be 'Feature'"));
        }

        // Validate that we have at least one layer filter
        if self.properties.layers.is_empty() {
            return Err(anyhow!("At least one layer filter must be specified"));
        }

        // Validate expressions in layer filters
        for (layer_name, layer_filter) in &self.properties.layers {
            if let Some(ref expr) = layer_filter.feature {
                validate_expression(expr)?;
            }
            if let Some(ref expr) = layer_filter.tag {
                validate_expression(expr)?;
            }
        }

        Ok(())
    }
}

/// Validate that an expression has the correct structure
fn validate_expression(expr: &Expression) -> Result<()> {
    match expr {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err(anyhow!("Expression array cannot be empty"));
            }

            // First element should be the operator
            if let Some(Value::String(op_str)) = arr.first() {
                Operator::from_str(op_str)?;
                // TODO: Add more specific validation for each operator's arguments
            } else {
                return Err(anyhow!(
                    "First element of expression must be operator string"
                ));
            }

            Ok(())
        }
        _ => Err(anyhow!("Expression must be an array")),
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

        let filter = FilterCollection::from_json(json_str).unwrap();
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
        assert!(Operator::from_str("$type").is_ok());
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

        let filter = FilterCollection::from_json(json_str).unwrap();
        let feature = &filter.features[0];
        let buildings_filter = &feature.properties.layers["buildings"];

        assert!(buildings_filter.feature.is_some());
        assert!(buildings_filter.tag.is_some());
    }

    #[test]
    fn test_invalid_feature_type() {
        let json_str = r#"
        {
          "type": "InvalidType",
          "features": []
        }
        "#;

        let result = FilterCollection::from_json(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_expression_array() {
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
                  "*": {
                    "feature": []
                  }
                }
              }
            }
          ]
        }
        "#;

        let result = FilterCollection::from_json(json_str);
        assert!(result.is_err());
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
                      ["==", "$type", "Point"],
                      ["==", "$type", "LineString"]
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

        let filter = FilterCollection::from_json(json_str).unwrap();
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

        let json_output = filter.to_json().unwrap();
        assert!(json_output.contains("FeatureCollection"));
        assert!(json_output.contains("test-filter"));

        // Verify we can parse it back
        let parsed = FilterCollection::from_json(&json_output).unwrap();
        assert_eq!(
            parsed.features[0].properties.id,
            Some("test-filter".to_string())
        );
    }
}
