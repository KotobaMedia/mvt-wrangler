use anyhow::{anyhow, Result};
use serde_json::Value;

/// Apply simple metadata overrides to a PMTiles metadata JSON string.
/// - Ensures the base is a JSON object (falls back to empty object if invalid)
/// - Sets `name`, `description`, `attribution` if provided
pub fn apply_overrides(
    base_json: &str,
    name: Option<&str>,
    description: Option<&str>,
    attribution: Option<&str>,
) -> Result<String> {
    // Parse base JSON; fall back to empty object if invalid or non-object
    let mut meta_value: Value = serde_json::from_str(base_json).unwrap_or_else(|_| {
        Value::Object(serde_json::Map::new())
    });
    if !meta_value.is_object() {
        meta_value = Value::Object(serde_json::Map::new());
    }

    if let Value::Object(obj) = &mut meta_value {
        if let Some(v) = name {
            obj.insert("name".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = description {
            obj.insert("description".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = attribution {
            obj.insert("attribution".to_string(), Value::String(v.to_string()));
        }
    } else {
        return Err(anyhow!("Metadata JSON is not an object"));
    }

    Ok(serde_json::to_string(&meta_value)?)
}

