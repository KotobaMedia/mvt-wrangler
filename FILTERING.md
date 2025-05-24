# Dynamic Filtering

You can filter features dynamically by passing a GeoJSON filter file.

## Overview

* Uses a valid GeoJSON `FeatureCollection` so any GeoJSON editor can load and modify the filters.
* Each `Feature` represents a filter rule scoped to its `geometry`. Only tile features intersecting that geometry are evaluated.
* Per-layer rules define whether to drop whole features or specific tags.

## File Structure

1. **Root**: a `FeatureCollection` containing one or more filter-defining `Feature`s.
2. **Feature**:

   * `geometry`: A GeoJSON geometry (e.g., `Polygon`) defining the spatial extent of the filter.
   * `properties`:

     * `id` (optional): Unique filter identifier.
     * `description` (optional): Human-readable summary.
     * `layers`: An object mapping **layer names** (or `"*"` for default) to `LayerFilter` objects.

### `LayerFilter` Object

Each LayerFilter can include:

* `feature`: An **expression** that, when it evaluates to `true`, removes the entire feature.
* `tag`: An **expression** that, when it evaluates to `true` for a given tag key/value, removes only that tag from the feature.

## Expressions

Expressions use a Maplibre Style Specification-inspired filter using JSON array syntax. The first element is an operator, followed by operands. Supported operators include:

| Category   | Operators                                                                                 |
| ---------- | ----------------------------------------------------------------------------------------- |
| Comparison | `==`, `!=`, `<`, `>`, `<=`, `>=`                                                          |
| Logical    | `any`, `all`, `none`, `not`                                                               |
| Membership | `in`, `not-in`                                                                            |
| String     | `starts-with`, `ends-with`, `regex-match`, `regex-capture`                                |
| Casting    | `boolean`, `literal`                                                                      |
| Context    | `tag` (feature property lookup), `key` (current tag key), `$type` (feature geometry type) |

### Common Patterns

* `["in", ["tag","kind"], ["literal", ["park","school"]]]`
* `["any", ["==","$type","Point"], ["==","$type","LineString"]]`
* `["starts-with", ["key"], "pgf:name:"]`
* `["regex-capture", ["key"], "^name:?(.*)$", 1]`

## Evaluation Flow

For each feature in the vector tile:

1. **Spatial Test**: Only evaluate filters if the feature geometry intersects the filter `geometry`.
2. **Layer Selection**: Use `properties.layers[layerName]`, or fall back to `properties.layers["*"]`. If the layer didn't match, no filters are applied and the feature is passed through to the output.
3. **Feature Filter**: If the `feature` expression exists and returns `true`, **drop** the entire feature.
4. **Tag Filter**: If the `tag` expression exists, evaluate it for each tag key/value pair; those returning `true` are **removed**.

## Example

```jsonc
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
```
