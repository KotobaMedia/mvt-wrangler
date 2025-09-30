# Filtering

Pass `--filter <file.geojson>` to apply rules from a GeoJSON `FeatureCollection`.

## Structure

- Root: `FeatureCollection` containing one or more filter `Feature`s.
- Each feature:
  - `geometry` defines the spatial mask.
  - `properties.layers` maps layer names (or `"*"`) to a LayerFilter.
  - Optional `id` and `description` for bookkeeping.

LayerFilter keys:

- `feature`: expression returning `true` drops the whole feature.
- `tag`: expression returning `true` removes that tag only.

## Expressions

Expressions follow Maplibre-style JSON arrays: `[operator, arg1, ...]`.

- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logic: `any`, `all`, `none`, `not`
- Membership: `in`, `not-in`
- Strings: `starts-with`, `ends-with`, `regex-match`, `regex-capture`
- Casting: `boolean`, `literal`, `string`
- Context: `tag` (property), `key` (current tag key), `type` (geometry type)

Common snippets: `["in", ["tag","kind"], ["literal", ["park","school"]]]`, `["starts-with", ["key"], "name:"]`.

## Evaluation

1. Only consider filters whose geometry intersects the tile feature.
2. Pick the matching layer entry or fall back to `"*"`.
3. Drop features when `feature` evaluates `true`.
4. Drop individual tags when `tag` evaluates `true` per key/value.

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
