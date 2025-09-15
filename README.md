# MVT Wrangler

A high-performance Rust tool for processing and transforming Mapbox Vector Tiles (MVT) with advanced filtering capabilities. Apply sophisticated spatial and attribute-based filters to slim down your tiles.

For example, I'm a big fan of [Protomaps' tiles generated from OpenStreetMap](https://maps.protomaps.com/builds/), but when serving tiles to Japanese customers, there are a couple things that need to be tweaked (sensitive areas, removing data that won't be shown anyways, etc..). Instead of downloading and reprocessing all the tiles from scratch, this tool takes in an already-built PMTiles archive, applies the desired filters, and outputs the filtered tiles.

## Overview

MVT Wrangler reads PMTiles files (containing MVT data) and outputs filtered PMTiles databases. It provides a powerful filtering system that allows you to:

- Remove entire features based on attributes or geometry types
- Strip specific tags/properties from features
- Apply filters spatially using GeoJSON geometries
- Process tiles efficiently with parallel processing

## Features

- **Spatial Filtering**: Filter features based on spatial intersection with GeoJSON geometries
- **Attribute Filtering**: Remove features or tags based on complex expressions
- **High Performance**: Parallel tile processing with optimized database writes
- **Flexible Expressions**: Maplibre-style filter expressions for complex filtering logic

## Installation

### Prerequisites

- Rust (if you don't have it, [rustup](https://rustup.rs/) is the easiest way to install)

### Building from Source

```bash
git clone https://github.com/KotobaMedia/mvt-wrangler.git
cd mvt-wrangler
cargo build --release
```

The compiled binary will be available at `target/release/mvt-wrangler`.

## Usage

### Basic Syntax

```bash
mvt-wrangler <input.pmtiles> <output.pmtiles> [--filter <filter.geojson>] [--name <string>] [--description <string>] [--attribution <string>]
```

### Arguments

- `input`: Path to the input PMTiles file
- `output`: Path for the output PMTiles file (will be overwritten if it exists)
- `--filter` / `-f`: Optional path to a GeoJSON filter file
- `--name` / `-n`: Set TileJSON `name`
- `--description` / `-N`: Set TileJSON `description`
- `--attribution` / `-A`: Set TileJSON `attribution`

### Examples

#### Simple Conversion (No Filtering)

```bash
mvt-wrangler input.pmtiles output.pmtiles
```

#### With Filtering

```bash
mvt-wrangler input.pmtiles output.pmtiles --filter my-filter.geojson
```

#### Override Metadata

```bash
# Set display name, description and attribution
mvt-wrangler input.pmtiles output.pmtiles \
  --name "My Tiles" \
  --description "Filtered tileset" \
  --attribution "© Me"
```

## Dynamic Filtering

The tool supports sophisticated filtering through GeoJSON filter files. See [FILTERING.md](FILTERING.md) for complete documentation.

### Quick Filter Example

Create a filter file `remove-parks.geojson`:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [-180, -90], [-180, 90], [180, 90], [180, -90], [-180, -90]
        ]]
      },
      "properties": {
        "id": "global-park-filter",
        "description": "Remove all park features worldwide",
        "layers": {
          "*": {
            "feature": [
              "in",
              ["tag", "kind"],
              ["literal", ["park", "recreation_ground"]]
            ]
          }
        }
      }
    }
  ]
}
```

Then apply it:

```bash
mvt-wrangler input.pmtiles clean-output.pmtiles --filter remove-parks.geojson
```

### Filter Capabilities

- **Spatial Scope**: Filters only apply to features intersecting the filter geometry
- **Layer Targeting**: Apply different rules to different MVT layers
- **Feature Removal**: Drop entire features based on conditions
- **Tag Stripping**: Remove specific properties/tags from features
- **Expression Language**: Rich filtering expressions supporting:
  - Comparison operators (`==`, `!=`, `<`, `>`, etc.)
  - Logical operators (`any`, `all`, `none`, `not`)
  - String operations (`starts-with`, `ends-with`, `regex-match`, `regex-capture`)
  - Membership tests (`in`)
  - Geometry type checks
  - And much more

## Filter Expression Examples

### Remove Features by Type
```json
["==", ["type"], "Point"]
```

### Remove Features with Specific Attributes
```json
["in", ["tag", "amenity"], ["literal", ["parking", "fuel"]]]
```

### Remove Tags by Key Pattern
```json
["starts-with", ["key"], "name:"]
```

### Complex Logical Conditions
```json
["all",
  ["==", ["type"], "LineString"],
  ["in", ["tag", "highway"], ["literal", ["residential", "tertiary"]]]
]
```

## Performance

The tool is optimized for processing large tile sets:

- **Parallel Processing**: Utilizes all CPU cores for tile transformation
- **Optimized Database**: Uses SQLite performance pragmas for fast writes
- **Memory Efficient**: Streams tiles without loading entire datasets
- **Progress Tracking**: Real-time progress indication for long operations

## Output Format

The output PMTiles file follows the [PMTiles specification](https://github.com/mapbox/pmtiles-spec).

This tool uses the [pmtiles-rs](https://github.com/stadiamaps/pmtiles-rs) library. When pmtiles-rs gains support for pmtiles writing, this tool will also switch to pmtiles output.

## Use Cases

### Data Cleaning
Remove unwanted features or properties from vector tiles:
```bash
# Remove all POI features globally
mvt-wrangler source.pmtiles clean.pmtiles --filter remove-pois.geojson
```

### Privacy Compliance
Strip personally identifiable information:
```bash
# Remove all name tags starting with personal prefixes
mvt-wrangler source.pmtiles anonymized.pmtiles --filter remove-personal-names.geojson
```

### Data Optimization
Reduce tile size by removing unnecessary attributes:
```bash
# Keep only essential properties for rendering
mvt-wrangler full.pmtiles minimal.pmtiles --filter essential-only.geojson
```

## Requirements

- Input files must be valid PMTiles with MVT tile type
- Filter files must be valid GeoJSON FeatureCollections

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
