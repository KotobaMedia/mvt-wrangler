# MVT Wrangler

[![Crates.io Version](https://img.shields.io/crates/v/mvt-wrangler)](https://crates.io/crates/mvt-wrangler)

A high-performance CLI tool for modifying Mapbox Vector Tile (MVT) archives with spatial and attribute filters.

We use this at KotobaMedia to make a Japan-oriented variant of [Protomaps' tiles generated from OpenStreetMap](https://maps.protomaps.com/builds/) without having to download and reprocess OpenStreetMap data from scratch.

## Install

- [Binaries](https://github.com/KotobaMedia/mvt-wrangler/releases/latest)
- Build: `cargo install --path .`

## Run

```bash
mvt-wrangler <input.pmtiles> <output.pmtiles> [options]
```

- `--filter/-f <geojson>`: Filter definition
- `--name/-n`, `--description/-N`, `--attribution/-A`: TileJSON metadata overrides

Examples:

```bash
mvt-wrangler input.pmtiles output.pmtiles
mvt-wrangler input.pmtiles output.pmtiles --filter filters.geojson
```

## Filtering

Filters are GeoJSON `FeatureCollection`s describing where to evaluate layer-specific expressions. See [FILTERING.md](FILTERING.md) for operators and structure.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
