use crate::filtering::EvaluationContext;
use crate::filtering::data::CompiledFilterCollection;
use crate::processing::format_tile_coord;
use anyhow::{Context, Result};
use geo::{BoundingRect, Coord, Intersects, MapCoords};
use geo_types::{Geometry, LineString, Polygon};
use geozero::ToGeo;
use geozero::mvt::{
    Tile,
    tile::{Feature, Value},
};
use pmtiles::TileCoord;
use prost::Message as _;
use std::collections::HashMap;

fn project_to_tile(geom: &Geometry<f64>, coords: &TileCoord, extent: u32) -> Geometry<f64> {
    let n = 2_f64.powi(coords.z() as i32);
    geom.map_coords(|Coord { x, y }| {
        // 1. fractional tile coords
        let x_frac = (x + 180.0) / 360.0 * n;
        let lat_rad = y.to_radians();
        let y_frac =
            (1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0 * n;
        // 2. local tile coords
        let x_local = (x_frac - coords.x() as f64) * extent as f64;
        let y_local = (y_frac - coords.y() as f64) * extent as f64;
        (x_local, y_local).into()
    })
}

fn bbox_intersects_tile(geom: &Geometry<f64>, extent: u32) -> bool {
    let max = extent as f64;
    geom.bounding_rect()
        .map(|rect| {
            let min = rect.min();
            let max_pt = rect.max();
            // Check for overlap on X and Y axes:
            // [min.x, max_pt.x] overlaps [0, max]
            min.x <= max && max_pt.x >= 0.0 &&
            // [min.y, max_pt.y] overlaps [0, max]
            min.y <= max && max_pt.y >= 0.0
        })
        // If there's no bounding rect (empty geom), treat as “no intersection”
        .unwrap_or(false)
}

fn tile_y_to_lat(y: f64, n: f64) -> f64 {
    let radians = std::f64::consts::PI * (1.0 - 2.0 * y / n);
    radians.sinh().atan().to_degrees()
}

fn tile_bounds(coords: &TileCoord) -> Geometry<f64> {
    let n = 2_f64.powi(coords.z() as i32);
    let x = coords.x() as f64;
    let y = coords.y() as f64;

    let west = x / n * 360.0 - 180.0;
    let east = (x + 1.0) / n * 360.0 - 180.0;
    let north = tile_y_to_lat(y, n);
    let south = tile_y_to_lat(y + 1.0, n);

    let ring = vec![
        (west, north),
        (east, north),
        (east, south),
        (west, south),
        (west, north),
    ];

    Geometry::Polygon(Polygon::new(LineString::from(ring), vec![]))
}

pub fn transform_tile(
    coords: &TileCoord,
    data: &[u8],
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Vec<u8>> {
    // decode the entire tile from bytes
    let mut tile = Tile::decode(data)
        .with_context(|| format!("Failed to decode MVT tile: {}", format_tile_coord(coords)))?;

    let filter_candidates = if let Some(fc) = filter_collection {
        let bounds = tile_bounds(coords);
        fc.get_filter_features(&bounds)
    } else {
        Vec::new()
    };

    for layer in &mut tile.layers {
        // if the filter_geometry is provided, we need to reproject it to tile coordinates
        // let's do a quick check to see if the filter intersects the tile
        // if it doesn't, set the filter_geometry to None
        // we do this per layer because the extent is set per layer.
        let extent = layer.extent.unwrap_or(4096);

        let filter_features = filter_candidates
            .iter()
            .filter_map(|f| {
                let mut feature = (*f).clone();
                let tile_geometry = project_to_tile(&feature.geometry, coords, extent);
                feature.geometry = tile_geometry;
                if bbox_intersects_tile(&feature.geometry, extent) {
                    Some(feature)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut keys: Vec<String> = Vec::with_capacity(layer.keys.len());
        let mut values: Vec<Value> = Vec::with_capacity(layer.values.len());
        let mut features: Vec<Feature> = Vec::with_capacity(layer.features.len());

        for feature in layer.features.drain(..) {
            // remove the feature from the layer
            let mut feature = feature;

            let mut tag_hashmap: HashMap<String, Value> = HashMap::new();
            for tags in feature.tags.chunks_exact(2) {
                let key_index = tags[0] as usize;
                let value_index = tags[1] as usize;

                // get the key and value from the layer
                let key = &layer.keys[key_index];
                let value = &layer.values[value_index];

                tag_hashmap.insert(key.to_string(), value.clone());
            }

            let feature_geom = feature.to_geo()?;
            let feature_geom_shape = match feature_geom {
                Geometry::Point(_) => "Point",
                Geometry::MultiPoint(_) => "Point",
                Geometry::LineString(_) => "LineString",
                Geometry::MultiLineString(_) => "LineString",
                Geometry::Polygon(_) => "Polygon",
                Geometry::MultiPolygon(_) => "Polygon",
                _ => "Unknown",
            };
            let intersecting_filters = filter_features
                .iter()
                .filter(|f| feature_geom.intersects(&f.geometry))
                .collect::<Vec<_>>();

            let mut ctx = EvaluationContext::new(&layer.name, tag_hashmap.clone())
                .with_geometry_type(feature_geom_shape);

            let mut should_remove_filter = false;
            for f in &intersecting_filters {
                if f.should_remove_feature(&ctx)? {
                    should_remove_filter = true;
                    break;
                }
            }
            if should_remove_filter {
                continue; // Skip this feature
            }

            let mut new_tags: Vec<u32> = Vec::with_capacity(feature.tags.len());
            for (key, value) in &tag_hashmap {
                ctx = ctx.with_current_key(key);
                let mut should_remove_tag = false;
                for f in &intersecting_filters {
                    if f.should_remove_tag(&ctx)? {
                        should_remove_tag = true;
                        break;
                    }
                }
                if should_remove_tag {
                    continue; // Skip this tag
                }

                // add the key and value to the new vectors
                let key_idx = {
                    if let Some(idx) = keys.iter().position(|k| k == key) {
                        idx
                    } else {
                        keys.push(key.clone());
                        keys.len() - 1
                    }
                };
                let value_idx = {
                    if let Some(idx) = values.iter().position(|v| v == value) {
                        idx
                    } else {
                        values.push(value.clone());
                        values.len() - 1
                    }
                };
                new_tags.push(key_idx as u32);
                new_tags.push(value_idx as u32);
            }

            feature.tags = new_tags;
            features.push(feature);
        }

        layer.keys = keys;
        layer.values = values;
        layer.features = features;
    }

    // re-encode to a fresh Vec<u8>
    Ok(tile.encode_to_vec()) // prost::Message::encode_to_vec
}
