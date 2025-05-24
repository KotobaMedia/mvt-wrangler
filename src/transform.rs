use crate::filtering::{data::FilterCollection, executor::FilterExecutor};
use crate::processing::TileCoordinates;
use anyhow::{Context, Result};
use geo::{BoundingRect, Coord, Intersects, MapCoords};
use geo_types::Geometry;
use geozero::{
    ToGeo,
    mvt::{
        Tile,
        tile::{Feature, Value},
    },
};
use prost::Message as _;
use regex::Regex;
use std::sync::OnceLock;

static NAME_MATCHER: OnceLock<Regex> = OnceLock::new();

fn project_to_tile(geom: &Geometry<f64>, coords: &TileCoordinates, extent: u32) -> Geometry<f64> {
    let n = 2_f64.powi(coords.z as i32);
    geom.map_coords(|Coord { x, y }| {
        // 1. fractional tile coords
        let x_frac = (x + 180.0) / 360.0 * n;
        let lat_rad = y.to_radians();
        let y_frac =
            (1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / std::f64::consts::PI) / 2.0 * n;
        // 2. local tile coords
        let x_local = (x_frac - coords.x as f64) * extent as f64;
        let y_local = (y_frac - coords.y as f64) * extent as f64;
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

pub fn transform_tile(
    coords: &TileCoordinates,
    data: &[u8],
    filter_geometry: Option<&Geometry>,
) -> Result<Vec<u8>> {
    // decode the entire tile from bytes
    let mut tile =
        Tile::decode(data).with_context(|| format!("Failed to decode MVT tile: {}", coords))?;

    // Get the regex, initializing it once
    let name_matcher = NAME_MATCHER.get_or_init(|| Regex::new(r"^name:?(.*)$").unwrap());

    for layer in &mut tile.layers {
        // if the filter_geometry is provided, we need to reproject it to tile coordinates
        // let's do a quick check to see if the filter intersects the tile
        // if it doesn't, set the filter_geometry to None
        // we do this per layer because the extent is set per layer.
        let extent = layer.extent.unwrap_or(4096);
        let filter_geometry = filter_geometry
            .map(|geom| {
                // check if the geometry intersects with the tile
                let tile_geometry = project_to_tile(geom, coords, extent);
                if bbox_intersects_tile(&tile_geometry, extent) {
                    Some(tile_geometry)
                } else {
                    None
                }
            })
            .flatten();

        let mut keys: Vec<String> = Vec::with_capacity(layer.keys.len());
        let mut values: Vec<Value> = Vec::with_capacity(layer.values.len());
        let mut features: Vec<Feature> = Vec::with_capacity(layer.features.len());

        for feature in layer.features.drain(..) {
            // remove the feature from the layer
            let mut feature = feature;

            // create the geometry
            if let Some(ref filter_geometry) = filter_geometry {
                let geometry = feature.to_geo()?;
                // check if the geometry intersects with the filter geometry
                if geometry.intersects(filter_geometry) {
                    // intersection filters:
                    // in this case, filter_geometry is a "sensitive area" geometry,
                    // so we want to remove some features, like boundaries and place names
                    if layer.name == "boundaries"
                        || layer.name == "roads"
                        || layer.name == "buildings"
                    {
                        // remove boundaries
                        continue;
                    } else if layer.name == "earth"
                        || layer.name == "water"
                        || layer.name == "pois"
                        || layer.name == "places"
                    {
                        match geometry {
                            Geometry::Point(_) => {
                                // remove points
                                continue;
                            }
                            _ => {} // keep the rest
                        }
                    }
                }
            }

            let mut new_tags: Vec<u32> = Vec::with_capacity(feature.tags.len());
            for tags in feature.tags.chunks_exact(2) {
                let key_index = tags[0] as usize;
                let value_index = tags[1] as usize;

                // get the key and value from the layer
                let key = &layer.keys[key_index];
                let value = &layer.values[value_index];

                // name tag filtering
                if let Some(captures) = name_matcher.captures(key) {
                    let lang = captures.get(1).map_or("", |m| m.as_str());
                    // empty means default `name` key
                    // if the language is not empty and not "ja", skip this tag
                    if !lang.is_empty() && lang != "ja" {
                        continue;
                    }
                }
                // pgf:name filtering
                if key.starts_with("pgf:name:") {
                    continue;
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

/// Transform tile with dynamic filtering using filter collection
pub fn transform_tile_dynamic(
    coords: &TileCoordinates,
    data: &[u8],
    filter_collection: Option<&FilterCollection>,
) -> Result<Vec<u8>> {
    // decode the entire tile from bytes
    let mut tile =
        Tile::decode(data).with_context(|| format!("Failed to decode MVT tile: {}", coords))?;

    for layer in &mut tile.layers {
        let extent = layer.extent.unwrap_or(4096);
        // Create filter executor if filters are provided
        let mut filter_executor = filter_collection.map(|fc| FilterExecutor::new(fc.clone()));

        let mut keys: Vec<String> = Vec::with_capacity(layer.keys.len());
        let mut values: Vec<Value> = Vec::with_capacity(layer.values.len());
        let mut features: Vec<Feature> = Vec::with_capacity(layer.features.len());

        for feature in layer.features.drain(..) {
            let mut feature = feature;

            // Convert feature to geometry for filtering
            let geometry = feature.to_geo()?;

            // Apply dynamic filtering if available
            if let Some(ref mut executor) = filter_executor {
                let (keep_feature, filtered_tags) = executor.apply_feature_filters(
                    &feature,
                    &geometry,
                    &layer.name,
                    &layer.keys,
                    &layer.values,
                )?;

                if !keep_feature {
                    continue; // Skip this feature
                }

                // Update feature tags with filtered ones
                feature.tags = filtered_tags;
            }

            // Rebuild the keys and values vectors with only the tags that remain
            let mut new_tags: Vec<u32> = Vec::with_capacity(feature.tags.len());
            for tags in feature.tags.chunks_exact(2) {
                let key_index = tags[0] as usize;
                let value_index = tags[1] as usize;

                if key_index >= layer.keys.len() || value_index >= layer.values.len() {
                    continue; // Skip invalid indices
                }

                let key = &layer.keys[key_index];
                let value = &layer.values[value_index];

                // Add the key and value to the new vectors if not already present
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
    Ok(tile.encode_to_vec())
}
