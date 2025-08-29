use anyhow::Result;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use mvt_wrangler::{Args, run};

/// Helper function to export PMTiles to GeoJSON using ogr2ogr
async fn export_pmtiles_to_geojson(
    pmtiles_path: &str,
    output_geojson_path: &str,
    layer_name: &str,
    zoom_level: u8,
) -> Result<()> {
    let output = Command::new("ogr2ogr")
        .arg("-f")
        .arg("GeoJSON")
        .arg("-oo")
        .arg(format!("ZOOM_LEVEL={}", zoom_level))
        .arg(output_geojson_path)
        .arg(pmtiles_path)
        .arg(layer_name)
        .output()?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "ogr2ogr failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(())
}

/// Helper function to verify that exported GeoJSON features don't contain a specific tag
async fn verify_tag_not_present(
    geojson_path: &str,
    tag_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojson_content = fs::read_to_string(geojson_path)?;
    let geojson: Value = serde_json::from_str(&geojson_content)?;

    if let Some(features) = geojson["features"].as_array() {
        for feature in features {
            if let Some(properties) = feature["properties"].as_object() {
                for (key, _) in properties {
                    if key == tag_name {
                        return Err(format!(
                            "Found {} tag in feature properties: {}",
                            tag_name, key
                        )
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_mvt_filtering_integration() {
    // Add filter GeoJSON file path
    let filter_geojson_path = "tests/fixtures/filter.geojson";

    // Add input PMTiles file path
    let input_pmtiles_path = "tests/fixtures/input.pmtiles";

    // Output PMTiles file path
    let output_path = "tests/fixtures/output.pmtiles";

    // Verify test fixtures exist (will be added later)
    assert!(
        Path::new(filter_geojson_path).exists(),
        "Filter GeoJSON file not found: {}",
        filter_geojson_path
    );

    assert!(
        Path::new(input_pmtiles_path).exists(),
        "Input PMTiles file not found: {}",
        input_pmtiles_path
    );

    // Run the main function with the test arguments
    let args = Args {
        input: PathBuf::from(input_pmtiles_path),
        output: PathBuf::from(output_path),
        filter: Some(PathBuf::from(filter_geojson_path)),
    };
    let result = run(args).await;
    assert!(result.is_ok(), "Integration test failed: {:?}", result);
    // Verify output file was created
    assert!(
        Path::new(output_path).exists(),
        "Output file not created: {}",
        output_path
    );

    // Now, let's verify the output file has performed the expected filtering
    let exported_geojson_path = "tests/fixtures/output_z10.geojson";

    // Export the PMTiles to GeoJSON for verification
    let export_result = export_pmtiles_to_geojson(
        output_path,
        exported_geojson_path,
        "earth", // layer name
        10,      // zoom level
    )
    .await;

    assert!(
        export_result.is_ok(),
        "Failed to export PMTiles to GeoJSON: {:?}",
        export_result
    );

    // Verify the exported GeoJSON file was created
    assert!(
        Path::new(exported_geojson_path).exists(),
        "Exported GeoJSON file not created: {}",
        exported_geojson_path
    );

    // Verify that the filtering worked - no name:fr tags should be present
    let verification_result = verify_tag_not_present(exported_geojson_path, "name:fr").await;
    assert!(
        verification_result.is_ok(),
        "Verification failed - name:fr tags found: {:?}",
        verification_result
    );
}
