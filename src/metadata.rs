use anyhow::Result;
use rusqlite::{Connection, params};

pub fn insert_metadata(
    conn: &mut Connection,
    metadata: &str,
    header: &pmtiles::Header,
) -> Result<()> {
    // Insert raw metadata from PMTiles
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params!["json", metadata],
    )?;

    // Insert header fields as metadata following MBTiles spec
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params!["minzoom", header.min_zoom.to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params!["maxzoom", header.max_zoom.to_string()],
    )?;
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params![
            "bounds",
            format!(
                "{},{},{},{}",
                header.min_longitude,
                header.min_latitude,
                header.max_longitude,
                header.max_latitude
            )
        ],
    )?;
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params![
            "center",
            format!(
                "{},{},{}",
                header.center_longitude, header.center_latitude, header.center_zoom
            )
        ],
    )?;
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params!["format", "pbf"],
    )?;

    Ok(())
}
