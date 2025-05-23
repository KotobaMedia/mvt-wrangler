use anyhow::Result;
use async_compression::tokio::write::GzipEncoder;
use geo_types::Geometry;
use geozero::{ToGeo, geojson::GeoJson};
use indicatif::{ProgressBar, ProgressStyle};
use pmtiles::{MmapBackend, async_reader::AsyncPmTilesReader};
use rusqlite::params;
use std::path::Path;
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    task::JoinSet,
};

use crate::transform::transform_tile;

#[derive(Clone)]
pub struct TileCoordinates {
    pub z: u8,
    pub x: u64,
    pub y: u64,
}
impl std::fmt::Display for TileCoordinates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.z, self.x, self.y)
    }
}

pub async fn process_tiles(
    pmtiles_path: &Path,
    mut out_conn: rusqlite::Connection,
    tile_compression: pmtiles::Compression,
    filter_path: Option<&Path>,
) -> Result<()> {
    // Parse GeoJSON filter if provided
    let filter_geometry = if let Some(path) = filter_path {
        let geojson_str = fs::read_to_string(path).await?;
        let geojson = GeoJson(&geojson_str);
        let geometry: Geometry = geojson.to_geo()?;
        Some(geometry)
    } else {
        None
    };

    let in_pmt = AsyncPmTilesReader::new_with_path(pmtiles_path).await?;
    let entries = in_pmt.entries().await?;
    let bar = ProgressBar::new(entries.len() as u64);
    bar.set_style(ProgressStyle::with_template(
        "[{msg}] {wide_bar} {pos:>7}/{len:7} {elapsed}/{duration}",
    )?);

    let mut handles = JoinSet::new();

    let (entry_tx, entry_rx) = flume::unbounded();
    {
        handles.spawn(async move {
            for entry in entries {
                entry_tx.send_async(entry).await.unwrap();
            }
        });
    }

    let (ins_tx, mut ins_rx) = tokio::sync::mpsc::unbounded_channel();
    {
        for _ in 0..num_cpus::get() {
            let entry_rx = entry_rx.clone();
            let bar = bar.clone();
            let ins_tx = ins_tx.clone();
            let pmtiles_path = pmtiles_path.to_path_buf();
            let filter_geometry = filter_geometry.clone();
            handles.spawn(async move {
                let in_pmt = AsyncPmTilesReader::new_with_path(pmtiles_path)
                    .await
                    .unwrap();
                while let Ok(entry) = entry_rx.recv_async().await {
                    if let Err(e) = process_single_tile(
                        entry,
                        &in_pmt,
                        &bar,
                        tile_compression,
                        &ins_tx,
                        filter_geometry.as_ref(),
                    )
                    .await
                    {
                        eprintln!("Error processing tile: {}", e);
                    }
                }
            });
        }
    }
    drop(entry_rx);
    drop(ins_tx);

    {
        let bar = bar.clone();
        handles.spawn_blocking(move || {
            let txn = out_conn.transaction().unwrap();
            {
                let mut ins = txn
                    .prepare(
                        "
                        INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data)
                        VALUES (?1, ?2, ?3, ?4)
                        ",
                    )
                    .unwrap();

                while let Some((coords, new_data)) = ins_rx.blocking_recv() {
                    let TileCoordinates { z, x, y } = coords;
                    // Convert Y coordinate from XYZ to TMS format
                    let tms_y = (1u64 << z) - 1 - y;
                    ins.execute(params![z, x, tms_y, new_data]).unwrap();
                    bar.inc(1);
                }
            }
            txn.commit().unwrap();
        });
    }

    // Wait for all tasks to finish
    handles.join_all().await;

    bar.finish_and_clear();

    Ok(())
}

async fn process_single_tile(
    entry: pmtiles::DirEntry,
    in_pmt: &AsyncPmTilesReader<MmapBackend>,
    bar: &ProgressBar,
    tile_compression: pmtiles::Compression,
    ins_tx: &tokio::sync::mpsc::UnboundedSender<(TileCoordinates, Vec<u8>)>,
    filter_geometry: Option<&Geometry>,
) -> Result<()> {
    let (z, x, y) = entry.xyz();
    bar.set_message(format!("{}/{}/{}", z, x, y));
    let coords = TileCoordinates { z, x, y };
    let data = in_pmt.get_tile_decompressed(z, x, y).await?;
    let Some(data) = data else { return Ok(()) }; // skip empty tiles
    let new_data = transform_tile_async(&coords, &data, filter_geometry).await?;

    let new_data = match tile_compression {
        pmtiles::Compression::Gzip => {
            let mut compressed = Vec::new();
            let mut encoder = GzipEncoder::new(BufWriter::new(&mut compressed));
            encoder.write_all(&new_data).await?;
            encoder.shutdown().await?;
            compressed
        }
        pmtiles::Compression::None => new_data,
        _ => {
            panic!("Unsupported tile compression: {:?}", tile_compression);
        }
    };

    ins_tx.send((coords, new_data)).unwrap();
    Ok(())
}

async fn transform_tile_async(
    coords: &TileCoordinates,
    data: &[u8],
    filter_geometry: Option<&Geometry>,
) -> Result<Vec<u8>> {
    let coords_c = coords.clone();
    let data_c = data.to_vec();
    let filter_geometry_c = filter_geometry.cloned();
    tokio::task::spawn_blocking(move || {
        transform_tile(&coords_c, &data_c, filter_geometry_c.as_ref())
    })
    .await?
}
