use anyhow::Result;
use async_compression::tokio::write::GzipEncoder;
use futures::TryStreamExt as _;
use indicatif::{ProgressBar, ProgressStyle};
use pmtiles::{AsyncPmTilesReader, MmapBackend, TileCoord};
use std::{path::Path, sync::Arc};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    task::JoinSet,
};

use crate::{filtering::data::CompiledFilterCollection, transform::transform_tile};

pub fn format_tile_coord(coord: &TileCoord) -> String {
    format!("{}/{}/{}", coord.z(), coord.x(), coord.y())
}

pub async fn process_tiles(
    pmtiles_path: &Path,
    mut out_pmt: pmtiles::PmTilesStreamWriter<std::fs::File>,
    tile_compression: pmtiles::Compression,
    filter_collection: Option<CompiledFilterCollection>,
) -> Result<()> {
    let in_pmt = Arc::new(AsyncPmTilesReader::new_with_path(pmtiles_path).await?);
    let entries = in_pmt.entries().try_collect::<Vec<_>>().await?;
    let tile_count = entries
        .iter()
        .map(|e| e.iter_coords().count())
        .sum::<usize>();
    let bar = ProgressBar::new(tile_count as u64);
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
            let filter_collection = filter_collection.clone();
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
                        filter_collection.as_ref(),
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
            while let Some((coords, new_data)) = ins_rx.blocking_recv() {
                out_pmt
                    .add_tile(coords, &new_data)
                    .expect("Failed to add tile");
                bar.inc(1);
            }
            out_pmt
                .finalize()
                .expect("Failed to finalize output PMTiles");
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
    ins_tx: &tokio::sync::mpsc::UnboundedSender<(TileCoord, Vec<u8>)>,
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<()> {
    let tiles = entry
        .iter_coords()
        .map(|tile_id| tile_id.into())
        .collect::<Vec<TileCoord>>();

    let coords = tiles
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("No tile coordinates found in entry: {:?}", entry))?;
    bar.set_message(format_tile_coord(&coords));

    let data = in_pmt.get_tile_decompressed(*coords).await?;
    let Some(data) = data else { return Ok(()) }; // skip empty tiles
    let new_data = transform_tile_async(&coords, &data, filter_collection).await?;

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

    for tile_coords in tiles {
        ins_tx.send((tile_coords, new_data.clone())).unwrap();
    }

    Ok(())
}

async fn transform_tile_async(
    coords: &TileCoord,
    data: &[u8],
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Vec<u8>> {
    let coords_c = coords.clone();
    let data_c = data.to_vec();
    let filter_collection_c = filter_collection.cloned();
    tokio::task::spawn_blocking(move || {
        transform_tile(&coords_c, &data_c, filter_collection_c.as_ref())
    })
    .await?
}
