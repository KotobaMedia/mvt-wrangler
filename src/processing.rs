use anyhow::Result;
use flate2::{Compression, write::GzEncoder};
use futures::{StreamExt, TryStreamExt as _, stream};
use indicatif::{ProgressBar, ProgressStyle};
use pmtiles::{AsyncPmTilesReader, TileCoord, TileId};
use rayon::prelude::*;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::task::JoinSet;

use crate::{filtering::data::CompiledFilterCollection, transform::transform_tile};

pub fn format_tile_coord(coords: &TileCoord) -> String {
    format!("{}/{}/{}", coords.z(), coords.x(), coords.y())
}

pub async fn process_tiles(
    pmtiles_path: &Path,
    mut out_pmt: pmtiles::PmTilesStreamWriter<std::fs::File>,
    tile_compression: pmtiles::Compression,
    filter_collection: Option<CompiledFilterCollection>,
) -> Result<()> {
    let in_pmt = Arc::new(AsyncPmTilesReader::new_with_path(pmtiles_path).await?);
    let entries = in_pmt.clone().entries().try_collect::<Vec<_>>().await?;

    let coords = entries
        .iter()
        .flat_map(|e| e.iter_coords())
        .collect::<Vec<_>>();
    let coords_count = coords.len();

    println!("Found {} tiles in the input archive", coords_count);

    let stream_in_pmt = in_pmt.clone();
    let (in_tx, in_rx) = flume::bounded::<(usize, TileId, Vec<u8>)>(1024);

    let mut tasks = JoinSet::new();
    // the async side of processing
    tasks.spawn(async move {
        let s = stream::iter(coords)
            .enumerate()
            .map(|(i, coord)| {
                let in_pmt = stream_in_pmt.clone();
                async move {
                    // Because we're enumerating tile coordinates, get_tile_decompress
                    // should never return a None, unless something is really wrong.
                    let data = in_pmt.get_tile_decompressed(coord).await?.unwrap();
                    Ok::<_, anyhow::Error>((i, coord, data.to_vec()))
                }
            })
            .buffered(num_cpus::get())
            .try_for_each(|item| {
                let tx = in_tx.clone();
                async move {
                    tx.send_async(item)
                        .await
                        .map_err(|e| anyhow::anyhow!("receiver dropped: {e}"))
                }
            });
        s.await?;
        Ok::<_, anyhow::Error>(())
    });
    // blocking processing
    let (out_tx, out_rx) = flume::bounded::<(usize, TileId, Vec<u8>)>(1024);
    tasks.spawn_blocking(move || {
        in_rx.into_iter().par_bridge().try_for_each_with(
            out_tx,
            |out_tx, (i, coord, input_data)| {
                let output_data = transform_tile_with_compression(
                    &coord.into(),
                    &input_data,
                    tile_compression,
                    filter_collection.as_ref(),
                )?;
                out_tx.send((i, coord, output_data))?;
                Ok::<_, anyhow::Error>(())
            },
        )?;

        Ok::<_, anyhow::Error>(())
    });
    tasks.spawn_blocking(move || {
        let bar = ProgressBar::new(coords_count as u64);
        bar.set_style(ProgressStyle::with_template(
            "[{msg}] {wide_bar} {pos:>7}/{len:7} {elapsed}/{duration}",
        )?);
        let mut next = 0usize;
        let mut buf = BTreeMap::new();
        while let Ok((i, coord, res)) = out_rx.recv() {
            bar.set_message(format_tile_coord(&coord.into()));
            buf.insert(i, (coord, res));
            while let Some(v) = buf.remove(&next) {
                let (coord, new_data) = v;
                out_pmt.add_tile(coord.into(), &new_data)?;
                bar.inc(1);
                next += 1;
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}

fn transform_tile_with_compression(
    coords: &TileCoord,
    data: &[u8],
    tile_compression: pmtiles::Compression,
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Vec<u8>> {
    // let duration = std::time::Instant::now();
    let bytes = transform_tile(coords, data, filter_collection)?;
    let new_data = match tile_compression {
        pmtiles::Compression::Gzip => {
            let mut compressed = Vec::new();
            {
                let mut encoder = GzEncoder::new(&mut compressed, Compression::default());
                std::io::Write::write_all(&mut encoder, &bytes)?;
                encoder.finish()?;
            }
            compressed
        }
        pmtiles::Compression::None => bytes,
        _ => {
            panic!("Unsupported tile compression: {:?}", tile_compression);
        }
    };
    // eprintln!(
    //     "! Transform tile duration: {}μs",
    //     duration.elapsed().as_micros()
    // );

    Ok(new_data)
}
