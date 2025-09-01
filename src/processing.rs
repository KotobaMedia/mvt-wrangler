use anyhow::Result;
use deadpool::managed::{Manager, Pool};
use flate2::{Compression, write::GzEncoder};
use futures::TryStreamExt as _;
use indicatif::{ProgressBar, ProgressStyle};
use pmtiles::{AsyncPmTilesReader, MmapBackend, TileCoord, TileId};
use rayon::prelude::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::task::JoinSet;

use crate::{filtering::data::CompiledFilterCollection, transform::transform_tile};

const QUEUE_CAPACITY: usize = 2_usize.pow(16);

pub fn format_tile_coord(coords: &TileCoord) -> String {
    format!("{}/{}/{}", coords.z(), coords.x(), coords.y())
}

struct PmTilesReaderManager {
    path: PathBuf,
}
impl Manager for PmTilesReaderManager {
    type Type = Arc<AsyncPmTilesReader<MmapBackend>>;
    type Error = anyhow::Error;
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let reader = AsyncPmTilesReader::new_with_path(&self.path).await?;
        Ok(Arc::new(reader))
    }
    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

pub async fn process_tiles(
    pmtiles_path: &Path,
    mut out_pmt: pmtiles::PmTilesStreamWriter<std::fs::File>,
    tile_compression: pmtiles::Compression,
    filter_collection: Option<CompiledFilterCollection>,
) -> Result<()> {
    let concurrency_limit = num_cpus::get();
    let in_pmt_manager = PmTilesReaderManager {
        path: pmtiles_path.to_path_buf(),
    };
    let in_pmt_pool: Pool<PmTilesReaderManager> = Pool::builder(in_pmt_manager)
        .max_size(concurrency_limit)
        .build()?;

    let in_pmt = in_pmt_pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("failed to get input PMTiles reader: {e}"))?;
    let entries = in_pmt.clone().entries().try_collect::<Vec<_>>().await?;
    drop(in_pmt); // release the reader back to the pool

    let coords = entries
        .iter()
        .flat_map(|e| e.iter_coords())
        .collect::<Vec<_>>();
    let coords_count = coords.len();

    println!("Found {} tiles in the input archive", coords_count);

    let (in_tx, in_rx) = flume::bounded::<(usize, TileId, Vec<u8>)>(QUEUE_CAPACITY);

    let mut tasks = JoinSet::new();
    // the async side of processing
    let stream_in_pmt_pool = in_pmt_pool.clone();
    let (coords_tx, coords_rx) = flume::unbounded::<(usize, TileId)>();
    tasks.spawn(async move {
        for (i, coord) in coords.into_iter().enumerate() {
            coords_tx.send((i, coord)).unwrap();
        }
        drop(coords_tx); // Close the sender when done
        Ok::<_, anyhow::Error>(())
    });
    for _ in 0..concurrency_limit {
        let in_pmt_pool = stream_in_pmt_pool.clone();
        let tx = in_tx.clone();
        let coords_rx = coords_rx.clone();
        tasks.spawn(async move {
            while let Ok((i, coord)) = coords_rx.recv() {
                let in_pmt = in_pmt_pool
                    .get()
                    .await
                    .map_err(|e| anyhow::anyhow!("Error getting tile decompressor: {}", e))?;
                // Because we're enumerating tile coordinates, get_tile_decompress
                // should never return a None, unless something is really wrong.
                let data = in_pmt.get_tile_decompressed(coord).await?.unwrap();
                let item = (i, coord, data.to_vec());

                tx.send_async(item).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    drop(coords_rx);

    // blocking processing
    let (out_tx, out_rx) = flume::bounded::<(usize, TileId, Vec<u8>)>(QUEUE_CAPACITY);

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
            "[{msg}] {wide_bar} {pos:>7}/{len:7} {elapsed}/{duration} {per_sec:7}",
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

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    Ok(())
}

fn transform_tile_with_compression(
    coords: &TileCoord,
    data: &[u8],
    tile_compression: pmtiles::Compression,
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Vec<u8>> {
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

    Ok(new_data)
}
