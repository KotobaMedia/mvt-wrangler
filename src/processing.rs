use anyhow::Result;
use async_compression::tokio::write::GzipEncoder;
use deadpool::managed::{Manager, Pool};
use futures::{StreamExt, TryStreamExt as _, stream};
use indicatif::{ProgressBar, ProgressStyle};
use pmtiles::{AsyncPmTilesReader, MmapBackend, TileCoord};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task,
};

use crate::{filtering::data::CompiledFilterCollection, transform::transform_tile};

struct PmTilesReaderManager {
    path: PathBuf,
}
impl Manager for PmTilesReaderManager {
    type Type = AsyncPmTilesReader<MmapBackend>;
    type Error = anyhow::Error;
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let reader = AsyncPmTilesReader::new_with_path(&self.path).await?;
        Ok(reader)
    }
    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

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

    let concurrency_limit = num_cpus::get();

    let in_pmt_manager = PmTilesReaderManager {
        path: pmtiles_path.to_path_buf(),
    };
    let in_pmt_pool: Pool<PmTilesReaderManager> = Pool::builder(in_pmt_manager)
        .max_size(concurrency_limit)
        .build()?;

    let (out_tx, mut out_rx) =
        mpsc::channel::<Option<(TileCoord, Vec<u8>)>>(concurrency_limit * 10);

    // Create a stream of individual tiles and process them with buffered parallelism while maintaining order
    let tile_stream = stream::iter(entries)
        .flat_map(|entry| {
            let coords = entry.iter_coords().collect::<Vec<_>>();
            stream::iter(coords)
        })
        .map(|coords| {
            let bar = bar.clone();
            let filter_collection = filter_collection.clone();
            let in_pmt_pool = in_pmt_pool.clone();

            async move {
                let in_pmt = in_pmt_pool.get().await.unwrap();

                let result = process_single_tile_coords(
                    coords.into(),
                    &in_pmt,
                    &bar,
                    tile_compression,
                    filter_collection.as_ref(),
                )
                .await;

                result
            }
        })
        .buffered(concurrency_limit);

    let out_bar = bar.clone();
    let out_handle = task::spawn_blocking(move || -> Result<()> {
        while let Some(msg) = out_rx.blocking_recv() {
            if let Some((coords, new_data)) = msg {
                out_pmt.add_tile(coords, &new_data)?;
                out_bar.inc(1);
            }
        }

        out_pmt.finalize()?;
        Ok(())
    });

    // Process the stream and then drop the sender to signal completion
    tile_stream
        .for_each(|result| {
            let result = result.unwrap();
            let out_tx = out_tx.clone();
            async move { out_tx.send(result).await.unwrap() }
        })
        .await;

    // Drop the sender to signal completion to the receiver
    drop(out_tx);

    let out = out_handle.await?;
    out?;
    bar.finish_and_clear();

    Ok(())
}

async fn process_single_tile_coords(
    coords: TileCoord,
    in_pmt: &AsyncPmTilesReader<MmapBackend>,
    bar: &ProgressBar,
    tile_compression: pmtiles::Compression,
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Option<(TileCoord, Vec<u8>)>> {
    bar.set_message(format_tile_coord(&coords));

    let data = in_pmt.get_tile_decompressed(coords).await?;

    let Some(data) = data else { return Ok(None) }; // skip empty tiles

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

    Ok(Some((coords, new_data)))
}

async fn transform_tile_async(
    coords: &TileCoord,
    data: &[u8],
    filter_collection: Option<&CompiledFilterCollection>,
) -> Result<Vec<u8>> {
    let coords_c = coords.clone();
    let data_c = data.to_vec();
    let filter_collection_c = filter_collection.cloned();

    let outer_duration = Instant::now();
    let (result, inner_duration) = tokio::task::spawn_blocking(move || {
        let tile_duration = Instant::now();
        let x = transform_tile(&coords_c, &data_c, filter_collection_c.as_ref());
        (x, tile_duration.elapsed())
    })
    .await?;
    eprintln!(
        "! Transform tile duration: {}μs, outer duration: {}μs",
        inner_duration.as_micros(),
        outer_duration.elapsed().as_micros()
    );

    result
}
