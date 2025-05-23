use anyhow::Result;
use clap::Parser;
use pmtiles::async_reader::AsyncPmTilesReader;
use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
};

mod metadata;
mod processing;
mod transform;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Input MBTiles file
    input: PathBuf,
    /// Output MBTiles file (will be overwritten if exists)
    output: PathBuf,
    /// Optional GeoJSON file to filter features
    #[arg(short, long)]
    filter: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Remove any existing output
    if args.output.exists() {
        fs::remove_file(&args.output)?;
    }

    let pmtiles_path = args.input;
    if !pmtiles_path.exists() {
        panic!("Input file does not exist: {}", pmtiles_path.display());
    }

    // Validate filter file if provided
    if let Some(filter_path) = &args.filter {
        if !Path::new(filter_path).exists() {
            panic!("Filter file does not exist: {}", filter_path.display());
        }
    }

    // Open input and new output DBs
    let in_pmt = AsyncPmTilesReader::new_with_path(&pmtiles_path).await?;
    let mut out_conn = Connection::open(&args.output)?;

    // Create the minimal MBTiles schema
    out_conn.execute_batch(
        r#"
        PRAGMA synchronous = OFF;        -- no fsync at COMMIT  
        PRAGMA journal_mode = OFF;       -- no rollback journal  
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA temp_store = MEMORY;  
        PRAGMA cache_size = -200000;      -- ~200 MB cache (negative = KB)  
        PRAGMA mmap_size = 268435456;     -- 256 MB mmap window  
    
        CREATE TABLE metadata (name TEXT, value TEXT);
        CREATE TABLE tiles (
          zoom_level INTEGER,
          tile_column INTEGER,
          tile_row INTEGER,
          tile_data BLOB
        );
        CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);
    "#,
    )?;

    let metadata = in_pmt.get_metadata().await?;
    let header = in_pmt.get_header();
    if header.tile_type != pmtiles::TileType::Mvt {
        panic!("Unsupported tile type: {:?}", header.tile_type);
    }
    let tile_compression = header.tile_compression;

    // Insert metadata
    metadata::insert_metadata(&mut out_conn, &metadata, &header)?;

    processing::process_tiles(
        &pmtiles_path,
        out_conn,
        tile_compression,
        args.filter.as_deref(),
    )
    .await?;

    println!("✅ Wrote transformed tiles to {}", args.output.display());
    Ok(())
}
