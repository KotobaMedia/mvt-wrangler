use anyhow::Result;
use clap::Parser;
use pmtiles::AsyncPmTilesReader;
use std::{fs::File, path::PathBuf};
use tokio::fs;

mod filtering;
mod processing;
mod transform;

#[derive(Parser)]
#[command(author, version, about)]
pub struct Args {
    /// Input PMTiles file
    pub input: PathBuf,

    /// Output PMTiles file (will be overwritten if exists)
    pub output: PathBuf,

    /// Optional? GeoJSON file to filter features. Honestly, why are you using this tool if you don't want to filter?
    /// See FILTERING.md for details on the syntax.
    #[arg(short, long)]
    pub filter: Option<PathBuf>,
}

pub async fn run(args: Args) -> Result<()> {
    // Remove any existing output
    if args.output.exists() {
        fs::remove_file(&args.output).await?;
    }

    let pmtiles_path = args.input;
    if !pmtiles_path.exists() {
        panic!("Input file does not exist: {}", pmtiles_path.display());
    }

    // Validate filter file if provided
    let mut fc = None;
    if let Some(filter_path) = &args.filter {
        if !filter_path.exists() {
            panic!("Filter file does not exist: {}", filter_path.display());
        }
        let filter_str = fs::read_to_string(filter_path).await?;
        let filter_json: filtering::data::FilterCollection = serde_json::from_str(&filter_str)?;
        let compiled = filter_json.compile()?;
        fc = Some(compiled);
    }

    // Ensure output has pmtiles extension
    if args.output.extension().and_then(|s| s.to_str()) != Some("pmtiles") {
        panic!("Output file must have .pmtiles extension");
    }

    // Open input and new output DBs
    let in_pmt = AsyncPmTilesReader::new_with_path(&pmtiles_path).await?;
    let out_pmt_f = File::create(&args.output)?;
    let metadata = in_pmt.get_metadata().await?;
    let out_pmt = pmtiles::PmTilesWriter::new(pmtiles::TileType::Mvt)
        .metadata(&metadata)
        .create(out_pmt_f)?;

    let header = in_pmt.get_header();
    if header.tile_type != pmtiles::TileType::Mvt {
        panic!("Unsupported tile type: {:?}", header.tile_type);
    }
    let tile_compression = header.tile_compression;

    processing::process_tiles(&pmtiles_path, out_pmt, tile_compression, fc).await?;

    println!("✅ Wrote transformed tiles to {}", args.output.display());
    Ok(())
}
