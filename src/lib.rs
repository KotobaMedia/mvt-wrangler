use anyhow::Result;
use clap::Parser;
use pmtiles::AsyncPmTilesReader;
use std::{fs::File, path::PathBuf};
use tokio::fs;

mod filtering;
mod metadata;
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

    /// Name of the tileset (for PMTiles metadata)
    #[arg(long, short = 'n')]
    pub name: Option<String>,

    /// Description of the tileset (for PMTiles metadata)
    #[arg(long, short = 'N')]
    pub description: Option<String>,

    /// Attribution information for the tileset (for PMTiles metadata)
    #[arg(long, short = 'A')]
    pub attribution: Option<String>,
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
    let header = in_pmt.get_header();
    let in_metadata_str = in_pmt.get_metadata().await?;
    if header.tile_type != pmtiles::TileType::Mvt {
        panic!("Unsupported tile type: {:?}", header.tile_type);
    }
    // Build output metadata by merging input metadata with overrides
    let out_metadata_str = metadata::apply_overrides(
        &in_metadata_str,
        args.name.as_deref(),
        args.description.as_deref(),
        args.attribution.as_deref(),
    )?;
    let out_pmt = pmtiles::PmTilesWriter::new(header.tile_type)
        .tile_compression(header.tile_compression)
        .min_zoom(header.min_zoom)
        .max_zoom(header.max_zoom)
        .bounds(
            header.min_longitude,
            header.min_latitude,
            header.max_longitude,
            header.max_latitude,
        )
        .center_zoom(header.center_zoom)
        .center(header.center_longitude, header.center_latitude)
        .metadata(&out_metadata_str)
        .create(out_pmt_f)?;

    processing::process_tiles(&pmtiles_path, out_pmt, header.tile_compression, fc).await?;

    println!("✅ Wrote transformed tiles to {}", args.output.display());
    Ok(())
}
