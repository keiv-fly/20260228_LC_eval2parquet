use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::builder::{FixedSizeBinaryBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use shakmaty::fen::Fen;
use shakmaty::zobrist::{Zobrist64, Zobrist128, ZobristHash};
use shakmaty::{CastlingMode, Chess, EnPassantMode, PositionError};

const DEFAULT_INPUT_DIR: &str = "/lichess_eval_parquet";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_parquet_zobr";
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Append zobr64 and zobr128 columns to lichess parquet parts."
)]
struct Args {
    /// Input parquet directory (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Output parquet directory (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Reader batch size in rows
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Parquet zstd compression level for output
    #[arg(long, default_value_t = DEFAULT_ZSTD_LEVEL)]
    parquet_zstd_level: i32,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let output_dir = resolve_user_path(&cwd, &args.output_dir);

    if input_dir == output_dir {
        bail!("input and output directories must be different");
    }

    let parquet_files = list_parquet_files(&input_dir)?;
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    println!("Input dir: {}", input_dir.display());
    println!("Output dir: {}", output_dir.display());
    println!("Parquet files found: {}", parquet_files.len());
    println!("Batch rows: {}", args.batch_rows);
    println!("Parquet zstd level: {}", args.parquet_zstd_level);

    let progress = build_file_progress_bar(parquet_files.len() as u64);
    let mut total_rows: u64 = 0;
    for input_path in &parquet_files {
        let file_name = input_path
            .file_name()
            .and_then(|x| x.to_str())
            .context("invalid UTF-8 filename in input parquet path")?;
        let output_path = output_dir.join(file_name);

        let rows = process_file(
            input_path,
            &output_path,
            args.batch_rows,
            args.parquet_zstd_level,
        )
        .with_context(|| format!("failed processing {}", input_path.display()))?;

        total_rows += rows;
        progress.inc(1);
    }
    progress.finish_with_message("Hashing complete");

    println!(
        "Done. Processed {} parquet files, total rows: {}",
        parquet_files.len(),
        total_rows
    );

    Ok(())
}

fn process_file(
    input_path: &Path,
    output_path: &Path,
    batch_rows: usize,
    zstd_level: i32,
) -> Result<u64> {
    let source = File::open(input_path)
        .with_context(|| format!("failed to open input file {}", input_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(source).with_context(|| {
        format!(
            "failed to open parquet metadata for {}",
            input_path.display()
        )
    })?;
    let input_schema = builder.schema().clone();
    let output_schema = Arc::new(extended_schema(input_schema.as_ref()));
    let mut reader = builder
        .with_batch_size(batch_rows)
        .build()
        .context("failed to build parquet reader")?;

    let output = File::create(output_path)
        .with_context(|| format!("failed to create output file {}", output_path.display()))?;
    let compression = Compression::ZSTD(ZstdLevel::try_new(zstd_level).with_context(|| {
        format!(
            "invalid zstd level {}, expected valid parquet zstd range",
            zstd_level
        )
    })?);
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_created_by("lc_eval2parquet add_zobrist".to_string())
        .build();

    let mut writer = ArrowWriter::try_new(output, output_schema, Some(props))
        .context("failed to create parquet writer")?;

    let mut rows_written: u64 = 0;
    for batch in &mut reader {
        let batch = batch.context("failed to read record batch")?;
        let augmented = append_zobrist_columns(&batch).with_context(|| {
            format!(
                "failed generating zobrist columns for {}",
                input_path.display()
            )
        })?;
        rows_written += augmented.num_rows() as u64;
        writer
            .write(&augmented)
            .context("failed to write record batch")?;
    }

    writer
        .close()
        .context("failed to finalize parquet output")?;
    Ok(rows_written)
}

fn append_zobrist_columns(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fen_index = schema
        .index_of("fen")
        .context("input parquet is missing required `fen` column")?;
    let fen_array = batch
        .column(fen_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("`fen` column must be Utf8")?;

    let mut zobr64_builder = UInt64Builder::with_capacity(batch.num_rows());
    let mut zobr128_builder = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 16);

    for row in 0..batch.num_rows() {
        let fen = fen_array.value(row);
        let (z64, z128_be) = zobrist_from_fen(fen)
            .with_context(|| format!("invalid FEN at row {}: {}", row, fen))?;
        zobr64_builder.append_value(z64);
        zobr128_builder
            .append_value(&z128_be)
            .context("failed to append zobr128 bytes")?;
    }

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(zobr64_builder.finish()));
    columns.push(Arc::new(zobr128_builder.finish()));
    let output_schema = Arc::new(extended_schema(schema.as_ref()));
    RecordBatch::try_new(output_schema, columns).context("failed to construct output record batch")
}

fn zobrist_from_fen(fen: &str) -> Result<(u64, [u8; 16])> {
    let parsed =
        Fen::from_ascii(fen.as_bytes()).with_context(|| format!("could not parse FEN: {}", fen))?;
    let position: Chess = parsed
        .into_position(CastlingMode::Standard)
        .or_else(PositionError::ignore_invalid_castling_rights)
        .or_else(PositionError::ignore_invalid_ep_square)
        .or_else(PositionError::ignore_impossible_check)
        .or_else(PositionError::ignore_too_much_material)
        .context("could not build chess position from FEN")?;

    let z64: Zobrist64 = position.zobrist_hash(EnPassantMode::Legal);
    let z128: Zobrist128 = position.zobrist_hash(EnPassantMode::Legal);
    Ok((u64::from(z64), u128::from(z128).to_be_bytes()))
}

fn extended_schema(input_schema: &Schema) -> Schema {
    let mut fields: Vec<Field> = input_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("zobr64", DataType::UInt64, false));
    fields.push(Field::new("zobr128", DataType::FixedSizeBinary(16), false));
    Schema::new_with_metadata(fields, input_schema.metadata().clone())
}

fn list_parquet_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    if !input_dir.exists() {
        bail!("input directory does not exist: {}", input_dir.display());
    }
    if !input_dir.is_dir() {
        bail!("input path is not a directory: {}", input_dir.display());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(input_dir)
        .with_context(|| format!("failed to read input directory {}", input_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let is_parquet = path
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| x.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false);
        if is_parquet {
            files.push(path);
        }
    }

    files.sort();
    if files.is_empty() {
        bail!("no parquet files found in {}", input_dir.display());
    }

    Ok(files)
}

fn resolve_user_path(cwd: &Path, user_path: &str) -> PathBuf {
    // On Windows, treat "/folder" notation as project-root-relative.
    if cfg!(windows) && (user_path.starts_with('/') || user_path.starts_with('\\')) {
        return cwd.join(user_path.trim_start_matches(['/', '\\']));
    }

    let path = PathBuf::from(user_path);
    if path.is_absolute() {
        return path;
    }
    cwd.join(path)
}

fn build_file_progress_bar(total_files: u64) -> ProgressBar {
    let progress = ProgressBar::new(total_files);
    progress.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos}/{len} files | elapsed: {elapsed_precise} | eta: {eta_precise}",
        )
        .expect("invalid progress bar template")
        .progress_chars("=> "),
    );
    progress
}
