use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray, UInt64Array};
use arrow_schema::DataType;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr_simplified";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_rocksdb";
const DEFAULT_BATCH_ROWS: usize = 50_000;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert simplified zobr parquet files to RocksDB keyed by zobr64."
)]
struct Args {
    /// Input parquet directory
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Output RocksDB directory
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Input reader batch size
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Remove output directory before writing
    #[arg(long)]
    overwrite: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredEval {
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: String,
    #[serde(rename = "move")]
    best_move: Option<String>,
}

#[derive(Default)]
struct Counters {
    input_rows: u64,
    updated_keys: u64,
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

    let input_files = list_parquet_files(&input_dir)?;
    let total_rows = total_rows_in_parquet_files(&input_files)?;
    prepare_output_dir(&output_dir, args.overwrite)?;
    let db = open_rocksdb(&output_dir)?;

    println!("Input dir: {}", input_dir.display());
    println!("Input parquet files: {}", input_files.len());
    println!("Total input rows: {}", total_rows);
    println!("Output DB dir: {}", output_dir.display());
    println!("Batch rows: {}", args.batch_rows);

    let progress = build_progress_bar(total_rows);
    let mut counters = Counters::default();

    for input_file in &input_files {
        process_input_file(&db, input_file, args.batch_rows, &progress, &mut counters)?;
    }

    db.flush().context("failed to flush RocksDB")?;
    progress.finish_with_message("Conversion complete");

    println!(
        "Done. input_rows={} updated_keys={}",
        counters.input_rows, counters.updated_keys
    );

    Ok(())
}

fn process_input_file(
    db: &DB,
    input_path: &Path,
    batch_rows: usize,
    progress: &ProgressBar,
    counters: &mut Counters,
) -> Result<()> {
    let source = File::open(input_path)
        .with_context(|| format!("failed to open input file {}", input_path.display()))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(source)
        .with_context(|| format!("failed to open parquet metadata {}", input_path.display()))?
        .with_batch_size(batch_rows)
        .build()
        .context("failed to build parquet reader")?;

    for batch in &mut reader {
        let batch = batch.context("failed reading input record batch")?;
        let rows_in_batch = batch.num_rows() as u64;
        process_batch(db, &batch, counters)?;
        progress.inc(rows_in_batch);
    }

    Ok(())
}

fn process_batch(db: &DB, batch: &RecordBatch, counters: &mut Counters) -> Result<()> {
    let schema = batch.schema();
    let zobr_idx = schema
        .index_of("zobr64")
        .context("input parquet is missing `zobr64` column")?;
    let eval_idx = schema
        .index_of("eval")
        .context("input parquet is missing `eval` column")?;
    let mate_idx = schema
        .index_of("mate")
        .context("input parquet is missing `mate` column")?;
    let depth_idx = schema
        .index_of("depth")
        .context("input parquet is missing `depth` column")?;
    let fen_idx = schema
        .index_of("fen")
        .context("input parquet is missing `fen` column")?;
    let move_idx = schema
        .index_of("move")
        .ok()
        .or_else(|| schema.index_of("first_move").ok());

    let zobr_col = batch.column(zobr_idx);
    let eval_col = batch.column(eval_idx);
    let mate_col = batch.column(mate_idx);
    let depth_col = batch.column(depth_idx);
    let fen_col = batch.column(fen_idx);
    let move_col = move_idx.map(|idx| batch.column(idx).as_ref());

    for row in 0..batch.num_rows() {
        counters.input_rows += 1;

        let zobr64 = get_u64_value(zobr_col.as_ref(), row)
            .with_context(|| format!("invalid zobr64 at row {}", row))?;
        let depth = get_required_i32_value(depth_col.as_ref(), row)
            .with_context(|| format!("invalid depth at row {}", row))?;
        let eval = get_optional_i32_value(eval_col.as_ref(), row)
            .with_context(|| format!("invalid eval at row {}", row))?;
        let mate = get_optional_i32_value(mate_col.as_ref(), row)
            .with_context(|| format!("invalid mate at row {}", row))?;
        let fen = get_required_string_value(fen_col.as_ref(), row)
            .with_context(|| format!("invalid fen at row {}", row))?;
        let best_move = if let Some(col) = move_col {
            get_optional_string_value(col, row)
                .with_context(|| format!("invalid move/first_move at row {}", row))?
        } else {
            None
        };

        let candidate = StoredEval {
            eval,
            mate,
            depth,
            fen,
            best_move,
        };

        write_key_value(db, zobr64, &candidate)?;
        counters.updated_keys += 1;
    }

    Ok(())
}

fn write_key_value(db: &DB, zobr64: u64, candidate: &StoredEval) -> Result<()> {
    let key = zobr64.to_be_bytes();
    let value = serde_json::to_vec(candidate).context("failed to encode RocksDB value JSON")?;
    db.put(key, value).context("rocksdb put failed")?;
    Ok(())
}

fn get_u64_value(array: &dyn Array, idx: usize) -> Result<u64> {
    match array.data_type() {
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("expected UInt64 array")?;
            if arr.is_null(idx) {
                bail!("unexpected NULL for non-nullable UInt64 value");
            }
            Ok(arr.value(idx))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("expected Int64 array")?;
            if arr.is_null(idx) {
                bail!("unexpected NULL for non-nullable Int64 value");
            }
            let value = arr.value(idx);
            u64::try_from(value).with_context(|| format!("value {} does not fit into u64", value))
        }
        other => bail!("expected UInt64 or Int64 array, got {:?}", other),
    }
}

fn get_required_i32_value(array: &dyn Array, idx: usize) -> Result<i32> {
    let Some(value) = get_optional_i32_value(array, idx)? else {
        bail!("unexpected NULL for required integer value");
    };
    Ok(value)
}

fn get_optional_i32_value(array: &dyn Array, idx: usize) -> Result<Option<i32>> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected Int32 array")?;
            if arr.is_null(idx) {
                Ok(None)
            } else {
                Ok(Some(arr.value(idx)))
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("expected Int64 array")?;
            if arr.is_null(idx) {
                Ok(None)
            } else {
                let value = arr.value(idx);
                let value = i32::try_from(value)
                    .with_context(|| format!("value {} does not fit into i32", value))?;
                Ok(Some(value))
            }
        }
        other => bail!("expected Int32 or Int64 array, got {:?}", other),
    }
}

fn get_required_string_value(array: &dyn Array, idx: usize) -> Result<String> {
    let Some(value) = get_optional_string_value(array, idx)? else {
        bail!("unexpected NULL for required string value");
    };
    Ok(value)
}

fn get_optional_string_value(array: &dyn Array, idx: usize) -> Result<Option<String>> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected StringArray")?;
            if arr.is_null(idx) {
                Ok(None)
            } else {
                Ok(Some(arr.value(idx).to_string()))
            }
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("expected LargeStringArray")?;
            if arr.is_null(idx) {
                Ok(None)
            } else {
                Ok(Some(arr.value(idx).to_string()))
            }
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .context("expected StringViewArray")?;
            if arr.is_null(idx) {
                Ok(None)
            } else {
                Ok(Some(arr.value(idx).to_string()))
            }
        }
        other => bail!(
            "expected Utf8, LargeUtf8, or Utf8View array, got {:?}",
            other
        ),
    }
}

fn prepare_output_dir(output_dir: &Path, overwrite: bool) -> Result<()> {
    if output_dir.exists() {
        if !output_dir.is_dir() {
            bail!(
                "output path exists and is not a directory: {}",
                output_dir.display()
            );
        }
        if overwrite {
            fs::remove_dir_all(output_dir)
                .with_context(|| format!("failed to remove {}", output_dir.display()))?;
            fs::create_dir_all(output_dir)
                .with_context(|| format!("failed to create {}", output_dir.display()))?;
            return Ok(());
        }

        if fs::read_dir(output_dir)
            .with_context(|| format!("failed to read {}", output_dir.display()))?
            .next()
            .is_some()
        {
            bail!(
                "output directory is not empty: {} (use --overwrite to recreate)",
                output_dir.display()
            );
        }
        return Ok(());
    }

    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    Ok(())
}

fn list_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        bail!("directory does not exist: {}", dir.display());
    }
    if !dir.is_dir() {
        bail!("path is not a directory: {}", dir.display());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
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
        bail!("no parquet files found in {}", dir.display());
    }
    Ok(files)
}

fn total_rows_in_parquet_files(files: &[PathBuf]) -> Result<u64> {
    let mut total_rows: u64 = 0;
    for path in files {
        let source = File::open(path)
            .with_context(|| format!("failed to open input file {}", path.display()))?;
        let reader = SerializedFileReader::new(source)
            .with_context(|| format!("failed to read parquet metadata {}", path.display()))?;
        let file_rows = u64::try_from(reader.metadata().file_metadata().num_rows())
            .context("parquet num_rows did not fit into u64")?;
        total_rows = total_rows
            .checked_add(file_rows)
            .context("total input rows overflowed u64")?;
    }
    Ok(total_rows)
}

fn open_rocksdb(output_dir: &Path) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.increase_parallelism(
        std::thread::available_parallelism()
            .map(|x| i32::try_from(x.get()).unwrap_or(1))
            .unwrap_or(1),
    );
    opts.optimize_level_style_compaction(256 * 1024 * 1024);

    DB::open(&opts, output_dir)
        .with_context(|| format!("failed to open RocksDB at {}", output_dir.display()))
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

fn build_progress_bar(total_rows: u64) -> ProgressBar {
    let progress = ProgressBar::new(total_rows);
    progress.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {percent:>3}% {pos}/{len} rows | elapsed: {elapsed_precise} | eta: {eta_precise}",
        )
        .expect("invalid progress bar template")
        .progress_chars("=> "),
    );
    progress
}
