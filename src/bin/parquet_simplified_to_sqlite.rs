use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow_array::{
    Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    UInt64Array,
};
use arrow_schema::DataType;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rusqlite::{Connection, TransactionBehavior, params};

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr_simplified";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_sqlite";
const DEFAULT_OUTPUT_FILE: &str = "lichess_eval.sqlite";
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_CACHE_SIZE_MB: usize = 512;
const DEFAULT_PAGE_SIZE: usize = 32 * 1024;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert simplified zobr parquet files to a single SQLite table keyed by zobr64."
)]
struct Args {
    /// Input parquet directory
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Output directory containing the SQLite database file
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// SQLite database filename created in --output-dir
    #[arg(long, default_value = DEFAULT_OUTPUT_FILE)]
    output_file: String,

    /// Input reader batch size
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// SQLite cache size in MB for bulk load (negative cache_size pragma)
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE_MB)]
    cache_size_mb: usize,

    /// SQLite page size in bytes (applies when creating a new DB file)
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    page_size: usize,

    /// Remove existing output db file before writing
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone)]
struct RowData {
    zobr64: u64,
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: String,
    first_move: Option<String>,
}

struct LoadStats {
    input_rows: u64,
    inserted_rows: u64,
    skipped_duplicate_keys: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let output_dir = resolve_user_path(&cwd, &args.output_dir);
    let output_file = PathBuf::from(&args.output_file);
    if output_file.components().count() != 1 {
        bail!("--output-file must be a filename, not a path");
    }

    let output_path = output_dir.join(&output_file);
    if input_dir == output_dir {
        bail!("input and output directories must be different");
    }

    let input_files = list_parquet_files(&input_dir)?;
    let total_rows = total_rows_in_parquet_files(&input_files)?;
    prepare_output_file(&output_dir, &output_path, args.overwrite)?;

    println!("Input dir: {}", input_dir.display());
    println!("Input parquet files: {}", input_files.len());
    println!("Total input rows: {}", total_rows);
    println!("Output SQLite file: {}", output_path.display());
    println!("Batch rows: {}", args.batch_rows);
    println!("SQLite page size: {}", args.page_size);
    println!("SQLite cache size: {} MB", args.cache_size_mb);

    let mut conn = Connection::open(&output_path)
        .with_context(|| format!("failed to open SQLite file {}", output_path.display()))?;

    configure_sqlite_for_bulk_load(&conn, args.page_size, args.cache_size_mb)?;
    create_schema(&conn)?;

    let progress = build_progress_bar(total_rows);
    let stats = load_with_single_transaction(&mut conn, &input_files, args.batch_rows, &progress)?;
    progress.finish_with_message("Conversion complete");

    finalize_sqlite_after_load(&conn)?;

    println!(
        "Done. input_rows={} inserted_rows={} skipped_duplicate_keys={}",
        stats.input_rows, stats.inserted_rows, stats.skipped_duplicate_keys
    );

    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    if args.cache_size_mb == 0 {
        bail!("--cache-size-mb must be > 0");
    }
    if args.page_size == 0 {
        bail!("--page-size must be > 0");
    }
    Ok(())
}

fn configure_sqlite_for_bulk_load(
    conn: &Connection,
    page_size: usize,
    cache_size_mb: usize,
) -> Result<()> {
    let cache_size_kib = cache_size_mb
        .checked_mul(1024)
        .context("cache-size-mb is too large")?;
    let pragmas = format!(
        "PRAGMA page_size = {page_size};
         PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -{cache_size_kib};
         PRAGMA foreign_keys = OFF;
         PRAGMA automatic_index = OFF;"
    );
    conn.execute_batch(&pragmas)
        .context("failed to apply SQLite bulk-load pragmas")
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS eval_by_zobr64 (
           zobr64     INTEGER PRIMARY KEY,
           eval       INTEGER,
           mate       INTEGER,
           depth      INTEGER NOT NULL,
           fen        TEXT NOT NULL,
           first_move TEXT
         ) STRICT, WITHOUT ROWID;",
    )
    .context("failed to create table `eval_by_zobr64`")
}

fn load_with_single_transaction(
    conn: &mut Connection,
    input_files: &[PathBuf],
    batch_rows: usize,
    progress: &ProgressBar,
) -> Result<LoadStats> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to begin transaction")?;
    let mut stmt = tx
        .prepare_cached(
            "INSERT INTO eval_by_zobr64 (zobr64, eval, mate, depth, fen, first_move)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .context("failed to prepare insert statement")?;

    let mut inserted_rows = 0u64;
    let mut skipped_duplicate_keys = 0u64;
    let mut pending: Option<RowData> = None;
    let mut last_input_key: Option<u64> = None;

    let input_rows = stream_rows(input_files, batch_rows, progress, |row| {
        if let Some(last) = last_input_key
            && row.zobr64 < last
        {
            bail!(
                "input is not sorted by zobr64 (saw {} after {}).",
                row.zobr64,
                last
            );
        }
        last_input_key = Some(row.zobr64);

        if let Some(previous) = pending.take() {
            if row.zobr64 == previous.zobr64 {
                // Keep only the latest row for duplicate zobr64 keys, mirroring KV semantics.
                skipped_duplicate_keys += 1;
                pending = Some(row);
            } else {
                insert_row(&mut stmt, &previous)?;
                inserted_rows += 1;
                pending = Some(row);
            }
        } else {
            pending = Some(row);
        }

        Ok(())
    })?;

    if let Some(last_row) = pending.take() {
        insert_row(&mut stmt, &last_row)?;
        inserted_rows += 1;
    }
    drop(stmt);

    tx.commit().context("failed to commit transaction")?;

    Ok(LoadStats {
        input_rows,
        inserted_rows,
        skipped_duplicate_keys,
    })
}

fn insert_row(stmt: &mut rusqlite::CachedStatement<'_>, row: &RowData) -> Result<()> {
    stmt.execute(params![
        u64_to_sqlite_i64_bits(row.zobr64),
        row.eval,
        row.mate,
        row.depth,
        row.fen,
        row.first_move
    ])
    .with_context(|| format!("failed inserting row for zobr64={}", row.zobr64))?;
    Ok(())
}

fn finalize_sqlite_after_load(conn: &Connection) -> Result<()> {
    conn.execute_batch("ANALYZE; PRAGMA optimize;")
        .context("failed to run SQLite post-load optimize steps")
}

fn stream_rows<F>(
    input_files: &[PathBuf],
    batch_rows: usize,
    progress: &ProgressBar,
    mut on_row: F,
) -> Result<u64>
where
    F: FnMut(RowData) -> Result<()>,
{
    let mut input_rows = 0u64;

    for input_path in input_files {
        let source = File::open(input_path)
            .with_context(|| format!("failed to open input file {}", input_path.display()))?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(source)
            .with_context(|| format!("failed to open parquet metadata {}", input_path.display()))?
            .with_batch_size(batch_rows)
            .build()
            .context("failed to build parquet reader")?;

        for batch in &mut reader {
            let batch = batch.context("failed reading input record batch")?;
            process_batch(&batch, &mut input_rows, &mut on_row)?;
            progress.inc(batch.num_rows() as u64);
        }
    }

    Ok(input_rows)
}

fn process_batch<F>(batch: &RecordBatch, input_rows: &mut u64, on_row: &mut F) -> Result<()>
where
    F: FnMut(RowData) -> Result<()>,
{
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
        *input_rows += 1;
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
        let first_move = if let Some(col) = move_col {
            get_optional_string_value(col, row)
                .with_context(|| format!("invalid move/first_move at row {}", row))?
        } else {
            None
        };

        on_row(RowData {
            zobr64,
            eval,
            mate,
            depth,
            fen,
            first_move,
        })?;
    }

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

fn u64_to_sqlite_i64_bits(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

fn prepare_output_file(output_dir: &Path, output_path: &Path, overwrite: bool) -> Result<()> {
    if output_dir.exists() && !output_dir.is_dir() {
        bail!(
            "output path exists and is not a directory: {}",
            output_dir.display()
        );
    }
    if !output_dir.exists() {
        fs::create_dir_all(output_dir)
            .with_context(|| format!("failed to create {}", output_dir.display()))?;
    }

    if output_path.exists() {
        if !output_path.is_file() {
            bail!(
                "output SQLite path exists and is not a file: {}",
                output_path.display()
            );
        }
        if overwrite {
            fs::remove_file(output_path)
                .with_context(|| format!("failed to remove {}", output_path.display()))?;
        } else {
            bail!(
                "output SQLite file already exists: {} (use --overwrite)",
                output_path.display()
            );
        }
    }
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
