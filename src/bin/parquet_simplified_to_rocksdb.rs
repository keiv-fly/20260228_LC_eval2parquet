use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow_array::{
    Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    UInt64Array,
};
use arrow_schema::DataType;
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rocksdb::{
    DB, DBCompressionType, IngestExternalFileOptions, Options, SstFileWriter, WriteBatch,
    WriteOptions,
};

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr_simplified";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_rocksdb";
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_WRITE_BATCH_ROWS: usize = 50_000;
const DEFAULT_SST_ROWS_PER_FILE: u64 = 2_000_000;

const VALUE_VERSION: u8 = 1;
const FLAG_HAS_EVAL: u8 = 1 << 0;
const FLAG_HAS_MATE: u8 = 1 << 1;
const FLAG_HAS_MOVE: u8 = 1 << 2;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum WritePath {
    /// Build SST files and ingest them into RocksDB (fast path for sorted input)
    Sst,
    /// Write with RocksDB WriteBatch
    Batch,
}

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

    /// Output write path (sst is fastest when input is sorted by zobr64)
    #[arg(long, value_enum, default_value_t = WritePath::Sst)]
    write_path: WritePath,

    /// Rows per RocksDB WriteBatch when using --write-path batch
    #[arg(long, default_value_t = DEFAULT_WRITE_BATCH_ROWS)]
    write_batch_rows: usize,

    /// Approximate rows per generated SST before ingesting when using --write-path sst
    #[arg(long, default_value_t = DEFAULT_SST_ROWS_PER_FILE)]
    sst_rows_per_file: u64,

    /// Remove output directory before writing
    #[arg(long)]
    overwrite: bool,
}

struct LoadStats {
    input_rows: u64,
    written_keys: u64,
    ingested_sst_files: u64,
}

struct ActiveSstWriter<'a> {
    path: PathBuf,
    writer: SstFileWriter<'a>,
    rows: u64,
}

struct SstIngestState<'a> {
    db: &'a DB,
    db_opts: &'a Options,
    ingest_opts: IngestExternalFileOptions,
    sst_tmp_dir: PathBuf,
    sst_rows_per_file: u64,
    part_index: usize,
    active: Option<ActiveSstWriter<'a>>,
    written_keys: u64,
    ingested_sst_files: u64,
}

impl<'a> SstIngestState<'a> {
    fn new(db: &'a DB, db_opts: &'a Options, sst_tmp_dir: PathBuf, sst_rows_per_file: u64) -> Self {
        let mut ingest_opts = IngestExternalFileOptions::default();
        ingest_opts.set_move_files(true);
        ingest_opts.set_snapshot_consistency(false);
        ingest_opts.set_allow_global_seqno(true);
        ingest_opts.set_allow_blocking_flush(true);

        Self {
            db,
            db_opts,
            ingest_opts,
            sst_tmp_dir,
            sst_rows_per_file,
            part_index: 0,
            active: None,
            written_keys: 0,
            ingested_sst_files: 0,
        }
    }

    fn write_pair(&mut self, zobr64: u64, value: &[u8]) -> Result<()> {
        self.ensure_writer()?;
        let key = zobr64.to_be_bytes();
        let active = self.active.as_mut().expect("writer must be open");
        active
            .writer
            .put(key, value)
            .context("failed to append key/value to SST file")?;
        active.rows += 1;
        self.written_keys += 1;

        if active.rows >= self.sst_rows_per_file {
            self.rotate_writer()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.rotate_writer()
    }

    fn ensure_writer(&mut self) -> Result<()> {
        if self.active.is_some() {
            return Ok(());
        }

        let path = self
            .sst_tmp_dir
            .join(format!("ingest-part-{:06}.sst", self.part_index));
        self.part_index += 1;

        let writer = SstFileWriter::create(self.db_opts);
        writer
            .open(&path)
            .with_context(|| format!("failed to open SST writer for {}", path.display()))?;

        self.active = Some(ActiveSstWriter {
            path,
            writer,
            rows: 0,
        });
        Ok(())
    }

    fn rotate_writer(&mut self) -> Result<()> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };

        active
            .writer
            .finish()
            .with_context(|| format!("failed to finalize SST file {}", active.path.display()))?;

        self.db
            .ingest_external_file_opts(&self.ingest_opts, vec![active.path.as_path()])
            .with_context(|| format!("failed to ingest {}", active.path.display()))?;
        self.ingested_sst_files += 1;

        if active.path.exists() {
            fs::remove_file(&active.path).with_context(|| {
                format!(
                    "failed to remove temporary SST file {}",
                    active.path.display()
                )
            })?;
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    if args.write_batch_rows == 0 {
        bail!("--write-batch-rows must be > 0");
    }
    if args.sst_rows_per_file == 0 {
        bail!("--sst-rows-per-file must be > 0");
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

    let db_opts = bulk_load_db_options();
    let db = DB::open(&db_opts, &output_dir)
        .with_context(|| format!("failed to open RocksDB at {}", output_dir.display()))?;

    println!("Input dir: {}", input_dir.display());
    println!("Input parquet files: {}", input_files.len());
    println!("Total input rows: {}", total_rows);
    println!("Output DB dir: {}", output_dir.display());
    println!("Batch rows: {}", args.batch_rows);
    println!("Write path: {:?}", args.write_path);

    let progress = build_progress_bar(total_rows);
    let stats = match args.write_path {
        WritePath::Sst => load_with_sst_ingest(
            &db,
            &db_opts,
            &output_dir,
            &input_files,
            args.batch_rows,
            args.sst_rows_per_file,
            &progress,
        )?,
        WritePath::Batch => load_with_write_batch(
            &db,
            &input_files,
            args.batch_rows,
            args.write_batch_rows,
            &progress,
        )?,
    };

    db.flush().context("failed to flush RocksDB")?;
    progress.finish_with_message("Conversion complete");

    println!(
        "Done. input_rows={} written_keys={} ingested_sst_files={}",
        stats.input_rows, stats.written_keys, stats.ingested_sst_files
    );

    Ok(())
}

fn load_with_sst_ingest(
    db: &DB,
    db_opts: &Options,
    output_dir: &Path,
    input_files: &[PathBuf],
    batch_rows: usize,
    sst_rows_per_file: u64,
    progress: &ProgressBar,
) -> Result<LoadStats> {
    let sst_tmp_dir = output_dir.join("_sst_ingest_tmp");
    if sst_tmp_dir.exists() {
        fs::remove_dir_all(&sst_tmp_dir)
            .with_context(|| format!("failed to remove {}", sst_tmp_dir.display()))?;
    }
    fs::create_dir_all(&sst_tmp_dir)
        .with_context(|| format!("failed to create {}", sst_tmp_dir.display()))?;

    let mut sst_state = SstIngestState::new(db, db_opts, sst_tmp_dir.clone(), sst_rows_per_file);
    let mut pending: Option<(u64, Vec<u8>)> = None;
    let mut last_input_key: Option<u64> = None;

    let input_rows = stream_rows(input_files, batch_rows, progress, |zobr64, encoded| {
        if let Some(last) = last_input_key
            && zobr64 < last
        {
            bail!(
                "input is not sorted by zobr64 (saw {} after {}). Use --write-path batch instead.",
                zobr64,
                last
            );
        }
        last_input_key = Some(zobr64);

        if let Some((pending_key, pending_value)) = pending.take() {
            if zobr64 == pending_key {
                // Keep only the last row for a duplicate key.
                pending = Some((zobr64, encoded));
            } else {
                sst_state.write_pair(pending_key, &pending_value)?;
                pending = Some((zobr64, encoded));
            }
        } else {
            pending = Some((zobr64, encoded));
        }

        Ok(())
    })?;

    if let Some((pending_key, pending_value)) = pending.take() {
        sst_state.write_pair(pending_key, &pending_value)?;
    }
    sst_state.finish()?;

    if sst_tmp_dir.exists() {
        fs::remove_dir_all(&sst_tmp_dir)
            .with_context(|| format!("failed to remove {}", sst_tmp_dir.display()))?;
    }

    Ok(LoadStats {
        input_rows,
        written_keys: sst_state.written_keys,
        ingested_sst_files: sst_state.ingested_sst_files,
    })
}

fn load_with_write_batch(
    db: &DB,
    input_files: &[PathBuf],
    batch_rows: usize,
    write_batch_rows: usize,
    progress: &ProgressBar,
) -> Result<LoadStats> {
    let mut write_opts = WriteOptions::new();
    write_opts.disable_wal(true);

    let batch_capacity_bytes = write_batch_rows.saturating_mul(128);
    let mut batch = WriteBatch::with_capacity_bytes(batch_capacity_bytes);
    let mut rows_in_batch = 0usize;
    let mut written_keys = 0u64;

    let input_rows = stream_rows(input_files, batch_rows, progress, |zobr64, encoded| {
        let key = zobr64.to_be_bytes();
        batch.put(key, &encoded);
        rows_in_batch += 1;
        written_keys += 1;

        if rows_in_batch >= write_batch_rows {
            let full_batch = std::mem::replace(
                &mut batch,
                WriteBatch::with_capacity_bytes(batch_capacity_bytes),
            );
            db.write_opt(full_batch, &write_opts)
                .context("failed to write RocksDB WriteBatch")?;
            rows_in_batch = 0;
        }
        Ok(())
    })?;

    if rows_in_batch > 0 {
        db.write_opt(batch, &write_opts)
            .context("failed to write final RocksDB WriteBatch")?;
    }

    Ok(LoadStats {
        input_rows,
        written_keys,
        ingested_sst_files: 0,
    })
}

fn stream_rows<F>(
    input_files: &[PathBuf],
    batch_rows: usize,
    progress: &ProgressBar,
    mut on_row: F,
) -> Result<u64>
where
    F: FnMut(u64, Vec<u8>) -> Result<()>,
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
    F: FnMut(u64, Vec<u8>) -> Result<()>,
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
        let best_move = if let Some(col) = move_col {
            get_optional_string_value(col, row)
                .with_context(|| format!("invalid move/first_move at row {}", row))?
        } else {
            None
        };

        let encoded = encode_value(eval, mate, depth, &fen, best_move.as_deref())?;
        on_row(zobr64, encoded)?;
    }

    Ok(())
}

fn encode_value(
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: &str,
    best_move: Option<&str>,
) -> Result<Vec<u8>> {
    let fen_bytes = fen.as_bytes();
    let fen_len = u16::try_from(fen_bytes.len())
        .with_context(|| format!("fen too long to encode: {} bytes", fen_bytes.len()))?;

    let move_bytes = best_move.map(str::as_bytes);
    if let Some(mv) = move_bytes
        && mv.len() > u8::MAX as usize
    {
        bail!("move too long to encode: {} bytes", mv.len());
    }

    let mut flags = 0u8;
    if eval.is_some() {
        flags |= FLAG_HAS_EVAL;
    }
    if mate.is_some() {
        flags |= FLAG_HAS_MATE;
    }
    if move_bytes.is_some() {
        flags |= FLAG_HAS_MOVE;
    }

    let mut out = Vec::with_capacity(
        2 + 4 + 4 + 4 + 2 + fen_bytes.len() + move_bytes.map_or(0, |mv| 1 + mv.len()),
    );
    out.push(VALUE_VERSION);
    out.push(flags);
    out.extend_from_slice(&depth.to_le_bytes());
    if let Some(v) = eval {
        out.extend_from_slice(&v.to_le_bytes());
    }
    if let Some(v) = mate {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&fen_len.to_le_bytes());
    out.extend_from_slice(fen_bytes);
    if let Some(mv) = move_bytes {
        out.push(mv.len() as u8);
        out.extend_from_slice(mv);
    }
    Ok(out)
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

fn bulk_load_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.prepare_for_bulk_load();
    opts.set_compression_type(DBCompressionType::None);
    opts.set_bottommost_compression_type(DBCompressionType::None);

    let parallelism = std::thread::available_parallelism()
        .map(|x| i32::try_from(x.get()).unwrap_or(1))
        .unwrap_or(1)
        .max(1);
    opts.increase_parallelism(parallelism);
    opts.set_max_background_jobs(parallelism);
    opts.set_max_subcompactions(u32::try_from(parallelism).unwrap_or(1));

    opts.set_write_buffer_size(256 * 1024 * 1024);
    opts.set_max_write_buffer_number(6);
    opts.set_target_file_size_base(256 * 1024 * 1024);
    opts.set_level_zero_file_num_compaction_trigger(32);
    opts.set_level_zero_slowdown_writes_trigger(256);
    opts.set_level_zero_stop_writes_trigger(1024);
    opts.set_bytes_per_sync(8 * 1024 * 1024);
    opts.set_compaction_readahead_size(2 * 1024 * 1024);
    opts.set_unordered_write(true);

    opts
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
