use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use arrow_array::builder::{Int32Builder, StringBuilder, UInt64Builder};
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, ListArray, RecordBatch, StringArray,
    StringViewArray, StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr_sorted";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_parquet_zobr_simplified";
const DEFAULT_TARGET_FILE_MB: u64 = 100;
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Simplify sorted zobr parquet into columns zobr64, eval, mate, depth, fen, first_move."
)]
struct Args {
    /// Input parquet directory (must be globally sorted by zobr64)
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Output parquet directory
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Approximate output parquet file size in MB
    #[arg(long, default_value_t = DEFAULT_TARGET_FILE_MB)]
    target_file_mb: u64,

    /// Input reader batch size
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Parquet zstd compression level for output
    #[arg(long, default_value_t = DEFAULT_ZSTD_LEVEL)]
    parquet_zstd_level: i32,

    /// Remove output directory before writing
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone)]
struct CandidateRow {
    zobr64: u64,
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: String,
    first_move: Option<String>,
    fen_without_castling_key: String,
}

#[derive(Default)]
struct Counters {
    input_rows: u64,
    skipped_rows_without_eval: u64,
    output_rows: u64,
}

struct CountingWriter {
    inner: File,
    bytes_written: Arc<AtomicU64>,
}

impl CountingWriter {
    fn new(inner: File, bytes_written: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            bytes_written,
        }
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written
            .fetch_add(written as u64, Ordering::Relaxed);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct ActiveWriter {
    path: PathBuf,
    writer: ArrowWriter<CountingWriter>,
    bytes_written: Arc<AtomicU64>,
    rows_written: u64,
}

impl ActiveWriter {
    fn approx_size_bytes(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    fn close(self) -> Result<(PathBuf, u64, u64)> {
        self.writer
            .close()
            .context("failed to finalize parquet file")?;
        let bytes = self.bytes_written.load(Ordering::Relaxed);
        Ok((self.path, bytes, self.rows_written))
    }
}

struct OutputState {
    output_dir: PathBuf,
    schema: Arc<Schema>,
    target_bytes: u64,
    zstd_level: i32,
    part_index: usize,
    writer: Option<ActiveWriter>,
    total_files: u64,
    total_output_bytes: u64,
}

impl OutputState {
    fn new(output_dir: PathBuf, target_bytes: u64, zstd_level: i32) -> Self {
        Self {
            output_dir,
            schema: Arc::new(output_schema()),
            target_bytes,
            zstd_level,
            part_index: 0,
            writer: None,
            total_files: 0,
            total_output_bytes: 0,
        }
    }

    fn write_rows(&mut self, rows: &[CandidateRow]) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        if self.writer.is_none() {
            self.writer = Some(open_output_writer(
                &self.output_dir,
                self.part_index,
                self.schema.clone(),
                self.zstd_level,
            )?);
        }

        let batch = build_output_batch(self.schema.clone(), rows)?;
        let row_count = batch.num_rows() as u64;

        let writer = self.writer.as_mut().expect("writer initialized");
        writer
            .writer
            .write(&batch)
            .context("failed to write output batch")?;
        writer.rows_written += row_count;

        if writer.approx_size_bytes() >= self.target_bytes {
            self.rotate_writer()?;
        }

        Ok(row_count)
    }

    fn finish(&mut self) -> Result<()> {
        if self.writer.is_some() {
            self.rotate_writer()?;
        }
        Ok(())
    }

    fn rotate_writer(&mut self) -> Result<()> {
        let active = self.writer.take().expect("writer present");
        let (_path, bytes, _rows) = active.close()?;
        self.total_files += 1;
        self.total_output_bytes += bytes;
        self.part_index += 1;
        Ok(())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    if args.target_file_mb == 0 {
        bail!("--target-file-mb must be > 0");
    }

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let output_dir = resolve_user_path(&cwd, &args.output_dir);
    if input_dir == output_dir {
        bail!("input and output directories must be different");
    }

    let input_files = list_parquet_files(&input_dir)?;
    prepare_output_dir(&output_dir, args.overwrite)?;

    println!("Input dir: {}", input_dir.display());
    println!("Input parquet files: {}", input_files.len());
    println!("Output dir: {}", output_dir.display());
    println!("Target file size: {} MB", args.target_file_mb);
    println!("Batch rows: {}", args.batch_rows);
    println!("Parquet zstd level: {}", args.parquet_zstd_level);

    let progress = build_progress_bar(input_files.len() as u64);
    let mut output_state = OutputState::new(
        output_dir,
        args.target_file_mb * 1024 * 1024,
        args.parquet_zstd_level,
    );
    let mut counters = Counters::default();

    let mut current_zobr64: Option<u64> = None;
    let mut current_group: HashMap<String, CandidateRow> = HashMap::new();

    for input_file in &input_files {
        process_input_file(
            input_file,
            args.batch_rows,
            &mut current_zobr64,
            &mut current_group,
            &mut output_state,
            &mut counters,
        )?;
        progress.inc(1);
    }

    if let Some(zobr64) = current_zobr64 {
        counters.output_rows += flush_group(zobr64, &mut current_group, &mut output_state)?;
    }

    output_state.finish()?;
    progress.finish_with_message("Simplification complete");

    println!(
        "Done. Input rows: {}, skipped rows (no eval): {}, output rows: {}, output files: {}, total parquet size: {:.2} GB",
        counters.input_rows,
        counters.skipped_rows_without_eval,
        counters.output_rows,
        output_state.total_files,
        output_state.total_output_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}

fn process_input_file(
    input_path: &Path,
    batch_rows: usize,
    current_zobr64: &mut Option<u64>,
    current_group: &mut HashMap<String, CandidateRow>,
    output_state: &mut OutputState,
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
        process_batch(
            &batch,
            current_zobr64,
            current_group,
            output_state,
            counters,
        )?;
    }

    Ok(())
}

fn process_batch(
    batch: &RecordBatch,
    current_zobr64: &mut Option<u64>,
    current_group: &mut HashMap<String, CandidateRow>,
    output_state: &mut OutputState,
    counters: &mut Counters,
) -> Result<()> {
    let schema = batch.schema();
    let zobr_idx = schema
        .index_of("zobr64")
        .context("input parquet is missing `zobr64` column")?;
    let fen_idx = schema
        .index_of("fen")
        .context("input parquet is missing `fen` column")?;
    let evals_idx = schema
        .index_of("evals")
        .context("input parquet is missing `evals` column")?;

    let zobr_arr = batch
        .column(zobr_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("`zobr64` column must be UInt64")?;
    let fen_col = batch.column(fen_idx);
    let evals_arr = batch
        .column(evals_idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .context("`evals` column must be List")?;

    for row in 0..batch.num_rows() {
        counters.input_rows += 1;
        let zobr64 = zobr_arr.value(row);
        let fen = fen_str_from_array(fen_col.as_ref(), row)
            .context("`fen` column must be Utf8, LargeUtf8, or Utf8View")?;

        if current_zobr64.is_none() {
            *current_zobr64 = Some(zobr64);
        }

        if current_zobr64.is_some_and(|current| current != zobr64) {
            let previous = current_zobr64.expect("checked Some");
            counters.output_rows += flush_group(previous, current_group, output_state)?;
            *current_zobr64 = Some(zobr64);
        }

        let Some((eval, mate, depth, first_move)) = extract_best_eval_for_row(evals_arr, row)
            .with_context(|| format!("failed to extract eval from row {}", row))?
        else {
            counters.skipped_rows_without_eval += 1;
            continue;
        };

        let fen_without_castling_key = fen_without_castling_key(&fen);
        let candidate = CandidateRow {
            zobr64,
            eval,
            mate,
            depth,
            fen,
            first_move,
            fen_without_castling_key,
        };
        upsert_best_depth(current_group, candidate);
    }

    Ok(())
}

fn extract_best_eval_for_row(
    evals_arr: &ListArray,
    row_idx: usize,
) -> Result<Option<(Option<i32>, Option<i32>, i32, Option<String>)>> {
    if evals_arr.is_null(row_idx) {
        return Ok(None);
    }

    let evals_values = evals_arr.value(row_idx);
    let evals_struct = evals_values
        .as_any()
        .downcast_ref::<StructArray>()
        .context("`evals` list elements must be structs")?;
    if evals_struct.is_empty() {
        return Ok(None);
    }

    let depth_arr = evals_struct
        .column_by_name("depth")
        .context("eval struct is missing `depth` field")?;
    let pvs_arr = evals_struct
        .column_by_name("pvs")
        .context("eval struct is missing `pvs` field")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("`pvs` field must be List")?;

    let mut best: Option<(Option<i32>, Option<i32>, i32, Option<String>)> = None;
    for eval_idx in 0..evals_struct.len() {
        let Some(depth) = get_i32_value(depth_arr.as_ref(), eval_idx)
            .with_context(|| format!("invalid depth at eval index {}", eval_idx))?
        else {
            continue;
        };

        let Some((eval, mate, first_move)) = first_pv_eval(pvs_arr, eval_idx)
            .with_context(|| format!("invalid PV at eval index {}", eval_idx))?
        else {
            continue;
        };

        if best
            .as_ref()
            .is_none_or(|(_, _, best_depth, _)| depth > *best_depth)
        {
            best = Some((eval, mate, depth, first_move));
        }
    }

    Ok(best)
}

fn first_pv_eval(
    pvs_arr: &ListArray,
    eval_idx: usize,
) -> Result<Option<(Option<i32>, Option<i32>, Option<String>)>> {
    if pvs_arr.is_null(eval_idx) {
        return Ok(None);
    }

    let pvs_values = pvs_arr.value(eval_idx);
    let pvs_struct = pvs_values
        .as_any()
        .downcast_ref::<StructArray>()
        .context("`pvs` list elements must be structs")?;
    if pvs_struct.is_empty() {
        return Ok(None);
    }

    let cp_arr = pvs_struct
        .column_by_name("cp")
        .context("pv struct is missing `cp` field")?;
    let mate_arr = pvs_struct
        .column_by_name("mate")
        .context("pv struct is missing `mate` field")?;
    let line_arr = pvs_struct
        .column_by_name("line")
        .context("pv struct is missing `line` field")?;
    let first_idx = 0usize;

    let eval = get_i32_value(cp_arr.as_ref(), first_idx).context("invalid `cp` value")?;
    let mate = get_i32_value(mate_arr.as_ref(), first_idx).context("invalid `mate` value")?;
    let line = get_string_value(line_arr.as_ref(), first_idx).context("invalid `line` value")?;
    let first_move = line.and_then(|line| first_move_from_line(&line));
    Ok(Some((eval, mate, first_move)))
}

fn fen_str_from_array(array: &dyn Array, row: usize) -> Result<String> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("fen: expected StringArray")?;
            Ok(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .context("fen: expected LargeStringArray")?;
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .context("fen: expected StringViewArray")?;
            Ok(arr.value(row).to_string())
        }
        other => bail!(
            "fen column must be Utf8, LargeUtf8, or Utf8View, got {:?}",
            other
        ),
    }
}

fn get_i32_value(array: &dyn Array, idx: usize) -> Result<Option<i32>> {
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
                let value = i32::try_from(arr.value(idx))
                    .with_context(|| format!("value {} does not fit in i32", arr.value(idx)))?;
                Ok(Some(value))
            }
        }
        other => bail!("expected Int32 or Int64 array, got {:?}", other),
    }
}

fn get_string_value(array: &dyn Array, idx: usize) -> Result<Option<String>> {
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

fn first_move_from_line(line: &str) -> Option<String> {
    line.split_whitespace().next().map(|mv| mv.to_string())
}

fn upsert_best_depth(group: &mut HashMap<String, CandidateRow>, candidate: CandidateRow) {
    let key = candidate.fen_without_castling_key.clone();
    match group.get_mut(&key) {
        Some(existing) => {
            if candidate.depth > existing.depth {
                *existing = candidate;
            }
        }
        None => {
            group.insert(key, candidate);
        }
    }
}

fn flush_group(
    _zobr64: u64,
    group: &mut HashMap<String, CandidateRow>,
    output_state: &mut OutputState,
) -> Result<u64> {
    if group.is_empty() {
        return Ok(0);
    }

    let mut rows: Vec<CandidateRow> = group.drain().map(|(_, row)| row).collect();
    rows.sort_by(|a, b| a.fen.cmp(&b.fen));
    output_state.write_rows(&rows)
}

fn fen_without_castling_key(fen: &str) -> String {
    // Keep board, side-to-move, and en-passant square. Ignore castling rights for de-dup.
    let mut parts = fen.split_whitespace();
    let board = parts.next();
    let side = parts.next();
    let _castling = parts.next();
    let ep = parts.next();
    match (board, side, ep) {
        (Some(board), Some(side), Some(ep)) => format!("{board} {side} {ep}"),
        _ => format!("raw:{fen}"),
    }
}

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("zobr64", DataType::UInt64, false),
        Field::new("eval", DataType::Int32, true),
        Field::new("mate", DataType::Int32, true),
        Field::new("depth", DataType::Int32, false),
        Field::new("fen", DataType::Utf8, false),
        Field::new("first_move", DataType::Utf8, true),
    ])
}

fn build_output_batch(schema: Arc<Schema>, rows: &[CandidateRow]) -> Result<RecordBatch> {
    let mut zobr_builder = UInt64Builder::with_capacity(rows.len());
    let mut eval_builder = Int32Builder::with_capacity(rows.len());
    let mut mate_builder = Int32Builder::with_capacity(rows.len());
    let mut depth_builder = Int32Builder::with_capacity(rows.len());
    let total_fen_bytes: usize = rows.iter().map(|r| r.fen.len()).sum();
    let total_first_move_bytes: usize = rows
        .iter()
        .map(|r| r.first_move.as_deref().map_or(0, str::len))
        .sum();
    let mut fen_builder = StringBuilder::with_capacity(rows.len(), total_fen_bytes);
    let mut first_move_builder = StringBuilder::with_capacity(rows.len(), total_first_move_bytes);

    for row in rows {
        zobr_builder.append_value(row.zobr64);
        match row.eval {
            Some(v) => eval_builder.append_value(v),
            None => eval_builder.append_null(),
        }
        match row.mate {
            Some(v) => mate_builder.append_value(v),
            None => mate_builder.append_null(),
        }
        depth_builder.append_value(row.depth);
        fen_builder.append_value(&row.fen);
        match row.first_move.as_deref() {
            Some(v) => first_move_builder.append_value(v),
            None => first_move_builder.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(zobr_builder.finish()),
        Arc::new(eval_builder.finish()),
        Arc::new(mate_builder.finish()),
        Arc::new(depth_builder.finish()),
        Arc::new(fen_builder.finish()),
        Arc::new(first_move_builder.finish()),
    ];
    RecordBatch::try_new(schema, columns).context("failed to build output batch")
}

fn open_output_writer(
    output_dir: &Path,
    part_index: usize,
    schema: Arc<Schema>,
    zstd_level: i32,
) -> Result<ActiveWriter> {
    let output_path = output_dir.join(format!("part-{part_index:05}.parquet"));
    let file = File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;
    let bytes_written = Arc::new(AtomicU64::new(0));
    let counting = CountingWriter::new(file, bytes_written.clone());

    let compression = Compression::ZSTD(ZstdLevel::try_new(zstd_level).with_context(|| {
        format!(
            "invalid zstd level {}, expected valid parquet zstd range",
            zstd_level
        )
    })?);
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_created_by("lc_eval2parquet simplify_zobr_parquet".to_string())
        .build();
    let writer = ArrowWriter::try_new(counting, schema, Some(props))
        .context("failed to create output parquet writer")?;

    Ok(ActiveWriter {
        path: output_path,
        writer,
        bytes_written,
        rows_written: 0,
    })
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

        let existing_parts = list_parquet_files(output_dir).unwrap_or_default();
        if !existing_parts.is_empty() {
            bail!(
                "output directory already has parquet files ({} files): {}. Use --overwrite.",
                existing_parts.len(),
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

fn build_progress_bar(total_files: u64) -> ProgressBar {
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
