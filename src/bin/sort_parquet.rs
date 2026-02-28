use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr";
const DEFAULT_OUTPUT_DIR: &str = "lichess_eval_parquet_zobr_sorted";
const DEFAULT_SORT_COLUMN: &str = "zobr64";
const DEFAULT_TARGET_FILE_MB: u64 = 100;
const DEFAULT_BATCH_ROWS: usize = 20_000;
const DEFAULT_ZSTD_LEVEL: i32 = 3;
const DEFAULT_MEMORY_LIMIT_MB: usize = 2_048;
const DEFAULT_SORT_SPILL_RESERVATION_MB: usize = 32;
const DEFAULT_SORT_IN_PLACE_THRESHOLD_KB: usize = 256;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Sort parquet rows by zobr64 using DataFusion and write ~100MB parquet parts."
)]
struct Args {
    /// Input parquet directory
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Output parquet directory
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Column used for global sort
    #[arg(long, default_value = DEFAULT_SORT_COLUMN)]
    sort_column: String,

    /// Approximate output parquet file size in MB
    #[arg(long, default_value_t = DEFAULT_TARGET_FILE_MB)]
    target_file_mb: u64,

    /// DataFusion execution batch size
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Maximum DataFusion execution memory before spilling (MB)
    #[arg(long, default_value_t = DEFAULT_MEMORY_LIMIT_MB)]
    memory_limit_mb: usize,

    /// DataFusion spill directory (defaults to output_dir/.datafusion_spill)
    #[arg(long, default_value = "")]
    spill_dir: String,

    /// Parquet zstd compression level for output
    #[arg(long, default_value_t = DEFAULT_ZSTD_LEVEL)]
    parquet_zstd_level: i32,

    /// Remove output directory before writing
    #[arg(long)]
    overwrite: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    if args.target_file_mb == 0 {
        bail!("--target-file-mb must be > 0");
    }
    if args.memory_limit_mb == 0 {
        bail!("--memory-limit-mb must be > 0");
    }

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let output_dir = resolve_user_path(&cwd, &args.output_dir);

    if input_dir == output_dir {
        bail!("input and output directories must be different");
    }
    if !input_dir.exists() {
        bail!("input directory does not exist: {}", input_dir.display());
    }
    if !input_dir.is_dir() {
        bail!("input path is not a directory: {}", input_dir.display());
    }

    let parquet_files = list_parquet_files(&input_dir)?;
    if output_dir.exists() && args.overwrite {
        fs::remove_dir_all(&output_dir).with_context(|| {
            format!("failed to remove output directory {}", output_dir.display())
        })?;
    }
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let existing_parts = list_parquet_files(&output_dir).unwrap_or_default();
    if !existing_parts.is_empty() {
        bail!(
            "output directory already has parquet files ({} files): {}. Use --overwrite.",
            existing_parts.len(),
            output_dir.display()
        );
    }

    println!("Input dir: {}", input_dir.display());
    println!("Input parquet files: {}", parquet_files.len());
    println!("Output dir: {}", output_dir.display());
    println!("Sort column: {}", args.sort_column);
    println!("Target file size: {} MB", args.target_file_mb);
    println!("Batch rows: {}", args.batch_rows);
    println!("DataFusion memory limit: {} MB", args.memory_limit_mb);
    println!("Parquet zstd level: {}", args.parquet_zstd_level);

    let spill_dir = if args.spill_dir.trim().is_empty() {
        output_dir.join(".datafusion_spill")
    } else {
        resolve_user_path(&cwd, &args.spill_dir)
    };
    fs::create_dir_all(&spill_dir)
        .with_context(|| format!("failed to create spill directory {}", spill_dir.display()))?;
    println!("DataFusion spill dir: {}", spill_dir.display());

    let session_config = SessionConfig::new()
        .with_batch_size(args.batch_rows)
        .set_str("datafusion.execution.spill_compression", "zstd")
        .set_usize(
            "datafusion.execution.sort_spill_reservation_bytes",
            DEFAULT_SORT_SPILL_RESERVATION_MB * 1024 * 1024,
        )
        .set_usize(
            "datafusion.execution.sort_in_place_threshold_bytes",
            DEFAULT_SORT_IN_PLACE_THRESHOLD_KB * 1024,
        );
    let runtime = RuntimeEnvBuilder::new()
        .with_temp_file_path(&spill_dir)
        .with_memory_limit(args.memory_limit_mb * 1024 * 1024, 1.0)
        .build_arc()
        .context("failed to initialize DataFusion runtime")?;
    let ctx = SessionContext::new_with_config_rt(session_config, runtime);
    let input_glob = input_dir.join("*.parquet").to_string_lossy().to_string();

    let total_rows = count_rows(&ctx, &input_glob)
        .await
        .context("failed to precalculate total rows")?;
    if total_rows == 0 {
        bail!("input parquet dataset contains zero rows");
    }
    println!("Total rows (precalculated): {}", total_rows);

    let target_bytes = args.target_file_mb * 1024 * 1024;
    let progress = build_progress_bar(total_rows);

    let sorted_df = ctx
        .read_parquet(&input_glob, ParquetReadOptions::default())
        .await
        .with_context(|| format!("failed to read parquet files from {}", input_dir.display()))?
        .sort(vec![col(&args.sort_column).sort(true, true)])
        .with_context(|| format!("failed to build sort plan on column `{}`", args.sort_column))?;

    let mut stream = sorted_df
        .execute_stream()
        .await
        .context("failed to execute sorted DataFusion stream")?;

    let mut part_index: usize = 0;
    let mut writer: Option<ActiveWriter> = None;
    let mut output_schema: Option<Arc<Schema>> = None;
    let mut total_files: u64 = 0;
    let mut total_output_bytes: u64 = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.context("failed to read sorted record batch")?;
        if batch.num_rows() == 0 {
            continue;
        }

        if output_schema.is_none() {
            output_schema = Some(batch.schema());
        }

        if writer.is_none() {
            writer = Some(open_parquet_writer(
                &output_dir,
                part_index,
                output_schema.clone().expect("schema initialized"),
                args.parquet_zstd_level,
            )?);
        }

        let current_writer = writer.as_mut().expect("writer initialized");
        current_writer
            .writer
            .write(&batch)
            .context("failed to write sorted record batch")?;
        current_writer.rows_written += batch.num_rows() as u64;
        progress.inc(batch.num_rows() as u64);

        if current_writer.approx_size_bytes() >= target_bytes {
            let (_path, bytes, _rows) = writer.expect("writer initialized").close()?;
            writer = None;
            total_files += 1;
            total_output_bytes += bytes;
            part_index += 1;
        }
    }

    if let Some(active) = writer {
        let (_path, bytes, _rows) = active.close()?;
        total_files += 1;
        total_output_bytes += bytes;
    }

    progress.finish_with_message("Write complete");

    println!(
        "Done. Wrote {} parquet files, {} rows, total parquet size {:.2} GB",
        total_files,
        total_rows,
        total_output_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}

async fn count_rows(ctx: &SessionContext, input_glob: &str) -> Result<u64> {
    let row_count = ctx
        .read_parquet(input_glob, ParquetReadOptions::default())
        .await?
        .count()
        .await
        .context("count query failed")?;
    u64::try_from(row_count).context("row count does not fit in u64")
}

fn open_parquet_writer(
    output_dir: &Path,
    part_index: usize,
    schema: Arc<Schema>,
    zstd_level: i32,
) -> Result<ActiveWriter> {
    let output_path = output_dir.join(format!("part-{part_index:05}.parquet"));
    let out_file = File::create(&output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;
    let bytes_written = Arc::new(AtomicU64::new(0));
    let counting = CountingWriter::new(out_file, bytes_written.clone());

    let compression = Compression::ZSTD(ZstdLevel::try_new(zstd_level).with_context(|| {
        format!(
            "invalid zstd level {}, expected valid parquet zstd range",
            zstd_level
        )
    })?);
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_created_by("lc_eval2parquet sort_parquet".to_string())
        .build();

    let writer = ArrowWriter::try_new(counting, schema, Some(props))
        .context("failed to create parquet writer")?;

    Ok(ActiveWriter {
        path: output_path,
        writer,
        bytes_written,
        rows_written: 0,
    })
}

fn resolve_user_path(cwd: &Path, user_path: &str) -> PathBuf {
    let path = PathBuf::from(user_path);
    if path.is_absolute() {
        return path;
    }
    cwd.join(path)
}

fn list_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        bail!("directory does not exist: {}", dir.display());
    }
    if !dir.is_dir() {
        bail!("path is not a directory: {}", dir.display());
    }

    let mut files = Vec::new();
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
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
    Ok(files)
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
