use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Fields, Schema};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use zstd::stream::read::Decoder;

const DEFAULT_INPUT_DIR: &str = "/lichess_db_eval";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_parquet";
const TARGET_FILE_MB: u64 = 100;
const DEFAULT_BATCH_ROWS: usize = 25_000;
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert lichess_db_eval.jsonl.zst to ~100MB parquet parts."
)]
struct Args {
    /// Input directory (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Specific input file path (optional). If omitted, scans input_dir for *.jsonl.zst.
    #[arg(long)]
    input_file: Option<String>,

    /// Output directory for parquet files (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Target parquet part size in MB (approximate, can overshoot by one batch)
    #[arg(long, default_value_t = TARGET_FILE_MB)]
    target_file_mb: u64,

    /// Arrow JSON read batch size (also write chunk size)
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Parquet zstd compression level
    #[arg(long, default_value_t = DEFAULT_ZSTD_LEVEL)]
    parquet_zstd_level: i32,
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
    part_index: usize,
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
    let input_file = resolve_input_file(&cwd, &input_dir, args.input_file.as_deref())?;

    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let schema = Arc::new(build_schema());
    let target_bytes = args.target_file_mb * 1024 * 1024;

    println!("Input file: {}", input_file.display());
    println!("Output dir: {}", output_dir.display());
    println!("Target part size: {} MB", args.target_file_mb);
    println!("Batch rows: {}", args.batch_rows);
    println!("Parquet zstd level: {}", args.parquet_zstd_level);

    let source = File::open(&input_file)
        .with_context(|| format!("failed to open input file {}", input_file.display()))?;
    let decoder = Decoder::new(source).context("failed to initialize zstd decoder")?;
    let buffered = BufReader::with_capacity(16 * 1024 * 1024, decoder);

    let mut json_reader = ReaderBuilder::new(schema.clone())
        .with_batch_size(args.batch_rows)
        .build(buffered)
        .context("failed to build Arrow JSON reader")?;

    let mut part_index = 0usize;
    let mut writer = open_parquet_writer(
        &output_dir,
        part_index,
        schema,
        args.parquet_zstd_level,
        &input_file,
    )?;

    let mut total_rows: u64 = 0;
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;

    while let Some(batch) = json_reader
        .next()
        .transpose()
        .context("failed while parsing JSON lines")?
    {
        if batch.num_rows() == 0 {
            continue;
        }

        writer
            .writer
            .write(&batch)
            .context("failed to write record batch to parquet")?;
        writer.rows_written += batch.num_rows() as u64;
        total_rows += batch.num_rows() as u64;

        if writer.approx_size_bytes() >= target_bytes {
            let next_part_index = writer.part_index + 1;
            let (path, size_bytes, rows) = writer.close()?;
            total_files += 1;
            total_bytes += size_bytes;
            println!(
                "Wrote {} (rows: {}, size: {:.2} MB)",
                path.display(),
                rows,
                size_bytes as f64 / (1024.0 * 1024.0)
            );

            part_index = next_part_index;
            writer = open_parquet_writer(
                &output_dir,
                part_index,
                build_schema().into(),
                args.parquet_zstd_level,
                &input_file,
            )?;
        }
    }

    if writer.rows_written > 0 {
        let (path, size_bytes, rows) = writer.close()?;
        total_files += 1;
        total_bytes += size_bytes;
        println!(
            "Wrote {} (rows: {}, size: {:.2} MB)",
            path.display(),
            rows,
            size_bytes as f64 / (1024.0 * 1024.0)
        );
    } else {
        // If the input was empty, don't keep an empty parquet file.
        fs::remove_file(&writer.path).with_context(|| {
            format!(
                "input produced zero rows; failed to remove empty file {}",
                writer.path.display()
            )
        })?;
    }

    println!(
        "Done. Total files: {}, total rows: {}, total parquet size: {:.2} GB",
        total_files,
        total_rows,
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}

fn resolve_user_path(cwd: &Path, user_path: &str) -> PathBuf {
    // On Windows, treat "/folder" notation as project-root-relative per task request.
    if cfg!(windows) && (user_path.starts_with('/') || user_path.starts_with('\\')) {
        return cwd.join(user_path.trim_start_matches(['/', '\\']));
    }

    let path = PathBuf::from(user_path);
    if path.is_absolute() {
        return path;
    }

    cwd.join(path)
}

fn resolve_input_file(cwd: &Path, input_dir: &Path, input_file: Option<&str>) -> Result<PathBuf> {
    if let Some(file) = input_file {
        let path = resolve_user_path(cwd, file);
        if !path.exists() {
            bail!("input file does not exist: {}", path.display());
        }
        return Ok(path);
    }

    if !input_dir.exists() {
        bail!("input directory does not exist: {}", input_dir.display());
    }

    let mut candidates = Vec::new();
    for entry in fs::read_dir(input_dir)
        .with_context(|| format!("failed to read input directory {}", input_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|x| x.to_str()) {
            if name.ends_with(".jsonl.zst") {
                candidates.push(path);
            }
        }
    }

    candidates.sort();
    let Some(selected) = candidates.into_iter().next() else {
        bail!(
            "no *.jsonl.zst input file found in directory {}",
            input_dir.display()
        );
    };
    Ok(selected)
}

fn open_parquet_writer(
    output_dir: &Path,
    part_index: usize,
    schema: Arc<Schema>,
    zstd_level: i32,
    input_file: &Path,
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
        .set_created_by(format!(
            "lc_eval2parquet (source: {})",
            input_file
                .file_name()
                .and_then(|x| x.to_str())
                .unwrap_or("unknown")
        ))
        .build();

    let writer = ArrowWriter::try_new(counting, schema, Some(props))
        .context("failed to create parquet ArrowWriter")?;

    Ok(ActiveWriter {
        part_index,
        path: output_path,
        writer,
        bytes_written,
        rows_written: 0,
    })
}

fn build_schema() -> Schema {
    let pv_struct = DataType::Struct(
        vec![
            Field::new("cp", DataType::Int64, true),
            Field::new("mate", DataType::Int64, true),
            Field::new("line", DataType::Utf8, false),
        ]
        .into(),
    );

    let eval_struct = DataType::Struct(
        vec![
            Field::new("knodes", DataType::Int64, false),
            Field::new("depth", DataType::Int64, false),
            Field::new(
                "pvs",
                DataType::List(Arc::new(Field::new("element", pv_struct, false))),
                false,
            ),
        ]
        .into(),
    );

    Schema::new(Fields::from(vec![
        Field::new("fen", DataType::Utf8, false),
        Field::new(
            "evals",
            DataType::List(Arc::new(Field::new("element", eval_struct, false))),
            false,
        ),
    ]))
}
