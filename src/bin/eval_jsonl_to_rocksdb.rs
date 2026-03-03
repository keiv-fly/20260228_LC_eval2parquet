use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use shakmaty::fen::Fen;
use shakmaty::zobrist::{Zobrist64, ZobristHash};
use shakmaty::{CastlingMode, Chess, EnPassantMode, PositionError};
use zstd::stream::read::Decoder;

const DEFAULT_INPUT_DIR: &str = "/lichess_db_eval";
const DEFAULT_OUTPUT_DIR: &str = "/lichess_eval_rocksdb";
const DEFAULT_PROGRESS_EVERY: u64 = 10_000_000;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert lichess_db_eval.jsonl.zst to RocksDB keyed by zobr64."
)]
struct Args {
    /// Input directory (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Specific input file path (optional). If omitted, scans input_dir for *.jsonl.zst.
    #[arg(long)]
    input_file: Option<String>,

    /// Output RocksDB directory (project-root-relative if starts with / or \)
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,

    /// Remove output directory before writing
    #[arg(long)]
    overwrite: bool,

    /// Print counters every N input lines
    #[arg(long, default_value_t = DEFAULT_PROGRESS_EVERY)]
    progress_every: u64,
}

struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicU64>,
}

impl<R> CountingReader<R> {
    fn new(inner: R, bytes_read: Arc<AtomicU64>) -> Self {
        Self { inner, bytes_read }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}

#[derive(Debug, Deserialize)]
struct InputRow {
    fen: String,
    evals: Vec<InputEval>,
}

#[derive(Debug, Deserialize)]
struct InputEval {
    depth: i32,
    pvs: Vec<InputPv>,
}

#[derive(Debug, Deserialize)]
struct InputPv {
    cp: Option<i32>,
    mate: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredEval {
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: String,
}

#[derive(Default)]
struct Counters {
    lines_read: u64,
    rows_with_eval: u64,
    rows_missing_eval: u64,
    updated_keys: u64,
    kept_existing_keys: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.progress_every == 0 {
        bail!("--progress-every must be > 0");
    }

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let output_dir = resolve_user_path(&cwd, &args.output_dir);
    let input_file = resolve_input_file(&cwd, &input_dir, args.input_file.as_deref())?;

    prepare_output_dir(&output_dir, args.overwrite)?;
    let db = open_rocksdb(&output_dir)?;

    println!("Input file: {}", input_file.display());
    println!("Output DB dir: {}", output_dir.display());
    println!("Progress interval: {} lines", args.progress_every);

    let source = File::open(&input_file)
        .with_context(|| format!("failed to open input file {}", input_file.display()))?;
    let total_input_bytes = source
        .metadata()
        .with_context(|| format!("failed to read metadata for {}", input_file.display()))?
        .len();
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let counting_source = CountingReader::new(source, compressed_bytes_read.clone());
    let decoder = Decoder::new(counting_source).context("failed to initialize zstd decoder")?;
    let mut buffered = BufReader::with_capacity(16 * 1024 * 1024, decoder);
    let progress = build_progress_bar(total_input_bytes);

    let mut counters = Counters::default();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes = buffered
            .read_line(&mut line)
            .context("failed to read line from zstd stream")?;
        if bytes == 0 {
            break;
        }
        counters.lines_read += 1;

        if line.trim().is_empty() {
            continue;
        }

        let row: InputRow = serde_json::from_str(&line).with_context(|| {
            format!(
                "failed to parse JSON at line {} (possibly corrupted input)",
                counters.lines_read
            )
        })?;

        if let Some(candidate) = candidate_from_row(&row) {
            counters.rows_with_eval += 1;
            upsert_best_by_depth(&db, &candidate)
                .with_context(|| format!("failed DB upsert at line {}", counters.lines_read))
                .map(|updated| {
                    if updated {
                        counters.updated_keys += 1;
                    } else {
                        counters.kept_existing_keys += 1;
                    }
                })?;
        } else {
            counters.rows_missing_eval += 1;
        }

        progress.set_position(
            compressed_bytes_read
                .load(Ordering::Relaxed)
                .min(total_input_bytes),
        );

        if counters.lines_read % args.progress_every == 0 {
            println!(
                "lines={} with_eval={} missing_eval={} updated={} kept_existing={}",
                counters.lines_read,
                counters.rows_with_eval,
                counters.rows_missing_eval,
                counters.updated_keys,
                counters.kept_existing_keys
            );
        }
    }

    db.flush().context("failed to flush RocksDB")?;
    progress.set_position(total_input_bytes);
    progress.finish_with_message("Conversion complete");

    println!(
        "Done. lines={} with_eval={} missing_eval={} updated={} kept_existing={}",
        counters.lines_read,
        counters.rows_with_eval,
        counters.rows_missing_eval,
        counters.updated_keys,
        counters.kept_existing_keys
    );

    Ok(())
}

fn candidate_from_row(row: &InputRow) -> Option<StoredEval> {
    // Step 1: For each FEN row, keep the evaluation entry with the highest depth.
    let best_eval = row.evals.iter().max_by_key(|e| e.depth)?;
    let best_pv = best_eval.pvs.first()?;

    Some(StoredEval {
        eval: best_pv.cp,
        mate: best_pv.mate,
        depth: best_eval.depth,
        fen: row.fen.clone(),
    })
}

fn upsert_best_by_depth(db: &DB, candidate: &StoredEval) -> Result<bool> {
    // Step 2: Many rows may map to the same zobr64; keep only the deepest one.
    let zobr64 = zobrist64_from_fen(&candidate.fen)?;
    let key = zobr64.to_be_bytes();

    if let Some(existing_bytes) = db
        .get(key)
        .context("rocksdb get failed while checking existing key")?
    {
        let existing: StoredEval = serde_json::from_slice(&existing_bytes)
            .context("failed to decode existing RocksDB value as StoredEval JSON")?;
        if existing.depth >= candidate.depth {
            return Ok(false);
        }
    }

    let value = serde_json::to_vec(candidate).context("failed to encode StoredEval JSON")?;
    db.put(key, value).context("rocksdb put failed")?;
    Ok(true)
}

fn zobrist64_from_fen(fen: &str) -> Result<u64> {
    let parsed =
        Fen::from_ascii(fen.as_bytes()).with_context(|| format!("could not parse FEN: {}", fen))?;
    let position: Chess = parsed
        .into_position(CastlingMode::Standard)
        .or_else(PositionError::ignore_invalid_castling_rights)
        .or_else(PositionError::ignore_invalid_ep_square)
        .or_else(PositionError::ignore_impossible_check)
        .or_else(PositionError::ignore_too_much_material)
        .context("could not build chess position from FEN")?;
    let hash: Zobrist64 = position.zobrist_hash(EnPassantMode::Legal);
    Ok(u64::from(hash))
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
    if !input_dir.is_dir() {
        bail!("input path is not a directory: {}", input_dir.display());
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

fn open_rocksdb(output_dir: &Path) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.increase_parallelism(
        std::thread::available_parallelism()
            .map(|x| i32::try_from(x.get()).unwrap_or(1))
            .unwrap_or(1),
    );

    DB::open(&opts, output_dir)
        .with_context(|| format!("failed to open RocksDB at {}", output_dir.display()))
}

fn build_progress_bar(total_input_bytes: u64) -> ProgressBar {
    let progress = ProgressBar::new(total_input_bytes);
    progress.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {percent:>3}% {bytes}/{total_bytes} | elapsed: {elapsed_precise} | eta: {eta_precise}",
        )
        .expect("invalid progress bar template")
        .progress_chars("=> "),
    );
    progress
}
