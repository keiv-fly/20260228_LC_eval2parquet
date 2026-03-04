use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::DataType;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

const DEFAULT_INPUT_DIR: &str = "lichess_eval_parquet_zobr_simplified";
const DEFAULT_BATCH_ROWS: usize = 50_000;
const DEFAULT_SAMPLE_PERCENT: f64 = 1.0;
const BOARD34_SIZE: usize = 34;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Print FENs with invalid castling flags from a sampled subset of simplified parquet rows."
)]
struct Args {
    /// Input parquet directory
    #[arg(long, default_value = DEFAULT_INPUT_DIR)]
    input_dir: String,

    /// Input reader batch size
    #[arg(long, default_value_t = DEFAULT_BATCH_ROWS)]
    batch_rows: usize,

    /// Percent of rows to scan, from the start of the dataset
    #[arg(long, default_value_t = DEFAULT_SAMPLE_PERCENT)]
    sample_percent: f64,
}

struct ScanStats {
    sampled_rows: u64,
    failed_rows: u64,
    invalid_castling_by_letter: BTreeMap<char, u64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    if !(0.0..=100.0).contains(&args.sample_percent) || args.sample_percent == 0.0 {
        bail!("--sample-percent must be in (0, 100]");
    }

    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let input_dir = resolve_user_path(&cwd, &args.input_dir);
    let input_files = list_parquet_files(&input_dir)?;
    let total_rows = total_rows_in_parquet_files(&input_files)?;

    if total_rows == 0 {
        bail!("input parquet dataset has zero rows");
    }
    let sample_rows = ((total_rows as f64) * (args.sample_percent / 100.0))
        .ceil()
        .max(1.0) as u64;
    let sample_rows = sample_rows.min(total_rows);

    eprintln!("Input dir: {}", input_dir.display());
    eprintln!("Input parquet files: {}", input_files.len());
    eprintln!("Total input rows: {}", total_rows);
    eprintln!(
        "Scanning first {} rows ({:.4}% requested)",
        sample_rows, args.sample_percent
    );

    let progress = build_progress_bar(sample_rows);
    let stats = scan_rows(&input_files, args.batch_rows, sample_rows, &progress)?;
    progress.finish_with_message("Scan complete");

    eprintln!(
        "Done. sampled_rows={} failed_rows={}",
        stats.sampled_rows, stats.failed_rows
    );
    eprintln!("Invalid castling letter distribution:");
    for line in castling_distribution_table_lines(&stats.invalid_castling_by_letter) {
        eprintln!("{}", line);
    }

    Ok(())
}

fn scan_rows(
    input_files: &[PathBuf],
    batch_rows: usize,
    target_rows: u64,
    progress: &ProgressBar,
) -> Result<ScanStats> {
    let mut sampled_rows = 0u64;
    let mut failed_rows = 0u64;
    let mut invalid_castling_by_letter = BTreeMap::new();

    for input_path in input_files {
        if sampled_rows >= target_rows {
            break;
        }

        let source = File::open(input_path)
            .with_context(|| format!("failed to open input file {}", input_path.display()))?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(source)
            .with_context(|| format!("failed to open parquet metadata {}", input_path.display()))?
            .with_batch_size(batch_rows)
            .build()
            .context("failed to build parquet reader")?;

        for batch in &mut reader {
            if sampled_rows >= target_rows {
                break;
            }
            let batch = batch.context("failed reading input record batch")?;
            process_batch(
                &batch,
                target_rows,
                &mut sampled_rows,
                &mut failed_rows,
                &mut invalid_castling_by_letter,
                progress,
            )?;
        }
    }

    Ok(ScanStats {
        sampled_rows,
        failed_rows,
        invalid_castling_by_letter,
    })
}

fn process_batch(
    batch: &RecordBatch,
    target_rows: u64,
    sampled_rows: &mut u64,
    failed_rows: &mut u64,
    invalid_castling_by_letter: &mut BTreeMap<char, u64>,
    progress: &ProgressBar,
) -> Result<()> {
    let schema = batch.schema();
    let fen_idx = schema
        .index_of("fen")
        .context("input parquet is missing `fen` column")?;
    let fen_col = batch.column(fen_idx);

    for row in 0..batch.num_rows() {
        if *sampled_rows >= target_rows {
            break;
        }

        *sampled_rows += 1;
        progress.inc(1);

        let fen = get_required_string_value(fen_col.as_ref(), row)
            .with_context(|| format!("invalid fen at row {}", row))?;

        if let Err(err) = fen_to_board34(&fen)
            && let Some(letter) = invalid_castling_letter_from_error(&err)
        {
            println!("{}", fen);
            *failed_rows += 1;
            *invalid_castling_by_letter.entry(letter).or_insert(0) += 1;
        }
    }

    Ok(())
}

fn invalid_castling_letter_from_error(err: &anyhow::Error) -> Option<char> {
    const PREFIX: &str = "invalid castling flag `";
    for cause in err.chain() {
        let msg = cause.to_string();
        let Some(start) = msg.find(PREFIX) else {
            continue;
        };
        let mut tail = msg[start + PREFIX.len()..].chars();
        let letter = tail.next()?;
        if tail.next() == Some('`') {
            return Some(letter);
        }
    }
    None
}

fn castling_distribution_table_lines(distribution: &BTreeMap<char, u64>) -> Vec<String> {
    let mut lines = vec![
        "invalid letters for castling | number of lines".to_string(),
        "-----------------------------+----------------".to_string(),
    ];

    if distribution.is_empty() {
        lines.push("- | 0".to_string());
    } else {
        for (letter, count) in distribution {
            lines.push(format!("{} | {}", letter, count));
        }
    }

    lines
}

fn fen_to_board34(fen: &str) -> Result<[u8; BOARD34_SIZE]> {
    let mut out = [0u8; BOARD34_SIZE];
    let parts: Vec<&str> = fen.split_whitespace().collect();
    if parts.len() < 4 {
        bail!("FEN must have at least 4 fields: {}", fen);
    }

    let mut squares = [0u8; 64];
    let ranks: Vec<&str> = parts[0].split('/').collect();
    if ranks.len() != 8 {
        bail!("FEN board must contain 8 ranks: {}", fen);
    }
    for (rank_from_top, rank_text) in ranks.iter().enumerate() {
        let board_rank = 7usize
            .checked_sub(rank_from_top)
            .context("invalid rank index while parsing FEN")?;
        let mut file_idx = 0usize;
        for ch in rank_text.chars() {
            if let Some(empty) = ch.to_digit(10) {
                let empty = usize::try_from(empty).context("digit in FEN rank overflowed usize")?;
                if empty == 0 || empty > 8 {
                    bail!("invalid empty-square count `{}` in FEN {}", ch, fen);
                }
                file_idx += empty;
                if file_idx > 8 {
                    bail!("FEN rank overflows 8 files: {}", fen);
                }
                continue;
            }

            if file_idx >= 8 {
                bail!("FEN rank overflows 8 files: {}", fen);
            }
            let sq = board_rank * 8 + file_idx;
            squares[sq] = piece_to_code(ch)?;
            file_idx += 1;
        }

        if file_idx != 8 {
            bail!("FEN rank does not fill 8 files: {}", fen);
        }
    }

    for i in 0..32 {
        out[i] = squares[2 * i] | (squares[2 * i + 1] << 4);
    }

    let mut state = 0u8;
    match parts[1] {
        "w" => {}
        "b" => state |= 1 << 0,
        side => bail!("invalid side-to-move `{}` in FEN {}", side, fen),
    }

    let castling = parts[2];
    if castling != "-" {
        for ch in castling.chars() {
            match ch {
                'K' => state |= 1 << 1,
                'Q' => state |= 1 << 2,
                'k' => state |= 1 << 3,
                'q' => state |= 1 << 4,
                other => bail!("invalid castling flag `{}` in FEN {}", other, fen),
            }
        }
    }
    out[32] = state;

    out[33] = if parts[3] == "-" {
        u8::MAX
    } else {
        parse_square(parts[3]).with_context(|| format!("invalid en-passant square in FEN {}", fen))?
    };

    Ok(out)
}

fn piece_to_code(ch: char) -> Result<u8> {
    match ch {
        'P' => Ok(1),
        'N' => Ok(2),
        'B' => Ok(3),
        'R' => Ok(4),
        'Q' => Ok(5),
        'K' => Ok(6),
        'p' => Ok(7),
        'n' => Ok(8),
        'b' => Ok(9),
        'r' => Ok(10),
        'q' => Ok(11),
        'k' => Ok(12),
        _ => bail!("invalid FEN piece code `{}`", ch),
    }
}

fn parse_square(square: &str) -> Result<u8> {
    let bytes = square.as_bytes();
    if bytes.len() != 2 {
        bail!("square must have length 2, got {}", square);
    }
    let file = bytes[0];
    let rank = bytes[1];
    if !(b'a'..=b'h').contains(&file) {
        bail!("square file must be a..h, got {}", square);
    }
    if !(b'1'..=b'8').contains(&rank) {
        bail!("square rank must be 1..8, got {}", square);
    }
    let file_idx = file - b'a';
    let rank_idx = rank - b'1';
    Ok(rank_idx * 8 + file_idx)
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
