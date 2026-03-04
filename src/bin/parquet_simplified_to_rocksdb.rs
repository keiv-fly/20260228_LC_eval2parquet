use std::collections::BTreeMap;
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

const BOARD34_SIZE: usize = 34;
const ENTRY_SIZE: usize = 39;

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
    skipped_rows: u64,
    invalid_castling_by_letter: BTreeMap<char, u64>,
    written_keys: u64,
    ingested_sst_files: u64,
}

struct StreamStats {
    input_rows: u64,
    skipped_rows: u64,
    invalid_castling_by_letter: BTreeMap<char, u64>,
}

struct CastlingSkipProgress {
    total_rows: u64,
    next_report_percent: u64,
    invalid_castling_by_letter: BTreeMap<char, u64>,
}

impl CastlingSkipProgress {
    fn new(total_rows: u64) -> Self {
        Self {
            total_rows,
            next_report_percent: 10,
            invalid_castling_by_letter: BTreeMap::new(),
        }
    }

    fn record_error(&mut self, err: &anyhow::Error) {
        if let Some(letter) = invalid_castling_letter_from_error(err) {
            *self.invalid_castling_by_letter.entry(letter).or_insert(0) += 1;
        }
    }

    fn maybe_report(&mut self, progress: &ProgressBar, processed_rows: u64) {
        if self.total_rows == 0 {
            return;
        }

        while self.next_report_percent <= 100
            && processed_rows.saturating_mul(100)
                >= self.total_rows.saturating_mul(self.next_report_percent)
        {
            progress.println(format!(
                "castling skip distribution at {}% (processed {}/{} rows):",
                self.next_report_percent, processed_rows, self.total_rows
            ));
            for line in castling_distribution_table_lines(&self.invalid_castling_by_letter) {
                progress.println(line);
            }
            self.next_report_percent += 10;
        }
    }

    fn into_distribution(self) -> BTreeMap<char, u64> {
        self.invalid_castling_by_letter
    }
}

struct PendingKey {
    zobr64: u64,
    entries: Vec<[u8; ENTRY_SIZE]>,
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

    db.flush().context("failed to flush RocksDB before compaction")?;
    println!("Running full compact_range on default column family...");
    db.compact_range::<&[u8], &[u8]>(None, None);
    db.flush().context("failed to flush RocksDB after compaction")?;
    progress.finish_with_message("Conversion complete");

    println!(
        "Done. input_rows={} skipped_rows={} written_keys={} ingested_sst_files={}",
        stats.input_rows, stats.skipped_rows, stats.written_keys, stats.ingested_sst_files
    );
    println!("Final castling skip distribution:");
    for line in castling_distribution_table_lines(&stats.invalid_castling_by_letter) {
        println!("{}", line);
    }

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
    let mut pending: Option<PendingKey> = None;
    let mut last_input_key: Option<u64> = None;

    let stream_stats = stream_rows(input_files, batch_rows, progress, |zobr64, entry| {
        if let Some(last) = last_input_key
            && zobr64 < last
        {
            bail!(
                "input is not sorted by zobr64 (saw {} after {}).",
                zobr64,
                last
            );
        }
        last_input_key = Some(zobr64);

        if let Some(active) = pending.as_mut()
            && active.zobr64 == zobr64
        {
            active.entries.push(entry);
            return Ok(());
        }

        if let Some(finished) = pending.take() {
            let value = encode_rocksdb_value(&finished.entries).with_context(|| {
                format!("failed to encode value for zobr64 {}", finished.zobr64)
            })?;
            sst_state.write_pair(finished.zobr64, &value)?;
        }

        pending = Some(PendingKey {
            zobr64,
            entries: vec![entry],
        });
        Ok(())
    })?;

    if let Some(finished) = pending.take() {
        let value = encode_rocksdb_value(&finished.entries)
            .with_context(|| format!("failed to encode value for zobr64 {}", finished.zobr64))?;
        sst_state.write_pair(finished.zobr64, &value)?;
    }
    sst_state.finish()?;

    if sst_tmp_dir.exists() {
        fs::remove_dir_all(&sst_tmp_dir)
            .with_context(|| format!("failed to remove {}", sst_tmp_dir.display()))?;
    }

    Ok(LoadStats {
        input_rows: stream_stats.input_rows,
        skipped_rows: stream_stats.skipped_rows,
        invalid_castling_by_letter: stream_stats.invalid_castling_by_letter,
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
    let mut keys_in_batch = 0usize;
    let mut written_keys = 0u64;
    let mut pending: Option<PendingKey> = None;
    let mut last_input_key: Option<u64> = None;

    let stream_stats = stream_rows(input_files, batch_rows, progress, |zobr64, entry| {
        if let Some(last) = last_input_key
            && zobr64 < last
        {
            bail!(
                "input is not sorted by zobr64 (saw {} after {}).",
                zobr64,
                last
            );
        }
        last_input_key = Some(zobr64);

        if let Some(active) = pending.as_mut()
            && active.zobr64 == zobr64
        {
            active.entries.push(entry);
            return Ok(());
        }

        if let Some(finished) = pending.take() {
            let value = encode_rocksdb_value(&finished.entries).with_context(|| {
                format!("failed to encode value for zobr64 {}", finished.zobr64)
            })?;
            let key = finished.zobr64.to_be_bytes();
            batch.put(key, &value);
            keys_in_batch += 1;
            written_keys += 1;
        }

        pending = Some(PendingKey {
            zobr64,
            entries: vec![entry],
        });

        if keys_in_batch >= write_batch_rows {
            let full_batch = std::mem::replace(
                &mut batch,
                WriteBatch::with_capacity_bytes(batch_capacity_bytes),
            );
            db.write_opt(full_batch, &write_opts)
                .context("failed to write RocksDB WriteBatch")?;
            keys_in_batch = 0;
        }
        Ok(())
    })?;

    if let Some(finished) = pending.take() {
        let value = encode_rocksdb_value(&finished.entries)
            .with_context(|| format!("failed to encode value for zobr64 {}", finished.zobr64))?;
        let key = finished.zobr64.to_be_bytes();
        batch.put(key, &value);
        keys_in_batch += 1;
        written_keys += 1;
    }

    if keys_in_batch > 0 {
        db.write_opt(batch, &write_opts)
            .context("failed to write final RocksDB WriteBatch")?;
    }

    Ok(LoadStats {
        input_rows: stream_stats.input_rows,
        skipped_rows: stream_stats.skipped_rows,
        invalid_castling_by_letter: stream_stats.invalid_castling_by_letter,
        written_keys,
        ingested_sst_files: 0,
    })
}

fn stream_rows<F>(
    input_files: &[PathBuf],
    batch_rows: usize,
    progress: &ProgressBar,
    mut on_row: F,
) -> Result<StreamStats>
where
    F: FnMut(u64, [u8; ENTRY_SIZE]) -> Result<()>,
{
    let mut input_rows = 0u64;
    let mut skipped_rows = 0u64;
    let mut castling_progress = CastlingSkipProgress::new(progress.length().unwrap_or(0));

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
            process_batch(
                &batch,
                &mut input_rows,
                &mut skipped_rows,
                &mut castling_progress,
                progress,
                &mut on_row,
            )?;
            progress.inc(batch.num_rows() as u64);
        }
    }

    Ok(StreamStats {
        input_rows,
        skipped_rows,
        invalid_castling_by_letter: castling_progress.into_distribution(),
    })
}

fn process_batch<F>(
    batch: &RecordBatch,
    input_rows: &mut u64,
    skipped_rows: &mut u64,
    castling_progress: &mut CastlingSkipProgress,
    progress: &ProgressBar,
    on_row: &mut F,
) -> Result<()>
where
    F: FnMut(u64, [u8; ENTRY_SIZE]) -> Result<()>,
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

        let encoded = match encode_entry(eval, mate, depth, &fen, best_move.as_deref()) {
            Ok(encoded) => encoded,
            Err(err) => {
                *skipped_rows += 1;
                castling_progress.record_error(&err);
                castling_progress.maybe_report(progress, *input_rows);
                continue;
            }
        };
        on_row(zobr64, encoded)?;
        castling_progress.maybe_report(progress, *input_rows);
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

fn encode_rocksdb_value(entries: &[[u8; ENTRY_SIZE]]) -> Result<Vec<u8>> {
    if entries.is_empty() {
        bail!("cannot encode empty entry list");
    }
    if entries.len() > u8::MAX as usize {
        bail!(
            "too many entries for one zobr64 key: {} (max {})",
            entries.len(),
            u8::MAX
        );
    }

    let mut out = Vec::with_capacity(1 + entries.len() * ENTRY_SIZE);
    out.push(entries.len() as u8);
    for entry in entries {
        out.extend_from_slice(entry);
    }
    Ok(out)
}

fn encode_entry(
    eval: Option<i32>,
    mate: Option<i32>,
    depth: i32,
    fen: &str,
    best_move: Option<&str>,
) -> Result<[u8; ENTRY_SIZE]> {
    // The schema stores either centipawns (kind=0) or mate score (kind=1) in one i16.
    let (raw_score, kind_bit) = if let Some(mate_score) = mate {
        (mate_score, 1u16)
    } else if let Some(eval_score) = eval {
        (eval_score, 0u16)
    } else {
        bail!("row has neither eval nor mate");
    };
    let score = clamp_i16(raw_score);
    let depth_u8 = clamp_depth_u8(depth);
    let move_meta = pack_move_meta(best_move, kind_bit)?;
    let board34 = fen_to_board34(fen)?;

    let mut out = [0u8; ENTRY_SIZE];
    out[0..2].copy_from_slice(&score.to_le_bytes());
    out[2] = depth_u8;
    out[3..5].copy_from_slice(&move_meta.to_le_bytes());
    out[5..].copy_from_slice(&board34);
    Ok(out)
}

fn clamp_i16(v: i32) -> i16 {
    if v > i16::MAX as i32 {
        i16::MAX
    } else if v < i16::MIN as i32 {
        i16::MIN
    } else {
        v as i16
    }
}

fn clamp_depth_u8(depth: i32) -> u8 {
    depth.clamp(0, u8::MAX as i32) as u8
}

fn pack_move_meta(best_move: Option<&str>, kind_bit: u16) -> Result<u16> {
    let (from, to, promo) = if let Some(mv) = best_move {
        parse_uci_move(mv)?
    } else {
        (0u8, 0u8, 0u8)
    };

    let packed = u16::from(from)
        | (u16::from(to) << 6)
        | (u16::from(promo) << 12)
        | (kind_bit << 15);
    Ok(packed)
}

fn parse_uci_move(mv: &str) -> Result<(u8, u8, u8)> {
    if mv == "0000" {
        return Ok((0, 0, 0));
    }

    let bytes = mv.as_bytes();
    if bytes.len() != 4 && bytes.len() != 5 {
        bail!("move is not UCI length 4/5: {}", mv);
    }

    let from = parse_square(&mv[0..2]).with_context(|| format!("invalid UCI from-square: {}", mv))?;
    let to = parse_square(&mv[2..4]).with_context(|| format!("invalid UCI to-square: {}", mv))?;
    let promo = if bytes.len() == 5 {
        parse_promo(bytes[4] as char)?
    } else {
        0
    };
    Ok((from, to, promo))
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

fn parse_promo(c: char) -> Result<u8> {
    match c.to_ascii_lowercase() {
        'n' => Ok(1),
        'b' => Ok(2),
        'r' => Ok(3),
        'q' => Ok(4),
        other => bail!("unsupported promotion piece: {}", other),
    }
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
    opts.set_compression_type(DBCompressionType::Zstd);
    opts.set_compression_options(0, 3, 0, 0);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_compression_options(0, 6, 0, 0, true);

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
