# lc_eval2parquet

Convert `lichess_db_eval.jsonl.zst` into a folder of Parquet files (about 100 MB each).

The converter:
- streams input directly from `.zst` (no full decompression to disk),
- writes nested Parquet rows using the schema in `design/20260228_parquet_schema.md`,
- rotates output files by size (`part-00000.parquet`, `part-00001.parquet`, ...).

## Requirements

- Rust toolchain (stable)
- Input file in `/lichess_db_eval` (project-root-relative on Windows in this project)

## Build

```bash
cargo build --release
```

## Run (default paths)

```bash
cargo run --release --
```

Defaults:
- input dir: `/lichess_db_eval`
- output dir: `/lichess_eval_parquet`
- target file size: `100 MB`
- batch rows: `25000`
- parquet compression: `zstd level 3`

## Run (explicit arguments)

```bash
cargo run --release -- --input-dir /lichess_db_eval --output-dir /lichess_eval_parquet --target-file-mb 100 --batch-rows 25000 --parquet-zstd-level 3
```

## Select a specific input file

```bash
cargo run --release -- --input-file /lichess_db_eval/lichess_db_eval.jsonl.zst
```

## Add zobrist hashes to parquet

This executable reads parquet files from `/lichess_eval_parquet`, computes hashes from `fen`, and writes output parquet files with two new columns:
- `zobr64` (`UInt64`)
- `zobr128` (`FixedSizeBinary(16)`)

Run with defaults:

```bash
cargo run --release --bin add_zobrist --
```

Defaults:
- input dir: `/lichess_eval_parquet`
- output dir: `/lichess_eval_parquet_zobr`
- batch rows: `50000`
- parquet compression: `zstd level 3`

Run with explicit arguments:

```bash
cargo run --release --bin add_zobrist -- --input-dir /lichess_eval_parquet --output-dir /lichess_eval_parquet_zobr --batch-rows 50000 --parquet-zstd-level 3
```

## Sort parquet by zobr64 using DataFusion

This executable reads parquet files from `../lichess_eval_parquet_zobr`, globally sorts rows by `zobr64` using DataFusion, and writes `part-*.parquet` files to `../lichess_eval_parquet_zobr_sorted`.

- Output parts are rotated at approximately `100 MB` each (`--target-file-mb`).
- Progress bar is row-based and includes elapsed time + ETA.
- Total row count is precalculated before writing so progress has a fixed total.

Run with defaults:

```bash
cargo run --release --bin sort_parquet --
```

Run with explicit arguments:

```bash
cargo run --release --bin sort_parquet -- --input-dir lichess_eval_parquet_zobr --output-dir lichess_eval_parquet_zobr_sorted --sort-column zobr64 --target-file-mb 100 --batch-rows 5000 --memory-limit-mb 2048 --parquet-zstd-level 3 --overwrite
```

## Simplify zobr-sorted parquet

This executable reads parquet files from `lichess_eval_parquet_zobr_sorted` (already sorted by `zobr64`) and writes simplified parquet parts to `/lichess_eval_parquet_zobr_simplified` with columns:

- `zobr64`
- `eval`
- `mate`
- `depth`
- `fen`

Rules applied:
- per input row, select the eval entry with the highest `depth` (using its first PV for `eval`/`mate`)
- per `zobr64`, if positions differ **only** by castling rights, keep only the deepest one
- per `zobr64`, if positions differ by anything else (board, side to move, or en-passant), keep each distinct position

Output files rotate at approximately `100 MB` each (`--target-file-mb`).

Run with defaults:

```bash
cargo run --release --bin simplify_zobr_parquet --
```

Run with explicit arguments:

```bash
cargo run --release --bin simplify_zobr_parquet -- --input-dir lichess_eval_parquet_zobr_sorted --output-dir /lichess_eval_parquet_zobr_simplified --target-file-mb 100 --batch-rows 50000 --parquet-zstd-level 3 --overwrite
```

## Load simplified parquet into SQLite

This executable reads parquet files from `lichess_eval_parquet_zobr_simplified` and writes a single SQLite database file in `/lichess_eval_sqlite`.

- output file: `/lichess_eval_sqlite/lichess_eval.sqlite`
- output table: `eval_by_zobr64`
- schema columns: `zobr64`, `eval`, `mate`, `depth`, `fen`, `first_move`
- row progress bar includes elapsed time + ETA
- one bulk transaction is used for the whole load
- duplicate `zobr64` rows are collapsed during load (last row for a key is kept)

Run with defaults:

```bash
cargo run --release --bin parquet_simplified_to_sqlite --
```

Defaults:
- input dir: `lichess_eval_parquet_zobr_simplified`
- output dir: `/lichess_eval_sqlite`
- output file: `lichess_eval.sqlite`
- batch rows: `50000`
- sqlite cache size: `512 MB`
- sqlite page size: `32768`

Run with explicit arguments:

```bash
cargo run --release --bin parquet_simplified_to_sqlite -- --input-dir lichess_eval_parquet_zobr_simplified --output-dir /lichess_eval_sqlite --output-file lichess_eval.sqlite --batch-rows 50000 --cache-size-mb 512 --page-size 32768 --overwrite
```

## Load simplified parquet into RocksDB

This executable reads parquet files from `lichess_eval_parquet_zobr_simplified` and writes a RocksDB in `/lichess_eval_rocksdb` using the schema in `design/20260303_rocksdb_schema.md`.

- key: `zobr64` (8 bytes, big-endian `u64`)
- value: `u8 count` + `count` fixed-size entries (39 bytes each)
- each entry: `i16 score` (LE), `u8 depth`, `u16 move_meta` (LE), `u8[34] board34`
- if multiple rows share the same `zobr64`, all entries are stored for that key
- score/depth are clamped to schema bounds during encoding
- RocksDB uses zstd compression (normal level 3, bottommost level 6) and runs full compaction at the end

Run with defaults:

```bash
cargo run --release --bin parquet_simplified_to_rocksdb --
```

Defaults:
- input dir: `lichess_eval_parquet_zobr_simplified`
- output dir: `/lichess_eval_rocksdb`
- batch rows: `50000`
- write path: `sst` (fastest when input is sorted by `zobr64`)
- sst rows per file: `2000000`

Run with explicit arguments:

```bash
cargo run --release --bin parquet_simplified_to_rocksdb -- --input-dir lichess_eval_parquet_zobr_simplified --output-dir /lichess_eval_rocksdb --batch-rows 50000 --write-path sst --sst-rows-per-file 2000000 --overwrite
```

Alternative WriteBatch mode:

```bash
cargo run --release --bin parquet_simplified_to_rocksdb -- --input-dir lichess_eval_parquet_zobr_simplified --output-dir /lichess_eval_rocksdb --batch-rows 50000 --write-path batch --write-batch-rows 50000 --overwrite
```

## Check bad castling FENs in simplified parquet

This executable scans a sample of rows from `lichess_eval_parquet_zobr_simplified` and prints FENs that fail due to invalid castling flags.

Run with defaults (first 1% of rows):

```bash
cargo run --release --bin check_bad_castling_fens --
```

Defaults:
- input dir: `lichess_eval_parquet_zobr_simplified`
- batch rows: `50000`
- sample percent: `1.0`

Run with explicit arguments and save failing FENs:

```bash
cargo run --release --bin check_bad_castling_fens -- --input-dir lichess_eval_parquet_zobr_simplified --batch-rows 50000 --sample-percent 1 > bad_castling_fens.txt
```

## Diagnose problematic FEN positions

This executable extracts FENs from raw text (including `zobr64: ...` collision dumps), parses them with `shakmaty`, and prints detected issues per position.

If `zobr64:` headers are present, output is grouped by `zobr64` and each group prints all its FEN diagnostics.

Detected problem types include:
- invalid FEN syntax
- impossible side to move for standard starting board
- invalid castling rights
- invalid en-passant square
- impossible check state
- too much material
- castling rights exist but required king/rook setup is impossible
- en-passant square exists but no legal en-passant capture is possible

Run with inline text:

```bash
cargo run --release --bin diagnose_fen_problems -- --text "zobr64: ... rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
```

Run from file:

```bash
cargo run --release --bin diagnose_fen_problems -- --input collisions.txt
```

Run from stdin:

```bash
cat collisions.txt | cargo run --release --bin diagnose_fen_problems --
```

All detected positions are printed. Valid positions are marked with `OK`.

## Convert eval jsonl.zst to RocksDB

This executable reads `*.jsonl.zst` from `/lichess_db_eval`, computes `zobr64` from `fen`, and writes a RocksDB to `/lichess_eval_rocksdb`.

- key: `zobr64` (8 bytes, big-endian `u64`)
- value: JSON containing `eval`, `mate`, `depth`, `fen`
- per input line: picks the eval entry with the biggest `depth`
- per `zobr64`: keeps only the entry with the biggest `depth`

Run with defaults:

```bash
cargo run --release --bin eval_jsonl_to_rocksdb --
```

Run with explicit arguments:

```bash
cargo run --release --bin eval_jsonl_to_rocksdb -- --input-dir /lichess_db_eval --output-dir /lichess_eval_rocksdb --progress-every 1000000 --overwrite
```

Use a specific input file:

```bash
cargo run --release --bin eval_jsonl_to_rocksdb -- --input-file /lichess_db_eval/lichess_db_eval.jsonl.zst --output-dir /lichess_eval_rocksdb --overwrite
```

## Performance and memory notes

- This tool is designed for large inputs (for example 20 GB `.zst`) with streaming I/O.
- Memory usage is primarily controlled by `--batch-rows` and `--memory-limit-mb`.
- The sorter defaults are conservative for large global sorts (`batch_rows=5000`, `target_partitions=2`, low spill reservation).
- If you still hit memory pressure, reduce batch size and/or memory limit further, for example:

```bash
cargo run --release --bin sort_parquet -- --batch-rows 2000 --memory-limit-mb 1024
```

## Check CLI help

```bash
cargo run --release -- --help
```
