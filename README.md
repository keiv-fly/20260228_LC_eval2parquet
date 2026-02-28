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

## Performance and memory notes

- This tool is designed for large inputs (for example 20 GB `.zst`) with streaming I/O.
- Memory usage is primarily controlled by `--batch-rows`.
- If you need lower RAM usage, reduce batch size, for example:

```bash
cargo run --release -- --batch-rows 10000
```

## Check CLI help

```bash
cargo run --release -- --help
```
