#!/usr/bin/env python3
"""
Sort parquet files by zobr64 out-of-memory and write ~100MB output parts.

Default behavior:
- input dir:  ../lichess_eval_parquet_zobr
- output dir: ../lichess_eval_parquet_zobr_sorted

The script uses Polars lazy scanning + sink_parquet for external-memory sorting.
Then it streams the sorted result and writes part files with a progress bar + ETA.
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterable

import polars as pl

try:
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def human_bytes(num: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"


def format_eta(seconds: float) -> str:
    if not math.isfinite(seconds) or seconds < 0:
        return "--:--:--"
    sec = int(seconds)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


class ProgressBar:
    def __init__(self, total: int, desc: str, unit: str = "rows") -> None:
        self.total = max(total, 1)
        self.desc = desc
        self.unit = unit
        self.done = 0
        self.started_at = time.time()
        self.last_draw = 0.0

    def update(self, n: int) -> None:
        self.done += n
        now = time.time()
        if now - self.last_draw >= 0.25 or self.done >= self.total:
            self._draw(now)
            self.last_draw = now

    def _draw(self, now: float) -> None:
        elapsed = max(now - self.started_at, 1e-9)
        rate = self.done / elapsed
        remaining = max(self.total - self.done, 0)
        eta = remaining / rate if rate > 0 else math.inf
        pct = min(self.done / self.total, 1.0)
        width = 30
        filled = int(pct * width)
        bar = "#" * filled + "." * (width - filled)
        msg = (
            f"\r{self.desc} [{bar}] {pct * 100:6.2f}% "
            f"{self.done}/{self.total} {self.unit} "
            f"| {rate:,.0f} {self.unit}/s | ETA {format_eta(eta)}"
        )
        print(msg, end="", flush=True)

    def close(self) -> None:
        self._draw(time.time())
        print()


def resolve_path(base_dir: Path, path_text: str) -> Path:
    candidate = Path(path_text)
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()


def estimate_rows_per_file(
    sorted_path: Path,
    total_rows: int,
    target_bytes: int,
    compression: str,
) -> int:
    sample_rows = min(250_000, total_rows)
    sample_df = pl.read_parquet(sorted_path, n_rows=sample_rows)

    with tempfile.TemporaryDirectory(prefix="rows-per-file-estimate-") as tmp_dir:
        tmp_sample = Path(tmp_dir) / "sample.parquet"
        sample_df.write_parquet(tmp_sample, compression=compression, statistics=True)
        sample_size = tmp_sample.stat().st_size

    bytes_per_row = max(sample_size / max(sample_rows, 1), 1.0)
    rows = int(target_bytes / bytes_per_row)
    return max(rows, 10_000)


def iter_sorted_batches(
    sorted_path: Path,
    batch_rows: int,
) -> Iterable[pl.DataFrame]:
    if HAS_PYARROW:
        parquet_file = pq.ParquetFile(sorted_path)
        for record_batch in parquet_file.iter_batches(batch_size=batch_rows):
            yield pl.from_arrow(record_batch)
        return

    # Fallback path if pyarrow is unavailable: still bounded memory, but slower
    # because each slice can trigger repeated scans of the same file.
    total_rows = (
        pl.scan_parquet(sorted_path)
        .select(pl.len().alias("n"))
        .collect(streaming=True)["n"][0]
    )
    offset = 0
    while offset < total_rows:
        chunk = (
            pl.scan_parquet(sorted_path)
            .slice(offset, batch_rows)
            .collect(streaming=True)
        )
        if chunk.height == 0:
            break
        yield chunk
        offset += chunk.height


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Sort parquet files by zobr64 out-of-memory and emit ~100MB parquet parts."
        )
    )
    parser.add_argument(
        "--input-dir",
        default="../lichess_eval_parquet_zobr",
        help="Input parquet directory.",
    )
    parser.add_argument(
        "--output-dir",
        default="../lichess_eval_parquet_zobr_sorted",
        help="Output parquet directory.",
    )
    parser.add_argument(
        "--sort-column",
        default="zobr64",
        help="Column used for global sort.",
    )
    parser.add_argument(
        "--target-file-mb",
        type=int,
        default=100,
        help="Approximate output parquet file size in MB.",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec.",
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=200_000,
        help="Streaming batch rows during final split/write stage.",
    )
    parser.add_argument(
        "--temp-dir",
        default="",
        help="Temp directory for Polars spill files and temp sorted file.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove existing output directory before writing.",
    )
    parser.add_argument(
        "--keep-temp-sorted",
        action="store_true",
        help="Keep temporary fully-sorted parquet file.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    input_dir = resolve_path(script_dir, args.input_dir)
    output_dir = resolve_path(script_dir, args.output_dir)
    temp_dir = resolve_path(script_dir, args.temp_dir) if args.temp_dir else None

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        os.environ["POLARS_TEMP_DIR"] = str(temp_dir)

    input_files = sorted(input_dir.glob("*.parquet"))
    if not input_files:
        raise FileNotFoundError(f"No parquet files found in: {input_dir}")

    if output_dir.exists() and args.overwrite:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    existing_parts = list(output_dir.glob("part-*.parquet"))
    if existing_parts:
        raise RuntimeError(
            f"Output directory already has parquet parts ({len(existing_parts)} files): "
            f"{output_dir}. Use --overwrite to replace."
        )

    print(f"Input files: {len(input_files)}")
    print(f"Sort column: {args.sort_column}")
    print("Starting external-memory sort with Polars...")

    tmp_parent = temp_dir if temp_dir is not None else output_dir
    tmp_sorted_path = tmp_parent / "__tmp_sorted_full.parquet"
    if tmp_sorted_path.exists():
        tmp_sorted_path.unlink()

    lf = pl.scan_parquet([str(p) for p in input_files], low_memory=True)
    schema = lf.collect_schema()
    if args.sort_column not in schema:
        raise RuntimeError(
            f"Sort column '{args.sort_column}' not found. Available columns: "
            f"{', '.join(schema.names())}"
        )
    sorted_lf = lf.sort(args.sort_column)

    try:
        sorted_lf.sink_parquet(str(tmp_sorted_path), compression=args.compression)
    except Exception as exc:
        raise RuntimeError(
            "Polars could not execute the external-memory sort. "
            "Use a recent Polars version with streaming support."
        ) from exc

    if not tmp_sorted_path.exists():
        raise RuntimeError("Temporary sorted parquet was not created.")

    total_rows = (
        pl.scan_parquet(tmp_sorted_path)
        .select(pl.len().alias("n"))
        .collect(streaming=True)["n"][0]
    )
    if total_rows <= 0:
        raise RuntimeError("Sorted parquet contains zero rows.")

    target_bytes = args.target_file_mb * 1024 * 1024
    rows_per_part = estimate_rows_per_file(
        sorted_path=tmp_sorted_path,
        total_rows=total_rows,
        target_bytes=target_bytes,
        compression=args.compression,
    )

    approx_parts = math.ceil(total_rows / rows_per_part)
    print(f"Sorted rows: {total_rows:,}")
    print(
        f"Target part size: ~{args.target_file_mb} MB "
        f"(estimated rows/part: {rows_per_part:,}, approx parts: {approx_parts:,})"
    )
    if not HAS_PYARROW:
        print(
            "pyarrow not found: using slower fallback split mode. "
            "Install pyarrow for faster sequential batch reads."
        )

    pbar = ProgressBar(total=total_rows, desc="Writing output parts", unit="rows")
    part_idx = 0
    buffer: list[pl.DataFrame] = []
    buffered_rows = 0

    def flush_buffer() -> None:
        nonlocal part_idx, buffer, buffered_rows
        if not buffer:
            return
        out_df = pl.concat(buffer, rechunk=True)
        out_path = output_dir / f"part-{part_idx:05d}.parquet"
        out_df.write_parquet(out_path, compression=args.compression, statistics=True)
        part_idx += 1
        buffer = []
        buffered_rows = 0

    try:
        for batch in iter_sorted_batches(tmp_sorted_path, args.batch_rows):
            buffer.append(batch)
            buffered_rows += batch.height
            pbar.update(batch.height)
            if buffered_rows >= rows_per_part:
                flush_buffer()
        flush_buffer()
    finally:
        pbar.close()

    sizes = [p.stat().st_size for p in sorted(output_dir.glob("part-*.parquet"))]
    if not sizes:
        raise RuntimeError("No output parquet files were written.")

    print(f"Wrote {len(sizes)} parquet files to: {output_dir}")
    print(
        f"Output size stats: min={human_bytes(min(sizes))}, "
        f"max={human_bytes(max(sizes))}, avg={human_bytes(sum(sizes) / len(sizes))}"
    )

    if args.keep_temp_sorted:
        print(f"Kept temp sorted parquet: {tmp_sorted_path}")
    else:
        if tmp_sorted_path.exists():
            tmp_sorted_path.unlink()
        print("Removed temp sorted parquet.")


if __name__ == "__main__":
    main()
