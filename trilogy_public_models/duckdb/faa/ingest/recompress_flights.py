#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyarrow",
# ]
# ///
"""One-shot: rewrite existing flights/flights_{year}.parquet files into
flights/flights_v2_{year}.parquet using the WASM-tuned layout shared with
refresh_flights.py — sort, Timestamp[s], 120k row groups, DELTA_BINARY_PACKED
for id2, bloom filters on tail_num/flight_num.

Lets us republish the optimised set without re-reading the BTS monthly
zips. After this runs, the v2 files sit alongside the v1 files; publish
with publish_flights.py and then update flight.preql to point at v2.

Usage:
  uv run trilogy_public_models/duckdb/faa/ingest/recompress_flights.py
  uv run trilogy_public_models/duckdb/faa/ingest/recompress_flights.py \
      --source flights --year 2024
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from refresh_flights import (
    BLOOM_COLUMNS,
    COLUMN_ENCODING,
    DICT_COLUMNS,
    ROW_GROUP_SIZE,
)

DEFAULT_SOURCE = Path(__file__).parent / "flights"
SOURCE_PATTERN = re.compile(r"^flights_(\d{4})\.parquet$")
OUT_FILENAME = "flights_v2_{year}.parquet"


def recompress(src: Path, dst: Path) -> tuple[int, int, int]:
    table = pq.read_table(src)
    # Recast dep_time/arr_time to seconds precision (BTS records minute resolution).
    new_fields = [
        pa.field(f.name, pa.timestamp("s")) if f.name in ("dep_time", "arr_time") else f
        for f in table.schema
    ]
    table = table.cast(pa.schema(new_fields), safe=False)

    pq.write_table(
        table,
        dst,
        compression="zstd",
        compression_level=9,
        use_dictionary=list(DICT_COLUMNS),
        column_encoding=dict(COLUMN_ENCODING),
        write_statistics=True,
        write_page_index=True,
        bloom_filter_options={"columns": {c: True for c in BLOOM_COLUMNS}},
        row_group_size=ROW_GROUP_SIZE,
    )
    return table.num_rows, src.stat().st_size, dst.stat().st_size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="recompress only this year (default: all flights_YYYY.parquet)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing v2 files (default skips when present)",
    )
    args = parser.parse_args(argv)

    if not args.source.is_dir():
        print(f"error: {args.source} is not a directory", file=sys.stderr)
        return 1

    candidates: list[tuple[int, Path]] = []
    for p in sorted(args.source.glob("flights_*.parquet")):
        m = SOURCE_PATTERN.match(p.name)
        if not m:
            continue
        year = int(m.group(1))
        if args.year is not None and year != args.year:
            continue
        candidates.append((year, p))

    if not candidates:
        print(f"no source files found under {args.source}", file=sys.stderr)
        return 1

    print(f"recompressing {len(candidates)} file(s)")
    total_in = total_out = total_rows = 0
    for year, src in candidates:
        dst = args.source / OUT_FILENAME.format(year=year)
        if dst.exists() and not args.force:
            print(f"  skip {dst.name} (exists; pass --force to overwrite)")
            total_in += src.stat().st_size
            total_out += dst.stat().st_size
            continue
        t0 = time.monotonic()
        rows, in_bytes, out_bytes = recompress(src, dst)
        elapsed = time.monotonic() - t0
        delta = (out_bytes - in_bytes) / in_bytes * 100
        print(
            f"  {dst.name}: {rows:>9,} rows  "
            f"{in_bytes / 1e6:>6.1f} MB -> {out_bytes / 1e6:>6.1f} MB  "
            f"({delta:+.1f}%, {elapsed:,.1f}s)"
        )
        total_in += in_bytes
        total_out += out_bytes
        total_rows += rows

    if total_in:
        delta = (total_out - total_in) / total_in * 100
        print(
            f"total: {total_rows:,} rows  "
            f"{total_in / 1e6:,.1f} MB -> {total_out / 1e6:,.1f} MB ({delta:+.1f}%)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
