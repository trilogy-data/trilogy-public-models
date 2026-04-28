#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.0",
#   "google-cloud-storage>=2.18",
# ]
# ///
"""One-off: take a single pre-existing flights.parquet and split it into
per-year parquet files matching the new partitioned layout, then upload
each year file to GCS. Avoids re-downloading BTS data from scratch.

Partitions on ``flight_date`` when the source parquet has that column (new
schema — always populated, including for cancelled rows). Falls back to
``dep_time`` for legacy parquets; in that mode cancelled rows with the
1900-01-01 sentinel land in ``flights_1900.parquet``.

NOTE: if your source parquet lacks flight_date, the preferred fix is to
re-run ``refresh_flights.py`` — the BTS monthly zips are cached under
``_raw/`` and the CSV→parquet transform is fast (no re-download).

Auth for upload: Application Default Credentials (run
``gcloud auth application-default login`` once if needed). Pass
``--skip-upload`` to only split locally.

Usage:
  uv run trilogy_public_models/duckdb/faa/ingest/split_and_publish_local.py \
      --source trilogy_public_models/duckdb/faa/ingest/flights.parquet
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "flights"
DEFAULT_BUCKET = "trilogy_public_models"
DEFAULT_PREFIX = "duckdb/faa/flights"


def split_by_year(source: Path, output_dir: Path) -> list[Path]:
    con = duckdb.connect(":memory:")
    src = str(source).replace("'", "''")

    cols = {
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{src}')"
        ).fetchall()
    }
    if "flight_date" in cols:
        partition_col = "flight_date"
        year_expr = "year(flight_date)"
        range_cast = "DATE"
        range_col = "flight_date"
    elif "dep_time" in cols:
        partition_col = "dep_time"
        year_expr = "year(dep_time)"
        range_cast = "TIMESTAMP"
        range_col = "dep_time"
        print(
            "WARNING: source lacks flight_date; partitioning by dep_time. "
            "Cancelled rows with sentinel 1900-01-01 will land in flights_1900.parquet. "
            "Prefer re-running refresh_flights.py (cached zips) to get flight_date."
        )
    else:
        raise RuntimeError(
            f"source parquet has neither flight_date nor dep_time column: {sorted(cols)}"
        )

    years = [
        int(r[0])
        for r in con.execute(
            f"SELECT DISTINCT {year_expr} FROM read_parquet('{src}') ORDER BY 1"
        ).fetchall()
    ]
    print(f"splitting {source} by {partition_col} into {len(years)} year file(s): {years}")
    output_dir.mkdir(parents=True, exist_ok=True)
    out_files: list[Path] = []
    for year in years:
        out = output_dir / f"flights_{year}.parquet"
        lo = f"{year:04d}-01-01"
        hi = f"{year + 1:04d}-01-01"
        out_str = str(out).replace("'", "''")
        t0 = time.monotonic()
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{src}')
                WHERE {range_col} >= {range_cast} '{lo}'
                  AND {range_col} < {range_cast} '{hi}'
            ) TO '{out_str}' (FORMAT 'parquet', COMPRESSION 'zstd')
            """
        )
        elapsed = time.monotonic() - t0
        size_mb = out.stat().st_size / 1e6
        rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_str}')"
        ).fetchone()[0]
        print(
            f"  wrote {out.name}: {rows:,} rows, {size_mb:,.1f} MB "
            f"({elapsed:,.1f}s)"
        )
        out_files.append(out)
    return out_files


def upload_all(
    files: list[Path], bucket_name: str, prefix: str, concurrency: int, force: bool
) -> None:
    from google.cloud import storage  # lazy: only needed when uploading

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = prefix.rstrip("/")

    def upload_one(path: Path) -> tuple[str, float, bool]:
        obj = f"{prefix}/{path.name}"
        blob = bucket.blob(obj)
        local_size = path.stat().st_size
        if not force and blob.exists():
            blob.reload()
            if blob.size == local_size:
                return (obj, 0.0, False)
        blob.chunk_size = 32 * 1024 * 1024
        t0 = time.monotonic()
        blob.upload_from_filename(str(path), timeout=None)
        return (obj, time.monotonic() - t0, True)

    total_bytes = sum(f.stat().st_size for f in files)
    print(
        f"uploading {len(files)} file(s), {total_bytes / 1e6:,.1f} MB total "
        f"-> gs://{bucket_name}/{prefix}/ (concurrency={concurrency})"
    )
    uploaded = skipped = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(upload_one, f): f for f in files}
        for fut in as_completed(futures):
            obj, secs, did_upload = fut.result()
            src = futures[fut]
            mb = src.stat().st_size / 1e6
            if did_upload:
                uploaded += 1
                rate = mb / max(secs, 1e-9)
                print(
                    f"  uploaded {obj} ({mb:,.1f} MB in {secs:,.1f}s, {rate:,.1f} MB/s)"
                )
            else:
                skipped += 1
                print(f"  skipped {obj} (size matches)")
    print(f"upload complete: {uploaded} uploaded, {skipped} skipped")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).parent / "flights.parquet",
        help="single parquet to split",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="where to write per-year parquet files",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="only split locally, do not upload to GCS",
    )
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument(
        "--force", action="store_true", help="re-upload even if size matches"
    )
    args = parser.parse_args(argv)

    if not args.source.exists():
        print(f"error: {args.source} does not exist", file=sys.stderr)
        return 1

    files = split_by_year(args.source, args.output_dir)
    if not files:
        print("no year files produced", file=sys.stderr)
        return 1

    if args.skip_upload:
        print("--skip-upload set, stopping after split")
        return 0

    upload_all(files, args.bucket, args.prefix, args.concurrency, args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
