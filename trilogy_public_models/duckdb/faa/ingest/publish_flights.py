#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "google-cloud-storage>=2.18",
# ]
# ///
"""Upload the local yearly flight parquet files to the public trilogy models
bucket.

Each *.parquet under ``--source`` is uploaded to
``gs://{bucket}/{prefix}/<name>``; existing objects with identical size are
skipped unless ``--force`` is passed.

Auth: uses Application Default Credentials. If you haven't authenticated
recently, run ``gcloud auth application-default login`` first (or set
``GOOGLE_APPLICATION_CREDENTIALS`` to a service account key path).

Usage:
  uv run trilogy_public_models/duckdb/faa/ingest/publish_flights.py
  uv run trilogy_public_models/duckdb/faa/ingest/publish_flights.py \
      --source /path/to/flights_dir \
      --bucket trilogy_public_models \
      --prefix duckdb/faa/flights
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google.cloud import storage

DEFAULT_SOURCE = Path(__file__).parent / "flights"
DEFAULT_BUCKET = "trilogy_public_models"
DEFAULT_PREFIX = "duckdb/faa/flights"
WATERMARK_PATH = Path(__file__).parent / "watermark.parquet"
WATERMARK_OBJECT = "duckdb/faa/watermark.parquet"


def upload_one(
    bucket: storage.Bucket,
    local_path: Path,
    object_name: str,
    force: bool,
) -> tuple[str, float, bool]:
    """Upload local_path -> bucket/object_name. Returns (object_name, seconds, uploaded)."""
    blob = bucket.blob(object_name)
    local_size = local_path.stat().st_size
    if not force and blob.exists():
        blob.reload()  # populates .size — only safe after exists()
        if blob.size == local_size:
            return (object_name, 0.0, False)
    blob.chunk_size = 32 * 1024 * 1024
    t0 = time.monotonic()
    blob.upload_from_filename(str(local_path), timeout=None)
    return (object_name, time.monotonic() - t0, True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--project", default=None, help="optional GCP project override")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="parallel uploads (default 4)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-upload even if size matches",
    )
    args = parser.parse_args(argv)

    if not args.source.exists():
        print(f"error: {args.source} does not exist", file=sys.stderr)
        return 1

    if args.source.is_file():
        files = [args.source]
    else:
        files = sorted(args.source.glob("*.parquet"))

    if not files:
        print(f"error: no .parquet files under {args.source}", file=sys.stderr)
        return 1

    client = storage.Client(project=args.project)
    bucket = client.bucket(args.bucket)

    total_bytes = sum(f.stat().st_size for f in files)
    prefix = args.prefix.rstrip("/")
    print(
        f"publishing {len(files)} file(s), {total_bytes / 1e6:,.1f} MB total "
        f"-> gs://{args.bucket}/{prefix}/ (concurrency={args.concurrency})"
    )

    t0 = time.monotonic()
    uploaded = 0
    skipped = 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(
                upload_one, bucket, f, f"{prefix}/{f.name}", args.force
            ): f
            for f in files
        }
        for fut in as_completed(futures):
            name, secs, did_upload = fut.result()
            src = futures[fut]
            mb = src.stat().st_size / 1e6
            if did_upload:
                uploaded += 1
                rate = mb / max(secs, 1e-9)
                print(f"  uploaded {name} ({mb:,.1f} MB in {secs:,.1f}s, {rate:,.1f} MB/s)")
            else:
                skipped += 1
                print(f"  skipped {name} (size matches)")

    elapsed = time.monotonic() - t0
    print(
        f"done in {elapsed:,.1f}s ({uploaded} uploaded, {skipped} skipped). "
        f"browse: https://storage.googleapis.com/{args.bucket}/{prefix}/"
    )

    # When publishing the main flights set (default prefix), also push the
    # watermark file so every refresh of the yearly parquets bumps the freshness
    # signal that the aggregate datasources check.
    if prefix == DEFAULT_PREFIX.rstrip("/") and WATERMARK_PATH.exists():
        wm_name, wm_secs, wm_did = upload_one(
            bucket, WATERMARK_PATH, WATERMARK_OBJECT, args.force
        )
        if wm_did:
            print(
                f"  uploaded {wm_name} ({WATERMARK_PATH.stat().st_size / 1e3:,.1f} KB "
                f"in {wm_secs:,.1f}s)"
            )
        else:
            print(f"  skipped {wm_name} (size matches)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
