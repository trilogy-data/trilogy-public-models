#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "google-cloud-storage>=2.18",
# ]
# ///
"""Apply scripts/cors.json to the public trilogy models bucket.

Replacement for ``gsutil cors set`` so we don't depend on a working gsutil.
DuckDB-WASM (browser) needs ``Range`` in the bucket's CORS allow-list to read
parquet footers — without it, preflight fails and DuckDB reports the file as
"too small to be a Parquet file" even though the file is intact.

Auth: Application Default Credentials (run
``gcloud auth application-default login`` once if needed).

Usage:
  uv run scripts/apply_cors.py
  uv run scripts/apply_cors.py --bucket trilogy_public_models --config scripts/cors.json
  uv run scripts/apply_cors.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from google.cloud import storage

DEFAULT_BUCKET = "trilogy_public_models"
DEFAULT_CONFIG = Path(__file__).parent / "cors.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--project", default=None, help="optional GCP project override")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be set without applying",
    )
    args = parser.parse_args(argv)

    if not args.config.exists():
        print(f"error: {args.config} does not exist", file=sys.stderr)
        return 1

    config = json.loads(args.config.read_text())
    if not isinstance(config, list):
        print(f"error: {args.config} must be a JSON list of CORS rules", file=sys.stderr)
        return 1

    client = storage.Client(project=args.project)
    bucket = client.get_bucket(args.bucket)

    print(f"current CORS on gs://{args.bucket}:")
    print(json.dumps(bucket.cors, indent=2))
    print()
    print(f"new CORS from {args.config}:")
    print(json.dumps(config, indent=2))

    if args.dry_run:
        print("\n--dry-run set, not applying")
        return 0

    bucket.cors = config
    bucket.patch()
    bucket.reload()
    print("\napplied. CORS now:")
    print(json.dumps(bucket.cors, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
