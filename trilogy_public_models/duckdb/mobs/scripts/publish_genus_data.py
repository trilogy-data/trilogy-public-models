#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "google-cloud-storage>=2.18",
# ]
# ///
"""Validate and upload the processed MOBS genus data to
``gs://trilogy_public_models/duckdb/mobs/genus_data_processed.csv`` — the object
the model's ``setup.sql`` reads from.

The upload is gated on the same domain constraints the model declares, so a bad
row cannot reach the published object and break `test_models` in CI. Today that
means every non-empty ``image_url`` must satisfy the ``url_image`` trait's
``\\S+://\\S+`` pattern, and no row may be a copy of the header line (a
checkpointed ingest run over a non-empty file has produced one before).

Uses Application Default Credentials (locally,
``gcloud auth application-default login``).

Usage:
  uv run trilogy_public_models/duckdb/mobs/scripts/publish_genus_data.py --dry-run
  uv run trilogy_public_models/duckdb/mobs/scripts/publish_genus_data.py
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

DEFAULT_SOURCE = (
    Path(__file__).resolve().parents[4]
    / "data"
    / "duckdb"
    / "mobs"
    / "genus_data_processed.csv"
)
DEFAULT_BUCKET = "trilogy_public_models"
DEFAULT_OBJECT = "duckdb/mobs/genus_data_processed.csv"

# mirrors `type url_image string['\S+://\S+']` from pytrilogy's std.net
URL_PATTERN = re.compile(r"\S+://\S+")
EXPECTED_FIELDS = ["genus", "image_url", "summary"]


def validate(path: Path) -> list[str]:
    """Return a list of human-readable problems; empty means safe to publish."""
    problems: list[str] = []
    csv.field_size_limit(10_000_000)
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_FIELDS:
            problems.append(
                f"header is {reader.fieldnames}, expected {EXPECTED_FIELDS}"
            )
            return problems
        seen: set[str] = set()
        rows = 0
        for row in reader:
            rows += 1
            # summaries carry embedded newlines, so a record index would not
            # match what an editor shows -- report the real line instead
            lineno = reader.line_num
            genus = (row.get("genus") or "").strip()
            image = (row.get("image_url") or "").strip()
            if all(key == value for key, value in row.items() if key):
                problems.append(f"line {lineno}: row is a copy of the header line")
                continue
            if not genus:
                problems.append(f"line {lineno}: empty genus")
            elif genus in seen:
                problems.append(f"line {lineno}: duplicate genus {genus!r}")
            else:
                seen.add(genus)
            if image and not URL_PATTERN.fullmatch(image):
                problems.append(
                    f"line {lineno}: genus {genus!r} has image_url {image!r}, "
                    f"which violates the url_image domain"
                )
        if not rows:
            problems.append("file contains no data rows")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--object", dest="object_name", default=DEFAULT_OBJECT)
    parser.add_argument("--project", default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate only; never contact GCS",
    )
    parser.add_argument(
        "--max-problems",
        type=int,
        default=20,
        help="how many validation problems to print before truncating",
    )
    args = parser.parse_args(argv)

    if not args.source.exists():
        print(
            f"error: {args.source} not found; run process_genus_data.py first",
            file=sys.stderr,
        )
        return 1

    problems = validate(args.source)
    if problems:
        print(
            f"error: {args.source} failed validation with {len(problems)} problem(s); "
            f"refusing to publish",
            file=sys.stderr,
        )
        for problem in problems[: args.max_problems]:
            print(f"  {problem}", file=sys.stderr)
        if len(problems) > args.max_problems:
            print(
                f"  ... and {len(problems) - args.max_problems} more", file=sys.stderr
            )
        return 1

    size_mb = args.source.stat().st_size / 1e6
    target = f"gs://{args.bucket}/{args.object_name}"
    print(f"validated {args.source} ({size_mb:,.2f} MB)")
    if args.dry_run:
        print(f"dry run: would upload -> {target}")
        return 0

    from google.cloud import storage

    client = storage.Client(project=args.project)
    blob = client.bucket(args.bucket).blob(args.object_name)
    blob.upload_from_filename(str(args.source), content_type="text/csv")
    print(f"uploaded -> {target}")
    print(
        "note: the public URL is served with Cache-Control max-age=3600, so readers "
        "(including CI) may see the previous copy for up to an hour."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
