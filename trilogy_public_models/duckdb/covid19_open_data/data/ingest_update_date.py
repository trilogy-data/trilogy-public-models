#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
"""Emit the COVID-19 Open Data publication time as a single-row Arrow table.

The upstream CSVs are static objects in a public GCS bucket, so the newest
``Last-Modified`` across the tables we ingest *is* the dataset's publication
time — and a HEAD request per table costs nothing next to the ~750MB the build
itself reads. This is the freshness anchor the published tiles compare against:
without a watermark concept, key-hash watermarks alone never mark an asset
stale and `trilogy refresh` would report everything up to date forever.

The dataset's final refresh was 2022-09-16, so in practice this returns a fixed
timestamp and a rebuilt tile stays fresh until upstream moves again.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen

import pyarrow as pa

BASE = "https://storage.googleapis.com/covid19-open-data/v3/"
TABLES = (
    "index",
    "geography",
    "demographics",
    "health",
    "epidemiology",
    "vaccinations",
    "hospitalizations",
)


def last_modified(table: str) -> datetime | None:
    request = Request(f"{BASE}{table}.csv", method="HEAD")
    with urlopen(request, timeout=30) as response:
        header = response.headers.get("Last-Modified")
    return parsedate_to_datetime(header) if header else None


def main() -> None:
    stamps = [ts for ts in (last_modified(t) for t in TABLES) if ts is not None]
    if not stamps:
        raise SystemExit(
            "no Last-Modified header on any upstream table; cannot establish a "
            "publication time to compare published tiles against"
        )
    updated_at = max(stamps).astimezone(timezone.utc)
    table = pa.table(
        {"data_update_date": pa.array([updated_at], type=pa.timestamp("us", tz="UTC"))}
    )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    main()
