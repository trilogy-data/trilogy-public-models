#!/usr/bin/env python
"""Freshness probe for the published FAA carriers dimension on GCS.

Carriers are derived from flights (refresh_carriers.py reads the flights
parquet for the carrier code list). Stale (`false`) iff the GCS carriers
parquet's Last-Modified is older than the GCS flight_watermark — i.e.
flights have been republished since carriers were last built.
"""
import sys
import urllib.request
from datetime import datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException

GCS_CARRIERS = "https://storage.googleapis.com/trilogy_public_models/duckdb/faa/dimensions/carriers_v2.parquet"
GCS_WATERMARK = (
    "https://storage.googleapis.com/trilogy_public_models/duckdb/faa/watermark.parquet"
)
UA = {"User-Agent": "trilogy-faa-probe/1.0"}


def _head(url: str) -> dict[str, str]:
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return dict(resp.headers)
    except (OSError, HTTPException):
        return {}


def _last_modified(headers: dict[str, str]) -> datetime | None:
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None


def main() -> int:
    carriers = _last_modified(_head(GCS_CARRIERS))
    watermark = _last_modified(_head(GCS_WATERMARK))
    if carriers is None or watermark is None:
        print("true")
        return 0
    print("false" if watermark > carriers else "true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
