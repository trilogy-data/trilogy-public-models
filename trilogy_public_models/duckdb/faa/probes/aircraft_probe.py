#!/usr/bin/env python
"""Freshness probe for the published FAA aircraft + aircraft_models on GCS.

Stale (`false`) iff the upstream FAA Releasable Aircraft zip has been
modified more recently than the GCS aircraft_v2.parquet object.
"""
import sys
import urllib.request
from datetime import datetime
from email.utils import parsedate_to_datetime

FAA_ZIP = "https://registry.faa.gov/database/ReleasableAircraft.zip"
GCS_AIRCRAFT = "https://storage.googleapis.com/trilogy_public_models/duckdb/faa/dimensions/aircraft_v2.parquet"
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


def _head(url: str) -> dict[str, str]:
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return dict(resp.headers)
    except Exception:
        return {}


def _last_modified(headers: dict[str, str]) -> datetime | None:
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def main() -> int:
    upstream = _last_modified(_head(FAA_ZIP))
    gcs = _last_modified(_head(GCS_AIRCRAFT))
    if upstream is None or gcs is None:
        print("true")
        return 0
    print("false" if upstream > gcs else "true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
