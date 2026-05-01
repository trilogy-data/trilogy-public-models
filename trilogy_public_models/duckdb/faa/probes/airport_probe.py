#!/usr/bin/env python
"""Freshness probe for the published FAA airports dimension on GCS.

NFDC publishes a new NASR cycle every 28 days. Stale (`false`) iff the
latest cycle date discovered on the index page is newer than the GCS
airports_v2.parquet object's Last-Modified.
"""
import re
import sys
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

INDEX_URL = (
    "https://www.faa.gov/air_traffic/flight_info/aeronav/aero_data/NASR_Subscription/"
)
GCS_AIRPORTS = "https://storage.googleapis.com/trilogy_public_models/duckdb/faa/dimensions/airports_v2.parquet"
CYCLE_RE = re.compile(r"NASR_Subscription/(\d{4}-\d{2}-\d{2})", re.IGNORECASE)
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


def _get_text(url: str) -> str | None:
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def _last_modified(headers: dict[str, str]) -> datetime | None:
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def _latest_cycle_date() -> datetime | None:
    text = _get_text(INDEX_URL)
    if not text:
        return None
    cycles = sorted(set(CYCLE_RE.findall(text)))
    today = datetime.now(timezone.utc).date().isoformat()
    eligible = [c for c in cycles if c <= today]
    pick = eligible[-1] if eligible else (cycles[0] if cycles else None)
    if pick is None:
        return None
    try:
        return datetime.strptime(pick, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def main() -> int:
    cycle = _latest_cycle_date()
    gcs = _last_modified(_head(GCS_AIRPORTS))
    if cycle is None or gcs is None:
        print("true")
        return 0
    print("false" if cycle > gcs else "true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
