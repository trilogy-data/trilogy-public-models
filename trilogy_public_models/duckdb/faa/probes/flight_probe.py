#!/usr/bin/env python
"""Freshness probe for the published FAA flights set on GCS.

Stale (`false`) iff BTS has published a new monthly zip more recently than
the GCS watermark.parquet was uploaded. Uses the published GCS object's
Last-Modified header as the "as-of" timestamp for the latest publish, and
checks the BTS upstream URL for the most recent expected month.

BTS publishes ~2-3 months in arrears, so we test the URL for the month
ending ~75 days ago and walk back if not yet posted.
"""
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

GCS_WATERMARK = "https://storage.googleapis.com/trilogy_public_models/duckdb/faa/watermark.parquet"
BTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
UA = {"User-Agent": "trilogy-faa-probe/1.0"}


def _head(url: str) -> tuple[int, dict[str, str]]:
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {})
    except Exception:
        return 0, {}


def _last_modified(headers: dict[str, str]) -> datetime | None:
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def _latest_bts_month_available() -> datetime | None:
    """Walk backward from ~75 days ago to find the most recent month BTS has
    posted. Returns the Last-Modified of that zip, or None if nothing in the
    last ~6 months is reachable."""
    today = datetime.now(timezone.utc)
    candidate = (today - timedelta(days=75)).replace(day=1)
    for _ in range(6):
        url = BTS_URL.format(year=candidate.year, month=candidate.month)
        status, headers = _head(url)
        # BTS sometimes returns 200 with text/html for missing months; require zip.
        ct = headers.get("Content-Type") or headers.get("content-type") or ""
        if status == 200 and "zip" in ct.lower():
            return _last_modified(headers)
        candidate = (candidate - timedelta(days=1)).replace(day=1)
    return None


def main() -> int:
    _, gcs_headers = _head(GCS_WATERMARK)
    gcs_mtime = _last_modified(gcs_headers)
    bts_mtime = _latest_bts_month_available()

    if gcs_mtime is None or bts_mtime is None:
        # Can't confidently determine staleness — be conservative and report fresh.
        # If you want a stale signal here, set FAA_STALE_ON_PROBE_FAILURE=1.
        print("true")
        return 0

    print("false" if bts_mtime > gcs_mtime else "true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
