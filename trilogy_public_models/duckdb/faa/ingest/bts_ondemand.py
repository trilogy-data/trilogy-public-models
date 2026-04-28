"""On-demand BTS flight-data fetch via the TranStats form POST.

The PREZIP bundle is missing every month from 1990-01 through 1999-12 (BTS
silently serves the TranStats homepage instead of a zip). This module drives
the on-demand download form at DL_SelectFields.aspx for those months.

The CSV inside the on-demand zip uses a different header convention than
PREZIP (snake_case uppercase) and a different FlightDate string format
(``M/D/YYYY 12:00:00 AM``). ``refresh_flights.read_month_csv`` normalizes
both, so downstream callers never need to care which source produced a zip.
"""
from __future__ import annotations

import re
from html import unescape
from pathlib import Path
from urllib.parse import urlencode

import httpx

FORM_URL = (
    "https://www.transtats.bts.gov/DL_SelectFields.aspx"
    "?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
)

# Checkbox names to request in the on-demand form. Mirror the columns
# NEEDED_COLUMNS in refresh_flights.py, using the on-demand naming.
FIELDS: tuple[str, ...] = (
    "YEAR",
    "QUARTER",
    "MONTH",
    "DAY_OF_MONTH",
    "DAY_OF_WEEK",
    "FL_DATE",
    "OP_UNIQUE_CARRIER",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "TAIL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "TAXI_OUT",
    "TAXI_IN",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "AIR_TIME",
    "DISTANCE",
    "CANCELLED",
    "DIVERTED",
)

# Mapping from on-demand CSV headers to the PREZIP header names used
# throughout refresh_flights.py. Applied in read_month_csv after detecting
# which variant the zip contains.
ON_DEMAND_TO_PREZIP: dict[str, str] = {
    "FL_DATE": "FlightDate",
    "OP_CARRIER": "IATA_CODE_Reporting_Airline",
    "OP_UNIQUE_CARRIER": "Reporting_Airline",
    "OP_CARRIER_FL_NUM": "Flight_Number_Reporting_Airline",
    "TAIL_NUM": "Tail_Number",
    "ORIGIN": "Origin",
    "DEST": "Dest",
    "CRS_DEP_TIME": "CRSDepTime",
    "DEP_TIME": "DepTime",
    "DEP_DELAY": "DepDelay",
    "TAXI_OUT": "TaxiOut",
    "TAXI_IN": "TaxiIn",
    "CRS_ARR_TIME": "CRSArrTime",
    "ARR_TIME": "ArrTime",
    "ARR_DELAY": "ArrDelay",
    "AIR_TIME": "AirTime",
    "DISTANCE": "Distance",
    "CANCELLED": "Cancelled",
    "DIVERTED": "Diverted",
}


def _extract_hidden(html: str, name: str) -> str:
    pattern = rf"<input[^>]*name=\"?{re.escape(name)}\"?[^>]*value=\"([^\"]*)\""
    m = re.search(pattern, html, re.IGNORECASE)
    if not m:
        raise RuntimeError(f"hidden field {name!r} not found on form")
    return unescape(m.group(1))


def download_month(client: httpx.Client, year: int, month: int, dest: Path) -> None:
    """Fetch a single month of on-time performance data via the on-demand form.

    Writes the returned zip to *dest* atomically. Raises on HTTP errors or if
    the response isn't a valid zip (e.g. the TranStats homepage fallback).
    """
    form_resp = client.get(FORM_URL)
    form_resp.raise_for_status()
    html = form_resp.text

    form: dict[str, str] = {
        "__VIEWSTATE": _extract_hidden(html, "__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": _extract_hidden(html, "__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": _extract_hidden(html, "__EVENTVALIDATION"),
        "affiliate": _extract_hidden(html, "affiliate"),
        "cboGeography": "All",
        "cboYear": str(year),
        "cboPeriod": str(month),
        # chkDownloadZip would say "just hand me the PREZIP bundle" — which is
        # broken for 1990-1999. Omit it so the server generates a fresh CSV.
        "btnDownload": "Download",
    }
    for field in FIELDS:
        form[field] = "on"

    body = urlencode(form)
    resp = client.post(
        FORM_URL,
        content=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    if resp.content[:2] != b"PK":
        ct = resp.headers.get("content-type", "?")
        raise RuntimeError(
            f"on-demand {year}-{month:02d} returned non-zip "
            f"(content-type={ct}, {len(resp.content):,} bytes)"
        )

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(resp.content)
    tmp.replace(dest)
