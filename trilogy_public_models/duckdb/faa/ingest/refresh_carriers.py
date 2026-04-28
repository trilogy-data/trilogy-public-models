#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "polars>=1.0",
#   "pyarrow",
# ]
# ///
"""Build ``dimensions/carriers_v2.parquet`` from the local flight parquets,
shaped to match ``faa/carrier.preql``.

There is no clean public CSV for the BTS L_UNIQUE_CARRIERS lookup —
``Download_Lookup.asp`` returns 500s for direct GETs and the HTML table
is rendered client-side. Since the dataset is tiny (a few dozen unique
codes across the entire 1987–present BTS history), we instead:

  1. Read every distinct ``carrier`` code from
     ``ingest/flights/flights_v2_*.parquet``.
  2. Resolve names from the in-script ``CARRIER_NAMES`` map.
  3. Fall back to a code-only row (name=code, nickname=code) and warn
     so unknown codes show up in the output rather than disappearing.
  4. Optionally merge a user-supplied CSV (``--manual carriers.csv`` with
     columns ``code,name,nickname``) for codes you want to override or
     add ahead of the next refresh.

Update ``CARRIER_NAMES`` in this file when BTS introduces a new code
(roughly once every few years). Or pass ``--manual``.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

INGEST_DIR = Path(__file__).parent
OUT_DIR = INGEST_DIR / "dimensions"
FLIGHTS_GLOB = INGEST_DIR / "flights" / "flights_v2_*.parquet"
CARRIERS_PARQUET = OUT_DIR / "carriers_v2.parquet"

# Sourced from BTS L_UNIQUE_CARRIERS history. Keys are the ``Reporting_Airline``
# / IATA code that BTS persists in the flight feed. Values are
# (legal_name, short_nickname). Add to this map when refreshing — BTS
# introduces new codes whenever a carrier (re)enters the on-time data.
CARRIER_NAMES: dict[str, tuple[str, str]] = {
    # Currently active majors / nationals (2020+)
    "AA": ("American Airlines", "American"),
    "AS": ("Alaska Airlines", "Alaska"),
    "B6": ("Jetblue Airways", "Jetblue"),
    "DL": ("Delta Air Lines", "Delta"),
    "F9": ("Frontier Airlines", "Frontier"),
    "G4": ("Allegiant Air", "Allegiant"),
    "HA": ("Hawaiian Airlines", "Hawaiian"),
    "NK": ("Spirit Air Lines", "Spirit"),
    "UA": ("United Airlines", "United"),
    "WN": ("Southwest Airlines", "Southwest"),
    # Currently active regional / connection partners
    "9E": ("Endeavor Air", "Endeavor"),
    "OH": ("PSA Airlines", "PSA"),
    "OO": ("SkyWest Airlines", "SkyWest"),
    "MQ": ("Envoy Air", "Envoy"),
    "YV": ("Mesa Airlines", "Mesa"),
    "YX": ("Republic Airways", "Republic"),
    "QX": ("Horizon Air", "Horizon"),
    "ZW": ("Air Wisconsin", "Air Wisconsin"),
    "C5": ("CommutAir", "CommutAir"),
    # Defunct / merged carriers that still appear in historical years
    "AL": ("USAir", "USAir"),
    "AQ": ("Aloha Airlines", "Aloha"),
    "CO": ("Continental Airlines", "Continental"),
    "DH": ("Atlantic Coast Airlines", "Atlantic Coast"),
    "EV": ("ExpressJet Airlines", "ExpressJet"),
    "FL": ("AirTran Airways", "AirTran"),
    "HP": ("America West Airlines", "America West"),
    "NW": ("Northwest Airlines", "Northwest"),
    "RU": ("Continental Express", "Continental Express"),
    "TW": ("Trans World Airlines", "TWA"),
    "TZ": ("ATA Airlines", "ATA"),
    "US": ("US Airways", "USAir"),
    "XE": ("ExpressJet Airlines", "ExpressJet"),
    "VX": ("Virgin America", "Virgin America"),
    "PA": ("Pan American World Airways", "Pan Am"),
    "EA": ("Eastern Air Lines", "Eastern"),
    "PI": ("Piedmont Aviation", "Piedmont"),
    "PS": ("Pacific Southwest Airlines", "PSA"),
    "ML": ("Midway Airlines", "Midway"),
    "OZ": ("Ozark Air Lines", "Ozark"),
    "WA": ("Western Airlines", "Western"),
    "NC": ("North Central Airlines", "North Central"),
    "RC": ("Republic Airlines", "Republic (Old)"),
}


def carriers_from_flights(flights_glob: Path) -> list[str]:
    pattern = str(flights_glob)
    matches = list(flights_glob.parent.glob(flights_glob.name))
    if not matches:
        raise FileNotFoundError(
            f"no flight parquets matched {pattern}; refresh_flights.py first"
        )
    df = (
        pl.scan_parquet(pattern)
        .select(pl.col("carrier").unique())
        .collect()
    )
    codes = sorted({c for c in df["carrier"].to_list() if c})
    return codes


def load_manual(path: Path) -> dict[str, tuple[str, str]]:
    df = pl.read_csv(path)
    cols = {c.strip().lower(): c for c in df.columns}
    for needed in ("code", "name", "nickname"):
        if needed not in cols:
            raise ValueError(
                f"{path} must have columns code,name,nickname (got {df.columns})"
            )
    out: dict[str, tuple[str, str]] = {}
    for row in df.iter_rows(named=True):
        code = (row[cols["code"]] or "").strip()
        if not code:
            continue
        out[code] = (
            (row[cols["name"]] or "").strip() or code,
            (row[cols["nickname"]] or "").strip() or code,
        )
    return out


def build_carriers(
    codes: list[str], overrides: dict[str, tuple[str, str]]
) -> pa.Table:
    rows = []
    unknown = []
    for code in codes:
        if code in overrides:
            name, nickname = overrides[code]
        elif code in CARRIER_NAMES:
            name, nickname = CARRIER_NAMES[code]
        else:
            name, nickname = code, code
            unknown.append(code)
        rows.append({"code": code, "name": name, "nickname": nickname})
    if unknown:
        print(
            f"WARNING: {len(unknown)} carrier code(s) had no name mapping "
            f"(stored as code-only): {unknown}\n"
            "  Add them to CARRIER_NAMES or pass --manual.",
            file=sys.stderr,
        )
    df = pl.DataFrame(rows, schema={"code": pl.Utf8, "name": pl.Utf8, "nickname": pl.Utf8})
    return df.sort("code").to_arrow()


def write_parquet(table: pa.Table, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    pq.write_table(
        table,
        tmp,
        compression="zstd",
        compression_level=9,
        write_statistics=True,
    )
    tmp.replace(dest)
    print(f"  wrote {dest} ({dest.stat().st_size / 1e3:.1f} KB, {table.num_rows:,} rows)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flights-glob",
        type=Path,
        default=FLIGHTS_GLOB,
        help="glob for local flight parquets (default: ingest/flights/flights_v2_*.parquet)",
    )
    parser.add_argument(
        "--manual",
        type=Path,
        default=None,
        help="optional CSV with columns code,name,nickname to override CARRIER_NAMES",
    )
    args = parser.parse_args(argv)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    codes = carriers_from_flights(args.flights_glob)
    print(f"found {len(codes)} distinct carrier code(s) in flights")

    overrides: dict[str, tuple[str, str]] = {}
    if args.manual:
        overrides = load_manual(args.manual)
        print(f"loaded {len(overrides)} override row(s) from {args.manual}")

    table = build_carriers(codes, overrides)
    write_parquet(table, CARRIERS_PARQUET)

    print(
        f"done at {datetime.now().isoformat(timespec='seconds')} — "
        f"upload via publish_dimensions.py"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
