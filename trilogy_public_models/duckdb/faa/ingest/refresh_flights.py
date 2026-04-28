#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "httpx",
#   "polars>=1.0",
#   "pyarrow",
# ]
# ///
"""Download BTS Reporting Carrier On-Time Performance data and produce one
parquet per calendar year under faa/ingest/flights/flights_{YEAR}.parquet,
conforming to the schema expected by faa/flight.preql.

Source: https://transtats.bts.gov/DatabaseInfo.asp?QO_VQ=EFD
Zip layout:
  https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YEAR}_{MONTH}.zip

Default range is the two most recent complete calendar years. Pass
``--full-history`` to pull 1987-10 through the last complete month, or
override with --start/--end (YYYY-MM). Output is written incrementally one
month per row-group, one file per year, so the full historical pull does
not need to fit in memory and consumers can fan out parallel fetches.

id2 is assigned monotonically across the whole range in chronological
order, so each yearly file holds a disjoint contiguous id2 range.

Output parquet schema (matches faa/flight.preql):
  id2, carrier, origin, destination, flight_num, flight_time, tail_num,
  flight_date, dep_time, arr_time, dep_delay, arr_delay, taxi_out,
  taxi_in, distance, cancelled, diverted

dep_time/arr_time are null when BTS did not record actuals (cancelled
before departure etc.); flight_date is the scheduled operation date and
is always populated, making it the natural partition key.
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from bts_ondemand import ON_DEMAND_TO_PREZIP, download_month as download_ondemand

# BTS on-time data begins in October 1987.
HISTORY_START = (1987, 10)

BASE_URL = "https://transtats.bts.gov/PREZIP"
FILENAME = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

INGEST_DIR = Path(__file__).parent
RAW_DIR = INGEST_DIR / "_raw"
OUT_DIR = INGEST_DIR / "flights"
YEAR_FILENAME = "flights_v2_{year}.parquet"
WATERMARK_PATH = INGEST_DIR / "watermark.parquet"

# Parquet layout tuned for DuckDB-WASM. Three cheap wins, measured on
# flights_2024 (101.3 MB baseline):
#  - DELTA_BINARY_PACKED on id2: ids are assigned monotonically as rows
#    arrive, so deltas are tiny. Cuts id2 from ~10 MB/file to <2 MB.
#  - Timestamp[s] on dep_time/arr_time: BTS records minute precision, so
#    the 6 zero microseconds we currently store are pure waste (small win,
#    ~0.7 MB).
#  - Bloom filters on tail_num + flight_num: turns point lookups (e.g.
#    "show me N12345's history") into row-group skips. Cost is negligible
#    once NDV is a sane fraction of the column cardinality.
# Net: ~95 MB/file, zstd level 9.
# Row-group size: WASM does separate HTTP range requests per column per
# row group it touches, so RG count drives per-query latency. Within-file
# pushdown only helps for date predicates that don't already match the
# year-sharded file boundary, and most analytical queries hit those big
# scans regardless — so optimise for HTTP roundtrips, not granularity.
# 2M rows ≈ 3-4 RGs per file (vs ~18 at 400k), measured ~4 % bigger but
# halves request count for the typical full-year scan.
# We deliberately do NOT sort within the file — the natural BTS row order
# leaves id2 monotonic (so DELTA stays optimal) and ZSTD already exploits
# the column patterns it produces; explicit sorts on (flight_date,
# carrier, origin, destination) measured ~17 % bigger.
ROW_GROUP_SIZE = 2_000_000
BLOOM_COLUMNS = ("tail_num", "flight_num")
DICT_COLUMNS = (
    "carrier",
    "origin",
    "destination",
    "flight_num",
    "tail_num",
    "flight_date",
    "dep_time",
    "arr_time",
    "flight_time",
    "dep_delay",
    "arr_delay",
    "taxi_out",
    "taxi_in",
    "distance",
    "cancelled",
    "diverted",
)
COLUMN_ENCODING = {"id2": "DELTA_BINARY_PACKED"}

# Subset of BTS columns we actually need for the flight datasource.
NEEDED_COLUMNS = [
    "FlightDate",
    "IATA_CODE_Reporting_Airline",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "AirTime",
    "Distance",
    "Cancelled",
    "Diverted",
]


@dataclass(frozen=True)
class MonthKey:
    year: int
    month: int

    @property
    def filename(self) -> str:
        return FILENAME.format(year=self.year, month=self.month)

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"


def default_range() -> tuple[MonthKey, MonthKey]:
    today = date.today()
    # Use the two most recent complete calendar years.
    end_year = today.year - 1
    start_year = end_year - 1
    return MonthKey(start_year, 1), MonthKey(end_year, 12)


def last_complete_month(today: date | None = None) -> MonthKey:
    today = today or date.today()
    y, m = today.year, today.month - 1
    if m == 0:
        y, m = y - 1, 12
    return MonthKey(y, m)


def parse_month(text: str) -> MonthKey:
    y, m = text.split("-")
    return MonthKey(int(y), int(m))


def iter_months(start: MonthKey, end: MonthKey):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield MonthKey(y, m)
        m += 1
        if m > 12:
            m = 1
            y += 1


def _is_valid_zip(path: Path) -> bool:
    """Cheap zip sniff: first two bytes must be 'PK'.

    BTS sometimes returns the TranStats homepage (HTML, ~107 KB) for months
    that aren't in the PREZIP bundle — notably all of 1990-1999. This lets us
    detect and reject those before caching.
    """
    if not path.exists() or path.stat().st_size < 4:
        return False
    with path.open("rb") as fd:
        return fd.read(2) == b"PK"


def download_zip(client: httpx.Client, key: MonthKey, dest: Path) -> Path:
    # Use the cache only if it's a real zip. HTML fallbacks from past runs
    # get evicted and retried via the on-demand form.
    if dest.exists():
        if _is_valid_zip(dest):
            print(f"skip (cached): {dest.name}")
            return dest
        print(f"evicting bad cache: {dest.name}")
        dest.unlink()

    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading PREZIP: {key.url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with client.stream("GET", key.url, timeout=None) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fd:
            for chunk in resp.iter_bytes(chunk_size=1 << 15):
                fd.write(chunk)
    if _is_valid_zip(tmp):
        tmp.replace(dest)
        return dest

    # PREZIP returned the HTML fallback. Fall back to the on-demand form.
    tmp.unlink(missing_ok=True)
    print(f"  PREZIP returned non-zip; falling back to on-demand form for {key.year}-{key.month:02d}")
    download_ondemand(client, key.year, key.month, dest)
    return dest


def read_month_csv(zip_path: Path) -> pl.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"no CSV inside {zip_path.name}")
        with zf.open(csv_names[0]) as fd:
            raw = fd.read()
    # BTS CSVs are UTF-8/Latin-1, comma-delimited, quoted strings.
    df = pl.read_csv(
        io.BytesIO(raw),
        has_header=True,
        infer_schema_length=0,  # read as strings; we'll coerce below
        ignore_errors=False,
        encoding="utf8-lossy",
    )

    # On-demand CSVs use snake-case uppercase headers (FL_DATE, OP_CARRIER, …)
    # whereas PREZIP uses mixed case (FlightDate, IATA_CODE_Reporting_Airline, …).
    # Rename to the PREZIP convention so the rest of the pipeline stays uniform.
    rename = {k: v for k, v in ON_DEMAND_TO_PREZIP.items() if k in df.columns}
    if rename:
        df = df.rename(rename)

    # On-demand encodes FlightDate as ``M/D/YYYY 12:00:00 AM``; PREZIP uses
    # ``YYYY-MM-DD``. Coerce on-demand rows to ISO so build_datetime's strptime
    # works unchanged.
    if "FlightDate" in df.columns:
        needs_convert = df.select(
            pl.col("FlightDate").str.contains(r"^\d{1,2}/\d{1,2}/\d{4}")
        ).item(0, 0)
        if needs_convert:
            df = df.with_columns(
                pl.col("FlightDate")
                .str.strptime(pl.Date, format="%m/%d/%Y %I:%M:%S %p", strict=False)
                .dt.strftime("%Y-%m-%d")
                .alias("FlightDate")
            )

    missing = [c for c in NEEDED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{zip_path.name} missing expected columns: {missing}")
    return df.select(NEEDED_COLUMNS)


def build_datetime(date_col: str, hhmm_col: str, out_col: str) -> pl.Expr:
    """Combine a yyyy-mm-dd date column with an 'hhmm' time column into a datetime.

    BTS encodes 2400 as midnight end-of-day; normalize to 0000 the next day.
    Returns null when the hhmm value is missing.
    """
    hhmm = (
        pl.col(hhmm_col)
        .str.strip_chars()
        .cast(pl.Int32, strict=False)
        .fill_null(-1)
    )
    rolled = pl.when(hhmm == 2400).then(0).otherwise(hhmm)
    extra_days = pl.when(hhmm == 2400).then(1).otherwise(0)
    hours = (rolled // 100).cast(pl.Int32)
    minutes = (rolled % 100).cast(pl.Int32)
    base = pl.col(date_col).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    return (
        pl.when(hhmm < 0)
        .then(None)
        .otherwise(
            base.dt.offset_by(
                (extra_days.cast(pl.Utf8) + "d")
            ).cast(pl.Datetime("us"))
            + pl.duration(hours=hours, minutes=minutes)
        )
        .alias(out_col)
    )


def transform(df: pl.DataFrame) -> pl.DataFrame:
    # Prefer actual times; fall back to scheduled where actuals are absent.
    dep = build_datetime("FlightDate", "DepTime", "dep_time")
    arr = build_datetime("FlightDate", "ArrTime", "arr_time")

    yn = lambda col: (
        pl.when(pl.col(col).str.strip_chars() == "1.00")
        .then(pl.lit("Y"))
        .when(pl.col(col).str.strip_chars() == "1")
        .then(pl.lit("Y"))
        .otherwise(pl.lit("N"))
    )

    to_int = lambda col: (
        pl.col(col)
        .str.strip_chars()
        .cast(pl.Float64, strict=False)
        .cast(pl.Int32, strict=False)
    )

    out = df.with_columns(
        [
            pl.coalesce(
                pl.col("IATA_CODE_Reporting_Airline").str.strip_chars(),
                pl.col("Reporting_Airline").str.strip_chars(),
            ).alias("carrier"),
            pl.col("Origin").str.strip_chars().alias("origin"),
            pl.col("Dest").str.strip_chars().alias("destination"),
            pl.col("Flight_Number_Reporting_Airline")
            .str.strip_chars()
            .alias("flight_num"),
            pl.col("Tail_Number").str.strip_chars().alias("tail_num"),
            to_int("AirTime").fill_null(0).alias("flight_time"),
            to_int("DepDelay").fill_null(0).alias("dep_delay"),
            to_int("ArrDelay").fill_null(0).alias("arr_delay"),
            to_int("TaxiOut").fill_null(0).alias("taxi_out"),
            to_int("TaxiIn").fill_null(0).alias("taxi_in"),
            to_int("Distance").fill_null(0).alias("distance"),
            yn("Cancelled").alias("cancelled"),
            yn("Diverted").alias("diverted"),
            pl.col("FlightDate")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("flight_date"),
            dep,
            arr,
        ]
    )

    # Flights that arrive next day have arr_time < dep_time; shift by 1 day.
    # dep_time / arr_time stay null for rows where BTS did not record the actuals
    # (cancelled before departure, cancelled before arrival, etc.) — use
    # flight_date for always-populated time slicing.
    out = out.with_columns(
        pl.when(
            pl.col("arr_time").is_not_null()
            & pl.col("dep_time").is_not_null()
            & (pl.col("arr_time") < pl.col("dep_time"))
        )
        .then(pl.col("arr_time").dt.offset_by("1d"))
        .otherwise(pl.col("arr_time"))
        .alias("arr_time")
    )

    return out.select(
        "carrier",
        "origin",
        "destination",
        "flight_num",
        "flight_time",
        "tail_num",
        "flight_date",
        "dep_time",
        "arr_time",
        "dep_delay",
        "arr_delay",
        "taxi_out",
        "taxi_in",
        "distance",
        "cancelled",
        "diverted",
    )


def prepare_for_write(frame: pl.DataFrame) -> pa.Table:
    """Cast dep_time/arr_time to seconds precision, return an arrow table.

    BTS records minute precision, so the microseconds we currently store
    are always zero — Timestamp[s] is lossless. Row order is left as
    polars produced it; that order keeps id2 monotonic, which is what
    makes DELTA_BINARY_PACKED collapse it to near-zero.
    """
    table = frame.to_arrow()
    new_fields = []
    for field in table.schema:
        if field.name in ("dep_time", "arr_time"):
            new_fields.append(pa.field(field.name, pa.timestamp("s")))
        else:
            new_fields.append(field)
    return table.cast(pa.schema(new_fields), safe=False)


def make_writer(path: Path, schema: pa.Schema) -> pq.ParquetWriter:
    return pq.ParquetWriter(
        path,
        schema,
        compression="zstd",
        compression_level=9,
        use_dictionary=list(DICT_COLUMNS),
        column_encoding=dict(COLUMN_ENCODING),
        write_statistics=True,
        write_page_index=True,
        bloom_filter_options={"columns": {c: True for c in BLOOM_COLUMNS}},
    )


def main(argv: list[str] | None = None) -> int:
    start_default, end_default = default_range()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start",
        default=os.environ.get("BTS_START", f"{start_default.year}-{start_default.month:02d}"),
        help="first month to download, YYYY-MM",
    )
    parser.add_argument(
        "--end",
        default=os.environ.get("BTS_END", f"{end_default.year}-{end_default.month:02d}"),
        help="last month to download, YYYY-MM",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help=f"override --start/--end to pull {HISTORY_START[0]}-{HISTORY_START[1]:02d} through the last complete month",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUT_DIR),
        help="directory to write yearly parquet files into",
    )
    args = parser.parse_args(argv)

    if args.full_history:
        start = MonthKey(*HISTORY_START)
        end = last_complete_month()
    else:
        start = parse_month(args.start)
        end = parse_month(args.end)
    months = list(iter_months(start, end))
    print(f"processing {len(months)} month(s) from {start.year}-{start.month:02d} to {end.year}-{end.month:02d}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    writer: pq.ParquetWriter | None = None
    writer_year: int | None = None
    writer_path: Path | None = None
    writer_rows = 0
    next_id = 0
    total_rows = 0
    year_files: list[tuple[int, Path, int]] = []

    def close_writer() -> None:
        nonlocal writer, writer_year, writer_path, writer_rows
        if writer is None:
            return
        writer.close()
        assert writer_path is not None and writer_year is not None
        year_files.append((writer_year, writer_path, writer_rows))
        print(
            f"  closed {writer_path.name}: {writer_rows:,} rows, "
            f"{writer_path.stat().st_size / 1e6:.1f} MB"
        )
        writer = None
        writer_year = None
        writer_path = None
        writer_rows = 0

    try:
        with httpx.Client(follow_redirects=True, http2=False, timeout=60.0) as client:
            for key in months:
                zip_path = RAW_DIR / key.filename
                try:
                    download_zip(client, key, zip_path)
                except httpx.HTTPError as exc:
                    print(f"WARNING: failed to download {key.filename}: {exc}", file=sys.stderr)
                    continue
                try:
                    raw = read_month_csv(zip_path)
                except Exception as exc:
                    print(f"WARNING: failed to read {zip_path.name}: {exc}", file=sys.stderr)
                    continue

                frame = transform(raw)
                ids = pl.Series("id2", range(next_id, next_id + frame.height), dtype=pl.Int64)
                frame = frame.with_columns(ids).select(
                    "id2",
                    "carrier",
                    "origin",
                    "destination",
                    "flight_num",
                    "flight_time",
                    "tail_num",
                    "flight_date",
                    "dep_time",
                    "arr_time",
                    "dep_delay",
                    "arr_delay",
                    "taxi_out",
                    "taxi_in",
                    "distance",
                    "cancelled",
                    "diverted",
                )
                next_id += frame.height
                total_rows += frame.height

                table = prepare_for_write(frame)
                if writer is None or writer_year != key.year:
                    close_writer()
                    writer_path = out_dir / YEAR_FILENAME.format(year=key.year)
                    writer = make_writer(writer_path, table.schema)
                    writer_year = key.year
                    writer_rows = 0
                else:
                    # Normalize the schema so row-groups concatenate cleanly
                    # (arrow infers types per chunk; monthly BTS files vary).
                    table = table.cast(writer.schema, safe=False)
                writer.write_table(table, row_group_size=ROW_GROUP_SIZE)
                writer_rows += frame.height
                print(f"  +{frame.height:>8,} rows from {key.year}-{key.month:02d} (running total {total_rows:,})")
    finally:
        close_writer()

    if total_rows == 0:
        print("no data collected, aborting", file=sys.stderr)
        return 1

    total_bytes = sum(p.stat().st_size for _, p, _ in year_files)
    print(
        f"wrote {total_rows:,} rows across {len(year_files)} year file(s) "
        f"in {out_dir} ({total_bytes / 1e6:.1f} MB total)"
    )

    # Single-row watermark parquet: referenced by flight_watermark in
    # flight.preql and used by every aggregate's freshness_by check.
    now = datetime.now().replace(microsecond=0)
    watermark_table = pa.table({"data_through": pa.array([now], type=pa.timestamp("us"))})
    pq.write_table(watermark_table, WATERMARK_PATH, compression="zstd")
    print(f"wrote watermark {WATERMARK_PATH} (data_through={now.isoformat()})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
