#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "httpx",
#   "polars>=1.0",
#   "pyarrow",
# ]
# ///
"""Download the FAA Releasable Aircraft Database and produce
``dimensions/aircraft_v2.parquet`` and ``dimensions/aircraft_models_v2.parquet``
shaped to match ``faa/aircraft.preql`` and ``faa/aircraft_model.preql``.

Source: https://registry.faa.gov/database/ReleasableAircraft.zip

The zip ships several CSVs; we consume three:

* ``MASTER.txt``  — one row per currently-registered N-number.
* ``DEREG.txt``   — historical deregistrations going back to ~1945 (~382k
                    rows). Pulling this in lifts join coverage on flights
                    from older years dramatically — without it, retired
                    tail numbers don't match.
* ``ACFTREF.txt`` — one row per FAA make/model code (the join key between
                    aircraft and the aircraft model dimension).

The FAA MASTER N-NUMBER is the bare digits/letters (e.g. ``12345``); BTS
flight data carries the painted tail number (``N12345``). We prepend the
``N`` so ``aircraft.tail_num`` joins cleanly to ``flight.tail_num``.

When MASTER and DEREG both carry rows for the same N-number (registration
revoked then re-issued), we prefer the active MASTER record. Within
DEREG, duplicates collapse to the most recent CERT-ISSUE-DATE.

Re-runs are safe: the zip is cached under ``ingest/_raw_dim/`` and the
parquets are written atomically. Pass ``--no-historic`` to skip DEREG.
"""
from __future__ import annotations

import argparse
import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

SOURCE_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"

INGEST_DIR = Path(__file__).parent
RAW_DIR = INGEST_DIR / "_raw_dim"
OUT_DIR = INGEST_DIR / "dimensions"
ZIP_NAME = "ReleasableAircraft.zip"
FLIGHTS_GLOB = INGEST_DIR / "flights" / "flights_v2_*.parquet"

# status_code value used to mark rows backfilled from the flight fact —
# tail_num was observed in BTS but not present in MASTER or DEREG. Lets
# downstream queries filter ``WHERE status_code <> 'U'`` for pure FAA
# truth without losing the join coverage.
INFERRED_STATUS_CODE = "U"

AIRCRAFT_PARQUET = OUT_DIR / "aircraft_v2.parquet"
AIRCRAFT_MODELS_PARQUET = OUT_DIR / "aircraft_models_v2.parquet"

# registry.faa.gov returns 403 to unidentified clients.
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}


def _is_valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with path.open("rb") as fd:
        return fd.read(2) == b"PK"


def download_zip(client: httpx.Client, dest: Path, force: bool) -> Path:
    if dest.exists() and not force:
        if _is_valid_zip(dest):
            print(f"skip (cached): {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
            return dest
        print(f"evicting bad cache: {dest.name}")
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {SOURCE_URL}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with client.stream("GET", SOURCE_URL, timeout=None) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fd:
            for chunk in resp.iter_bytes(chunk_size=1 << 15):
                fd.write(chunk)
    if not _is_valid_zip(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{SOURCE_URL} returned non-zip content")
    tmp.replace(dest)
    print(f"  wrote {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def _read_member(zf: zipfile.ZipFile, name: str) -> pl.DataFrame:
    """FAA CSVs have wide quoted fields padded with trailing spaces (e.g.
    ``"A"                 ``), which polars rejects as malformed. pyarrow's
    CSV reader tolerates them; we read with pyarrow then hand the table
    over to polars for the rest of the pipeline.

    DEREG.txt also has a few rows with an extra column (header has a
    trailing comma, sentinel rows occasionally have one extra value). We
    skip those rather than failing the whole read."""
    candidates = [n for n in zf.namelist() if n.lower().endswith(name.lower())]
    if not candidates:
        raise RuntimeError(f"{name} not found inside zip; got {zf.namelist()}")
    with zf.open(candidates[0]) as fd:
        raw = fd.read()

    skipped: list[object] = []

    def _on_invalid(row: object) -> str:
        skipped.append(row)
        return "skip"

    table = pa_csv.read_csv(
        io.BytesIO(raw),
        read_options=pa_csv.ReadOptions(encoding="utf-8"),
        parse_options=pa_csv.ParseOptions(
            delimiter=",",
            quote_char='"',
            ignore_empty_lines=True,
            invalid_row_handler=_on_invalid,
        ),
        convert_options=pa_csv.ConvertOptions(
            strings_can_be_null=True,
            null_values=[""],
        ),
    )
    if skipped:
        print(f"  warning: skipped {len(skipped)} malformed row(s) in {candidates[0]}")
    # Force every column to string for uniform downstream coercion.
    string_table = pa.Table.from_arrays(
        [c.cast(pa.string()) for c in table.columns],
        names=table.column_names,
    )
    return pl.from_arrow(string_table)


def _strip_str(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Utf8).str.strip_chars()


def _to_int(name: str) -> pl.Expr:
    return (
        pl.col(name)
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Int64, strict=False)
        .fill_null(0)
    )


def _yyyymmdd_to_date(name: str) -> pl.Expr:
    cleaned = pl.col(name).cast(pl.Utf8).str.strip_chars()
    return (
        pl.when((cleaned.is_null()) | (cleaned == "") | (cleaned == "0"))
        .then(None)
        .otherwise(cleaned.str.strptime(pl.Date, "%Y%m%d", strict=False))
    )


def _ac_weight_class(name: str) -> pl.Expr:
    """ACFTREF AC-WEIGHT is encoded as "CLASS 1".."CLASS 4". The legacy
    parquet stored an integer (and was all zeros). Pull the class number
    where present, else 0."""
    cleaned = pl.col(name).cast(pl.Utf8).str.strip_chars()
    return cleaned.str.extract(r"(\d+)", 1).cast(pl.Int64, strict=False).fill_null(0)


AIRCRAFT_OUTPUT_COLUMNS = (
    "tail_num",
    "aircraft_serial",
    "aircraft_model_code",
    "aircraft_engine_code",
    "year_built",
    "aircraft_type_id",
    "aircraft_engine_type_id",
    "registrant_type_id",
    "name",
    "address1",
    "address2",
    "city",
    "state",
    "zip",
    "region",
    "county",
    "country",
    "certification",
    "status_code",
    "mode_s_code",
    "fract_owner",
    "last_action_date",
    "cert_issue_date",
    "air_worth_date",
)

# MASTER.txt → canonical column names. ``None`` means the source CSV has
# no equivalent and the field is filled with a default (0 for ints, null
# for strings/dates).
MASTER_COLUMN_MAP: dict[str, str | None] = {
    "tail_num": "N-NUMBER",
    "aircraft_serial": "SERIAL NUMBER",
    "aircraft_model_code": "MFR MDL CODE",
    "aircraft_engine_code": "ENG MFR MDL",
    "year_built": "YEAR MFR",
    "aircraft_type_id": "TYPE AIRCRAFT",
    "aircraft_engine_type_id": "TYPE ENGINE",
    "registrant_type_id": "TYPE REGISTRANT",
    "name": "NAME",
    "address1": "STREET",
    "address2": "STREET2",
    "city": "CITY",
    "state": "STATE",
    "zip": "ZIP CODE",
    "region": "REGION",
    "county": "COUNTY",
    "country": "COUNTRY",
    "certification": "CERTIFICATION",
    "status_code": "STATUS CODE",
    "mode_s_code": "MODE S CODE",
    "fract_owner": "FRACT OWNER",
    "last_action_date": "LAST ACTION DATE",
    "cert_issue_date": "CERT ISSUE DATE",
    "air_worth_date": "AIR WORTH DATE",
}

# DEREG.txt → canonical column names. DEREG omits the per-airframe type
# fields (TYPE AIRCRAFT/ENGINE/REGISTRANT) and FRACT OWNER; we backfill
# those with 0/null. DEREG also splits address into MAIL/PHYSICAL — we
# keep the mailing address to mirror MASTER's STREET semantics.
DEREG_COLUMN_MAP: dict[str, str | None] = {
    "tail_num": "N-NUMBER",
    "aircraft_serial": "SERIAL-NUMBER",
    "aircraft_model_code": "MFR-MDL-CODE",
    "aircraft_engine_code": "ENG-MFR-MDL",
    "year_built": "YEAR-MFR",
    "aircraft_type_id": None,
    "aircraft_engine_type_id": None,
    "registrant_type_id": None,
    "name": "NAME",
    "address1": "STREET-MAIL",
    "address2": "STREET2-MAIL",
    "city": "CITY-MAIL",
    "state": "STATE-ABBREV-MAIL",
    "zip": "ZIP-CODE-MAIL",
    "region": "REGION",
    "county": "COUNTY-MAIL",
    "country": "COUNTRY-MAIL",
    "certification": "CERTIFICATION",
    "status_code": "STATUS-CODE",
    "mode_s_code": "MODE-S-CODE",
    "fract_owner": None,
    "last_action_date": "LAST-ACT-DATE",
    "cert_issue_date": "CERT-ISSUE-DATE",
    "air_worth_date": "AIR-WORTH-DATE",
}

# Output column → polars dtype, used to fill missing-source columns.
INT_FIELDS = {
    "year_built",
    "aircraft_type_id",
    "aircraft_engine_type_id",
    "registrant_type_id",
}
DATE_FIELDS = {"last_action_date", "cert_issue_date", "air_worth_date"}


def _project_to_canonical(df: pl.DataFrame, mapping: dict[str, str | None]) -> pl.DataFrame:
    """Return a DataFrame with the canonical AIRCRAFT_OUTPUT_COLUMNS, drawn
    from ``df`` via ``mapping``. Columns mapped to ``None`` get the right
    null/zero default.

    Source column-name lookup is case- and whitespace-insensitive.
    """
    cols = {c.strip().upper(): c for c in df.columns}

    def lookup(src: str) -> str:
        try:
            return cols[src.upper()]
        except KeyError as exc:
            raise KeyError(
                f"source CSV missing expected column {src!r}; have {sorted(cols)[:20]}…"
            ) from exc

    exprs: list[pl.Expr] = []
    for out in AIRCRAFT_OUTPUT_COLUMNS:
        src = mapping[out]
        if out == "tail_num":
            assert src is not None
            exprs.append((pl.lit("N") + _strip_str(lookup(src))).alias("tail_num"))
            continue
        if src is None:
            if out in INT_FIELDS:
                exprs.append(pl.lit(0, dtype=pl.Int64).alias(out))
            elif out in DATE_FIELDS:
                exprs.append(pl.lit(None, dtype=pl.Date).alias(out))
            else:
                exprs.append(pl.lit(None, dtype=pl.Utf8).alias(out))
            continue
        col_name = lookup(src)
        if out in INT_FIELDS:
            exprs.append(_to_int(col_name).alias(out))
        elif out in DATE_FIELDS:
            exprs.append(_yyyymmdd_to_date(col_name).alias(out))
        else:
            exprs.append(_strip_str(col_name).alias(out))

    return df.select(exprs).filter(pl.col("tail_num") != "N")


def _flight_tail_nums(flights_glob: Path) -> pl.DataFrame | None:
    """Return distinct, non-empty tail_nums from local flight parquets, or
    None if no flight files are present (e.g. fresh checkout)."""
    if not list(flights_glob.parent.glob(flights_glob.name)):
        return None
    return (
        pl.scan_parquet(str(flights_glob))
        .select(pl.col("tail_num").str.strip_chars().alias("tail_num"))
        .filter((pl.col("tail_num").is_not_null()) & (pl.col("tail_num") != ""))
        .unique()
        .collect()
    )


def _stub_rows_for(missing: pl.DataFrame) -> pl.DataFrame:
    """Build stub aircraft rows for tail_nums observed in flights but
    absent from the FAA registry. Only ``tail_num`` and ``status_code``
    are populated — other fields are null/0 sentinels matching the
    canonical schema dtypes."""
    n = missing.height
    return missing.select(
        [
            pl.col("tail_num"),
            *[
                (
                    pl.lit(0, dtype=pl.Int64).alias(c)
                    if c in INT_FIELDS
                    else pl.lit(None, dtype=pl.Date).alias(c)
                    if c in DATE_FIELDS
                    else (
                        pl.lit(INFERRED_STATUS_CODE, dtype=pl.Utf8).alias(c)
                        if c == "status_code"
                        else pl.lit(None, dtype=pl.Utf8).alias(c)
                    )
                )
                for c in AIRCRAFT_OUTPUT_COLUMNS
                if c != "tail_num"
            ],
        ]
    ) if n else missing


def build_aircraft(
    master: pl.DataFrame,
    dereg: pl.DataFrame | None,
    flight_tails: pl.DataFrame | None,
) -> pa.Table:
    active = _project_to_canonical(master, MASTER_COLUMN_MAP)
    print(f"  active (MASTER): {active.height:,} rows")

    if dereg is None:
        combined = active
    else:
        historic = _project_to_canonical(dereg, DEREG_COLUMN_MAP)
        # Within DEREG, multiple deregistrations can share an N-number
        # across decades. Keep the most recent by cert_issue_date.
        historic = (
            historic.sort("cert_issue_date", nulls_last=True)
            .unique(subset=["tail_num"], keep="last")
        )
        # Drop tail_nums that are already active in MASTER — the active
        # record is the right one for current and recent flights; flights
        # for the prior holder of a re-issued tail number will mismatch,
        # but the schema doesn't support multiple rows per tail_num.
        historic = historic.join(
            active.select("tail_num"), on="tail_num", how="anti"
        )
        print(f"  historic (DEREG, deduped, MASTER-anti): {historic.height:,} rows")
        combined = pl.concat([active, historic], how="vertical_relaxed")

    if flight_tails is not None:
        missing = flight_tails.join(
            combined.select("tail_num"), on="tail_num", how="anti"
        )
        if missing.height:
            stubs = _stub_rows_for(missing)
            print(
                f"  inferred (flights anti-FAA, status_code='{INFERRED_STATUS_CODE}'): "
                f"{stubs.height:,} rows"
            )
            combined = pl.concat([combined, stubs], how="vertical_relaxed")

    # Stable surrogate id, ordered by tail_num so re-runs are reproducible.
    combined = combined.sort("tail_num").with_row_index(name="id").with_columns(
        pl.col("id").cast(pl.Int64)
    )

    return combined.select("id", *AIRCRAFT_OUTPUT_COLUMNS).to_arrow()


def build_aircraft_models(acftref: pl.DataFrame) -> pa.Table:
    cols = {c.strip().upper(): c for c in acftref.columns}

    def col(name: str) -> str:
        try:
            return cols[name]
        except KeyError as exc:
            raise KeyError(
                f"ACFTREF.txt missing expected column {name!r}; have {list(cols)}"
            ) from exc

    df = acftref.with_columns(
        [
            _strip_str(col("CODE")).alias("aircraft_model_code"),
            _strip_str(col("MFR")).alias("manufacturer"),
            _strip_str(col("MODEL")).alias("model"),
            _to_int(col("TYPE-ACFT")).alias("aircraft_type_id"),
            _to_int(col("TYPE-ENG")).alias("aircraft_engine_type_id"),
            _to_int(col("AC-CAT")).alias("aircraft_category_id"),
            _to_int(col("BUILD-CERT-IND")).alias("amateur"),
            _to_int(col("NO-ENG")).alias("engines"),
            _to_int(col("NO-SEATS")).alias("seats"),
            _ac_weight_class(col("AC-WEIGHT")).alias("weight"),
            _to_int(col("SPEED")).alias("speed"),
        ]
    )
    df = df.filter(pl.col("aircraft_model_code") != "")
    df = df.unique(subset=["aircraft_model_code"], keep="first").sort(
        "aircraft_model_code"
    )
    return df.select(
        "aircraft_model_code",
        "manufacturer",
        "model",
        "aircraft_type_id",
        "aircraft_engine_type_id",
        "aircraft_category_id",
        "amateur",
        "engines",
        "seats",
        "weight",
        "speed",
    ).to_arrow()


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
    print(f"  wrote {dest} ({dest.stat().st_size / 1e6:.2f} MB, {table.num_rows:,} rows)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="re-download the FAA zip even if already cached",
    )
    parser.add_argument(
        "--zip", type=Path, default=RAW_DIR / ZIP_NAME, help="path to cached zip"
    )
    parser.add_argument(
        "--no-historic",
        action="store_true",
        help="skip DEREG.txt; only include currently-registered aircraft",
    )
    parser.add_argument(
        "--no-backfill-from-flights",
        action="store_true",
        help=(
            "skip backfilling tail_nums seen in local flights/flights_v2_*.parquet "
            f"but absent from the FAA registry (status_code='{INFERRED_STATUS_CODE}')"
        ),
    )
    parser.add_argument(
        "--flights-glob",
        type=Path,
        default=FLIGHTS_GLOB,
        help="glob for local flight parquets used to backfill missing tail_nums",
    )
    args = parser.parse_args(argv)

    args.zip.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(follow_redirects=True, timeout=60.0, headers=UA) as client:
        zip_path = download_zip(client, args.zip, force=args.force_download)

    with zipfile.ZipFile(zip_path) as zf:
        master = _read_member(zf, "MASTER.txt")
        acftref = _read_member(zf, "ACFTREF.txt")
        dereg = None if args.no_historic else _read_member(zf, "DEREG.txt")

    print(f"MASTER.txt  : {master.height:,} rows, {len(master.columns)} cols")
    print(f"ACFTREF.txt : {acftref.height:,} rows, {len(acftref.columns)} cols")
    if dereg is not None:
        print(f"DEREG.txt   : {dereg.height:,} rows, {len(dereg.columns)} cols")

    flight_tails: pl.DataFrame | None = None
    if not args.no_backfill_from_flights:
        flight_tails = _flight_tail_nums(args.flights_glob)
        if flight_tails is None:
            print(
                f"  no flight parquets at {args.flights_glob}; skipping backfill"
            )
        else:
            print(
                f"flights tail_nums: {flight_tails.height:,} distinct "
                f"(from {args.flights_glob})"
            )

    aircraft_table = build_aircraft(master, dereg, flight_tails)
    write_parquet(aircraft_table, AIRCRAFT_PARQUET)

    models_table = build_aircraft_models(acftref)
    write_parquet(models_table, AIRCRAFT_MODELS_PARQUET)

    print(
        f"done at {datetime.now().isoformat(timespec='seconds')} — "
        f"upload via publish_dimensions.py"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
