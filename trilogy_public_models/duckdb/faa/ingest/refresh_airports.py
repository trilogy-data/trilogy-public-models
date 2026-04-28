#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "httpx",
#   "polars>=1.0",
#   "pyarrow",
# ]
# ///
"""Download the FAA NFDC NASR 28-day airport CSV and produce
``dimensions/airports_v2.parquet`` shaped to match ``faa/airport.preql``.

Discovery flow:
  1. GET https://www.faa.gov/air_traffic/flight_info/aeronav/aero_data/NASR_Subscription/
  2. Parse the ``NASR_Subscription/YYYY-MM-DD`` cycle dates linked from
     that page; pick the latest ≤ today (or use ``--cycle YYYY-MM-DD``).
  3. Download
     ``https://nfdc.faa.gov/webContent/28DaySub/extra/{DD_Mon_YYYY}_APT_CSV.zip``
     where ``DD_Mon_YYYY`` is the cycle date formatted as e.g. ``16_Apr_2026``.

Inside the zip we read ``APT_BASE.csv`` (one row per facility); the
runway, remark and contact companions are ignored.

Notes on the legacy schema:
  * ``cert`` was a free-form "AS 05/1973"-style string. NFDC ships
    FAR_139_TYPE_CODE + ARFF_CERT_TYPE_DATE separately; we concatenate
    when both are present.
  * ``fed_agree`` and ``major`` have no direct NFDC equivalent in the
    APT_BASE table — left null. Hand-curate or join an overlay if you
    care about preserving those flags.
  * ``cntl_twr`` is set ``Y`` when ``TWR_TYPE_CODE`` is populated; the
    legacy parquet stored ``Y``/``N`` strings.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

INDEX_URL = "https://www.faa.gov/air_traffic/flight_info/aeronav/aero_data/NASR_Subscription/"
ZIP_URL_TEMPLATE = "https://nfdc.faa.gov/webContent/28DaySub/extra/{short}_APT_CSV.zip"

INGEST_DIR = Path(__file__).parent
RAW_DIR = INGEST_DIR / "_raw_dim"
OUT_DIR = INGEST_DIR / "dimensions"
AIRPORTS_PARQUET = OUT_DIR / "airports_v2.parquet"

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

# NFDC SITE_TYPE_CODE → legacy fac_type word.
SITE_TYPE_MAP = {
    "A": "AIRPORT",
    "H": "HELIPORT",
    "S": "SEAPLANE BASE",
    "U": "ULTRALIGHT",
    "G": "GLIDERPORT",
    "B": "BALLOONPORT",
}

CYCLE_LINK_RE = re.compile(r"NASR_Subscription/(\d{4}-\d{2}-\d{2})", re.IGNORECASE)


def discover_latest_cycle(client: httpx.Client) -> str:
    """Return latest cycle date (YYYY-MM-DD) ≤ today from the NASR index."""
    print(f"GET {INDEX_URL}")
    resp = client.get(INDEX_URL, timeout=60.0)
    resp.raise_for_status()
    cycles = sorted(set(CYCLE_LINK_RE.findall(resp.text)))
    if not cycles:
        raise RuntimeError(
            f"no NASR cycles found at {INDEX_URL}; pass --cycle explicitly"
        )
    today = date.today().isoformat()
    eligible = [c for c in cycles if c <= today]
    if not eligible:
        # All cycles in the future — fall back to the earliest listed.
        return cycles[0]
    return eligible[-1]


def cycle_short(cycle: str) -> str:
    """``2026-04-16`` -> ``16_Apr_2026``."""
    d = datetime.strptime(cycle, "%Y-%m-%d").date()
    return d.strftime("%d_%b_%Y")


def _is_valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with path.open("rb") as fd:
        return fd.read(2) == b"PK"


def download_zip(client: httpx.Client, url: str, dest: Path, force: bool) -> Path:
    if dest.exists() and not force and _is_valid_zip(dest):
        print(f"skip (cached): {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest
    if dest.exists():
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with client.stream("GET", url, timeout=None) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fd:
            for chunk in resp.iter_bytes(chunk_size=1 << 15):
                fd.write(chunk)
    if not _is_valid_zip(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{url} returned non-zip content")
    tmp.replace(dest)
    print(f"  wrote {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def _read_member(zf: zipfile.ZipFile, name: str) -> pl.DataFrame:
    candidates = [n for n in zf.namelist() if n.lower().endswith(name.lower())]
    if not candidates:
        raise RuntimeError(f"{name} not found inside zip; got {zf.namelist()}")
    with zf.open(candidates[0]) as fd:
        raw = fd.read()
    return pl.read_csv(
        io.BytesIO(raw),
        has_header=True,
        infer_schema_length=0,
        encoding="utf8-lossy",
        truncate_ragged_lines=True,
    )


def _strip(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Utf8).str.strip_chars()


def _to_int(name: str) -> pl.Expr:
    return (
        pl.col(name)
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Float64, strict=False)
        .cast(pl.Int64, strict=False)
        .fill_null(0)
    )


def _to_float(name: str) -> pl.Expr:
    return pl.col(name).cast(pl.Utf8).str.strip_chars().cast(
        pl.Float64, strict=False
    )


def build_airports(base: pl.DataFrame) -> pa.Table:
    cols = {c.strip().upper(): c for c in base.columns}

    def need(name: str) -> str:
        if name not in cols:
            raise KeyError(
                f"APT_BASE.csv missing column {name!r}; have {sorted(cols)[:25]}…"
            )
        return cols[name]

    def opt(name: str) -> str | None:
        return cols.get(name)

    site_type_col = need("SITE_TYPE_CODE")
    far_139_col = opt("FAR_139_TYPE_CODE")
    arff_date_col = opt("ARFF_CERT_TYPE_DATE")
    twr_col = opt("TWR_TYPE_CODE")

    # Build cert as "{FAR_139} {ARFF_DATE}" when both present, else either, else null.
    cert_expr: pl.Expr
    if far_139_col and arff_date_col:
        far = pl.col(far_139_col).cast(pl.Utf8).str.strip_chars()
        arff = pl.col(arff_date_col).cast(pl.Utf8).str.strip_chars()
        cert_expr = (
            pl.when((far != "") & (arff != ""))
            .then(far + pl.lit(" ") + arff)
            .when(far != "")
            .then(far)
            .when(arff != "")
            .then(arff)
            .otherwise(None)
        )
    elif far_139_col:
        cert_expr = _strip(far_139_col)
    elif arff_date_col:
        cert_expr = _strip(arff_date_col)
    else:
        cert_expr = pl.lit(None, dtype=pl.Utf8)

    # cntl_twr: "Y" if TWR_TYPE_CODE is non-empty, else "N".
    if twr_col:
        twr_clean = pl.col(twr_col).cast(pl.Utf8).str.strip_chars()
        cntl_twr_expr = pl.when(twr_clean != "").then(pl.lit("Y")).otherwise(pl.lit("N"))
    else:
        cntl_twr_expr = pl.lit("N")

    df = base.with_columns(
        [
            _strip(need("ARPT_ID")).alias("code"),
            _strip(need("SITE_NO")).alias("site_number"),
            _strip(site_type_col)
            .replace_strict(SITE_TYPE_MAP, default=pl.col(site_type_col))
            .alias("fac_type"),
            _strip(need("FACILITY_USE_CODE")).alias("fac_use"),
            _strip(need("REGION_CODE")).alias("faa_region"),
            _strip(need("ADO_CODE")).alias("faa_dist"),
            _strip(need("CITY")).alias("city"),
            _strip(need("COUNTY_NAME")).alias("county"),
            _strip(need("STATE_CODE")).alias("state"),
            _strip(need("ARPT_NAME")).alias("full_name"),
            _strip(need("OWNERSHIP_TYPE_CODE")).alias("own_type"),
            _to_float(need("LONG_DECIMAL")).alias("longitude"),
            _to_float(need("LAT_DECIMAL")).alias("latitude"),
            _to_int(need("ELEV")).alias("elevation"),
            _strip(need("CHART_NAME")).alias("aero_cht"),
            _to_int(need("DIST_CITY_TO_AIRPORT")).alias("cbd_dist"),
            _strip(need("DIRECTION_CODE")).alias("cbd_dir"),
            _strip(need("ACTIVATION_DATE")).alias("act_date"),
            cert_expr.alias("cert"),
            pl.lit(None, dtype=pl.Utf8).alias("fed_agree"),
            _strip(need("CUST_FLAG")).alias("cust_intl"),
            _strip(need("LNDG_RIGHTS_FLAG")).alias("c_ldg_rts"),
            _strip(need("JOINT_USE_FLAG")).alias("joint_use"),
            _strip(need("MIL_LNDG_FLAG")).alias("mil_rts"),
            cntl_twr_expr.alias("cntl_twr"),
            pl.lit(None, dtype=pl.Utf8).alias("major"),
        ]
    ).filter(pl.col("code") != "")

    df = df.unique(subset=["code"], keep="first").sort("code")
    df = df.with_row_index(name="id").with_columns(pl.col("id").cast(pl.Int64))

    return df.select(
        "id",
        "code",
        "site_number",
        "fac_type",
        "fac_use",
        "faa_region",
        "faa_dist",
        "city",
        "county",
        "state",
        "full_name",
        "own_type",
        "longitude",
        "latitude",
        "elevation",
        "aero_cht",
        "cbd_dist",
        "cbd_dir",
        "act_date",
        "cert",
        "fed_agree",
        "cust_intl",
        "c_ldg_rts",
        "joint_use",
        "mil_rts",
        "cntl_twr",
        "major",
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
        "--cycle",
        default=None,
        help="NFDC 28-day cycle date YYYY-MM-DD; default = auto-discover latest",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="re-download the NFDC zip even if already cached",
    )
    args = parser.parse_args(argv)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(follow_redirects=True, timeout=60.0, headers=UA) as client:
        cycle = args.cycle or discover_latest_cycle(client)
        short = cycle_short(cycle)
        print(f"using cycle {cycle} ({short})")
        zip_url = ZIP_URL_TEMPLATE.format(short=short)
        zip_path = RAW_DIR / f"APT_CSV_{cycle}.zip"
        download_zip(client, zip_url, zip_path, force=args.force_download)

    with zipfile.ZipFile(zip_path) as zf:
        base = _read_member(zf, "APT_BASE.csv")
    print(f"APT_BASE.csv: {base.height:,} rows, {len(base.columns)} cols")

    table = build_airports(base)
    write_parquet(table, AIRPORTS_PARQUET)

    print(
        f"done at {datetime.now().isoformat(timespec='seconds')} — "
        f"upload via publish_dimensions.py"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
