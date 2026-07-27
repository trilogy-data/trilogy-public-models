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
  * ``fed_agree`` is sourced from NFDC ``NASP_CODE`` — the concatenated
    NPIAS/Grant/Surplus flag matches the legacy semantics.
  * The legacy ``major`` flag (~270 hand-tagged airports) has no NFDC
    equivalent; we drop the column rather than emit it all-null.
  * ``cntl_twr`` is ``Y`` when TWR_TYPE_CODE starts with ``ATCT`` (any
    controlled-tower variant), ``N`` for ``NON-ATCT``.
  * The customs / landing-rights / joint-use / military-rights flags
    are stored as ``Y``/``N`` or null (NFDC ships empty strings; we
    coerce empty → null so the columns are clean two-value enums).
"""
from __future__ import annotations

import argparse
import io
import re
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

INDEX_URL = (
    "https://www.faa.gov/air_traffic/flight_info/aeronav/aero_data/NASR_Subscription/"
)
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

# NFDC SITE_TYPE_CODE → legacy fac_type word. The FAA reassigned the
# seaplane-base code from ``S`` to ``C`` at some point, so the canonical
# legacy ``S`` would silently miss every seaplane base today.
SITE_TYPE_MAP = {
    "A": "AIRPORT",
    "H": "HELIPORT",
    "C": "SEAPLANE BASE",
    "U": "ULTRALIGHT",
    "G": "GLIDERPORT",
    "B": "BALLOONPORT",
}

CYCLE_LINK_RE = re.compile(r"NASR_Subscription/(\d{4}-\d{2}-\d{2})", re.IGNORECASE)

# Sentinel airport row appended to the dim so flight facts can coalesce null
# origin/destination codes to a real key. NFDC never emits "UNK" so we own it.
UNKNOWN_AIRPORT_CODE = "UNK"

# BTS records origin/destination using IATA codes; NFDC publishes by FAA LID.
# For most US airports the two match, but ~15 commercially-active airports
# diverge — without this overlay, count_by_origin/by_source_dest_date drop
# every flight to/from these (~360k rows). We rewrite the LID-keyed NFDC row
# to use the IATA code so the flight-side join lines up.
IATA_TO_FAA_LID: dict[str, str] = {
    "AZA": "IWA",  # Phoenix-Mesa Gateway, AZ
    "FCA": "GPI",  # Glacier Park Intl, Kalispell MT
    "YUM": "NYL",  # Yuma Intl (joint civilian/MCAS Yuma)
    "CLD": "CRQ",  # McClellan-Palomar, Carlsbad CA
    "MQT": "SAW",  # Sawyer Intl, Marquette MI
    "SCE": "UNV",  # State College Rgnl, PA
    "HHH": "HXD",  # Hilton Head Island, SC
    "SPN": "GSN",  # Saipan Intl, Northern Mariana Islands
    "USA": "JQF",  # Concord-Padgett Rgnl, Concord NC
    "BKG": "BBG",  # Branson, MO
    "ROP": "GRO",  # Rota Intl, Northern Mariana Islands
    "YAP": "T11",  # Yap Intl, Federated States of Micronesia
    "UST": "SGJ",  # Northeast Florida Rgnl, St Augustine
    "GUF": "JKA",  # Jack Edwards/Gulf Shores Intl, AL
    "UTM": "UTA",  # Tunica Muni, MS
}

# Airports BTS still references in historical years but NFDC has dropped from
# the active dataset (closed/decommissioned). Hand-rolled stub rows so the
# flight join lines up; lat/lon/elevation are best-known approximations.
HISTORICAL_AIRPORT_STUBS: list[dict] = [
    # Panama City-Bay County Intl, FL — closed 2010-05 when ECP opened.
    {
        "code": "PFN", "site_number": "PFN-CLOSED",
        "fac_type": "AIRPORT", "fac_use": "PU", "faa_region": None,
        "faa_dist": "ATL", "city": "PANAMA CITY", "county": "BAY",
        "state": "FL", "full_name": "PANAMA CITY-BAY COUNTY INTL (CLOSED 2010)",
        "own_type": "PU", "longitude": -85.6828, "latitude": 30.2121,
        "elevation": 21, "aero_cht": "JACKSONVILLE", "cbd_dist": 3,
        "cbd_dir": "NW", "act_date": None, "cert": None, "fed_agree": None,
        "cust_intl": None, "c_ldg_rts": None, "joint_use": None,
        "mil_rts": None, "cntl_twr": "Y",
    },
    # Sloulin Field Intl, Williston ND — closed 2019-10 when XWA opened.
    {
        "code": "ISN", "site_number": "ISN-CLOSED",
        "fac_type": "AIRPORT", "fac_use": "PU", "faa_region": None,
        "faa_dist": "BIS", "city": "WILLISTON", "county": "WILLIAMS",
        "state": "ND", "full_name": "SLOULIN FLD INTL (CLOSED 2019)",
        "own_type": "PU", "longitude": -103.6422, "latitude": 48.1779,
        "elevation": 1982, "aero_cht": "GREAT FALLS", "cbd_dist": 1,
        "cbd_dir": "NW", "act_date": None, "cert": None, "fed_agree": None,
        "cust_intl": None, "c_ldg_rts": None, "joint_use": None,
        "mil_rts": None, "cntl_twr": "N",
    },
    # Oneida County, Utica NY — closed 2007-01.
    {
        "code": "UCA", "site_number": "UCA-CLOSED",
        "fac_type": "AIRPORT", "fac_use": "PU", "faa_region": None,
        "faa_dist": "NYC", "city": "UTICA", "county": "ONEIDA",
        "state": "NY", "full_name": "ONEIDA COUNTY (CLOSED 2007)",
        "own_type": "PU", "longitude": -75.3835, "latitude": 43.1455,
        "elevation": 745, "aero_cht": "NEW YORK", "cbd_dist": 6,
        "cbd_dir": "SW", "act_date": None, "cert": None, "fed_agree": None,
        "cust_intl": None, "c_ldg_rts": None, "joint_use": None,
        "mil_rts": None, "cntl_twr": "N",
    },
]


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
    today = datetime.now(timezone.utc).date().isoformat()
    eligible = [c for c in cycles if c <= today]
    if not eligible:
        # All cycles in the future — fall back to the earliest listed.
        return cycles[0]
    return eligible[-1]


def cycle_short(cycle: str) -> str:
    """``2026-04-16`` -> ``16_Apr_2026``."""
    d = date.fromisoformat(cycle)
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


def _strip_yn(name: str) -> pl.Expr:
    """Strip and treat empty as null. NFDC populates the customs/landing
    /joint-use/military-rights flags as 'Y'/'N' or empty; converting empty
    to null keeps the downstream column a clean two-value enum."""
    s = pl.col(name).cast(pl.Utf8).str.strip_chars()
    return pl.when(s == "").then(None).otherwise(s)


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
    return pl.col(name).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)


def _apply_iata_overlay(df: pl.DataFrame) -> pl.DataFrame:
    """Rewrite NFDC LID-keyed rows to use BTS IATA codes, then append stub
    rows for closed airports BTS still references in historical years."""
    lid_to_iata = {lid: iata for iata, lid in IATA_TO_FAA_LID.items()}
    aliased = set(df.filter(pl.col("code").is_in(list(lid_to_iata)))["code"].to_list())
    missing = sorted(set(lid_to_iata) - aliased)
    if missing:
        print(
            f"  warning: {len(missing)} IATA-alias LID(s) not found in NFDC, "
            f"flights to these will still drop: {missing}"
        )
    df = df.with_columns(pl.col("code").replace(lid_to_iata).alias("code"))

    stubs = pl.DataFrame(HISTORICAL_AIRPORT_STUBS, schema=df.schema)
    print(
        f"  IATA overlay: {len(aliased)} LID->IATA rewrites, "
        f"{stubs.height} historical stubs"
    )
    return pl.concat([df, stubs], how="vertical_relaxed")


def _unknown_airport_row(schema: dict) -> pl.DataFrame:
    """Single sentinel row used as the coalesce target for null origin/destination
    in the flight fact. fac_type uses 'AIRPORT' to satisfy the enum constraint
    declared in airport.preql."""
    return pl.DataFrame(
        [
            {
                "code": UNKNOWN_AIRPORT_CODE,
                "site_number": "UNK",
                "fac_type": "AIRPORT",
                "fac_use": "PU",
                "faa_region": None,
                "faa_dist": "UNK",
                "city": "UNKNOWN",
                "county": "UNKNOWN",
                "state": None,
                "full_name": "UNKNOWN",
                "own_type": "PU",
                "longitude": 0.0,
                "latitude": 0.0,
                "elevation": 0,
                "aero_cht": "UNKNOWN",
                "cbd_dist": 0,
                "cbd_dir": None,
                "act_date": None,
                "cert": None,
                "fed_agree": None,
                "cust_intl": None,
                "c_ldg_rts": None,
                "joint_use": None,
                "mil_rts": None,
                "cntl_twr": "N",
            }
        ],
        schema=schema,
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
    nasp_col = opt("NASP_CODE")

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

    # cntl_twr: "Y" if a tower is on the field. NFDC populates
    # TWR_TYPE_CODE for every record — "NON-ATCT" means no tower, while
    # "ATCT", "ATCT-TRACON", "ATCT-RAPCON", "ATCT-A/C", "ATCT-RATCF" are
    # the various controlled-tower variants. Match on the ATCT prefix.
    if twr_col:
        twr_clean = pl.col(twr_col).cast(pl.Utf8).str.strip_chars()
        cntl_twr_expr = (
            pl.when(twr_clean.str.starts_with("ATCT"))
            .then(pl.lit("Y"))
            .otherwise(pl.lit("N"))
        )
    else:
        cntl_twr_expr = pl.lit("N")

    # fed_agree comes from NFDC NASP_CODE — concatenated letters like
    # "NGY"/"NGPY" carrying NPIAS/Grant/Surplus flags plus year digits,
    # which matches the legacy fed_agree semantics one-for-one.
    fed_agree_expr = _strip(nasp_col) if nasp_col else pl.lit(None, dtype=pl.Utf8)

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
            fed_agree_expr.alias("fed_agree"),
            _strip_yn(need("CUST_FLAG")).alias("cust_intl"),
            _strip_yn(need("LNDG_RIGHTS_FLAG")).alias("c_ldg_rts"),
            _strip_yn(need("JOINT_USE_FLAG")).alias("joint_use"),
            _strip_yn(need("MIL_LNDG_FLAG")).alias("mil_rts"),
            cntl_twr_expr.alias("cntl_twr"),
        ]
    ).filter(pl.col("code") != "")

    df = df.unique(subset=["code"], keep="first").select(
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
    )
    df = _apply_iata_overlay(df)
    df = pl.concat([df, _unknown_airport_row(df.schema)], how="vertical_relaxed")
    df = df.sort("code")
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
    print(
        f"  wrote {dest} ({dest.stat().st_size / 1e6:.2f} MB, {table.num_rows:,} rows)"
    )


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
        f"done at {datetime.now(timezone.utc).isoformat(timespec='seconds')} — "
        f"upload via publish_dimensions.py"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
