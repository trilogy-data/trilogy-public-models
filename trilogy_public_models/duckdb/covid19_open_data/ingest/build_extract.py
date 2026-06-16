#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.0",
# ]
# ///
"""Build compact parquet extracts of the COVID-19 Open Data tables.

Source: https://github.com/GoogleCloudPlatform/covid-19-open-data
        (published CSVs under https://storage.googleapis.com/covid19-open-data/v3/)

The upstream per-table CSVs are large (epidemiology alone is ~520MB, weather
~1.7GB) because they include county / locality level rows. For a public demo
model we keep:

  * static dimension tables (index, demographics, health, geography) for every
    location, joined into a single ``location`` table; and
  * the daily time-series tables (epidemiology, vaccinations, hospitalizations)
    filtered to aggregation_level <= 1 — i.e. country (0) and
    state / province (1). This drops the high-cardinality county / locality
    rows while keeping every national and sub-national trend.

Output parquet files land next to the model's ``setup.sql`` and are committed
to the repo; ``setup.sql`` reads them back via the github.io published path
(rewritten to the local checkout by ``trilogy_public_models.main``).

Run with:  uv run build_extract.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

BASE = "https://storage.googleapis.com/covid19-open-data/v3/"
OUT_DIR = Path(__file__).resolve().parent.parent
# country (0) + state / province (1); 2 = county, 3 = locality are dropped.
MAX_LEVEL = 1


def src(table: str) -> str:
    return f"read_csv_auto('{BASE}{table}.csv', sample_size=-1)"


def main() -> None:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    print("Loading index (location dimension keys)...")
    con.execute(
        f"""
        CREATE TABLE idx AS
        SELECT * FROM {src('index')}
        WHERE aggregation_level <= {MAX_LEVEL}
        """
    )
    n_loc = con.execute("SELECT count(*) FROM idx").fetchone()[0]
    print(f"  {n_loc} locations at aggregation_level <= {MAX_LEVEL}")

    # ---- location: index + geography + a few static attributes -------------
    print("Building location.parquet (index + geography)...")
    con.execute(
        f"""
        COPY (
            SELECT
                i.location_key,
                i.country_code,
                i.country_name,
                i.subregion1_code,
                i.subregion1_name,
                i.subregion2_code,
                i.subregion2_name,
                i.locality_name,
                i.iso_3166_1_alpha_3,
                i.aggregation_level,
                g.latitude,
                g.longitude,
                g.area_sq_km
            FROM idx i
            LEFT JOIN {src('geography')} g USING (location_key)
        ) TO '{OUT_DIR / 'location.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("Building demographics.parquet...")
    con.execute(
        f"""
        COPY (
            SELECT d.location_key, d.population, d.population_male, d.population_female,
                   d.population_density, d.human_development_index
            FROM {src('demographics')} d
            SEMI JOIN idx USING (location_key)
        ) TO '{OUT_DIR / 'demographics.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("Building health.parquet...")
    con.execute(
        f"""
        COPY (
            SELECT h.location_key, h.life_expectancy, h.smoking_prevalence,
                   h.diabetes_prevalence, h.hospital_beds_per_1000,
                   h.physicians_per_1000, h.health_expenditure_usd
            FROM {src('health')} h
            SEMI JOIN idx USING (location_key)
        ) TO '{OUT_DIR / 'health.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("Building epidemiology.parquet (this downloads ~520MB, please wait)...")
    con.execute(
        f"""
        COPY (
            SELECT e.date, e.location_key,
                   e.new_confirmed, e.new_deceased, e.new_recovered, e.new_tested,
                   e.cumulative_confirmed, e.cumulative_deceased,
                   e.cumulative_recovered, e.cumulative_tested
            FROM {src('epidemiology')} e
            SEMI JOIN idx USING (location_key)
        ) TO '{OUT_DIR / 'epidemiology.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("Building vaccinations.parquet (this downloads ~164MB)...")
    con.execute(
        f"""
        COPY (
            SELECT v.date, v.location_key,
                   v.new_persons_vaccinated, v.cumulative_persons_vaccinated,
                   v.new_persons_fully_vaccinated, v.cumulative_persons_fully_vaccinated,
                   v.new_vaccine_doses_administered, v.cumulative_vaccine_doses_administered
            FROM {src('vaccinations')} v
            SEMI JOIN idx USING (location_key)
        ) TO '{OUT_DIR / 'vaccinations.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("Building hospitalizations.parquet (this downloads ~66MB)...")
    con.execute(
        f"""
        COPY (
            SELECT hp.date, hp.location_key,
                   hp.new_hospitalized_patients, hp.cumulative_hospitalized_patients,
                   hp.current_hospitalized_patients,
                   hp.new_intensive_care_patients, hp.current_intensive_care_patients
            FROM {src('hospitalizations')} hp
            SEMI JOIN idx USING (location_key)
        ) TO '{OUT_DIR / 'hospitalizations.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    print("\nDone. Output files:")
    for p in sorted(OUT_DIR.glob("*.parquet")):
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{p}')"
        ).fetchone()[0]
        print(f"  {p.name:28s} {p.stat().st_size/1e6:8.2f} MB  {rows:>10,} rows")


if __name__ == "__main__":
    main()
