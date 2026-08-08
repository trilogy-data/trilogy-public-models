# COVID-19 Open Data

A Trilogy model over the [COVID-19 Open Data](https://github.com/GoogleCloudPlatform/covid-19-open-data)
dataset — Google Cloud Platform's large, harmonized collection of COVID-19
epidemiology, vaccination, hospitalization, demographic and health data covering
countries and their first-level subdivisions worldwide.

## Engine

DuckDB. The model reads compact zstd-parquet extracts of the upstream tables
hosted at `gs://trilogy_public_models/duckdb/covid19_open_data/`; no external
warehouse credentials are required to query it.

## Scope

Every aggregation level is included — **country (0)**, **state / province (1)**,
**county / admin-2 (2)** and **locality (3)** — across ~23,000 locations
(epidemiology alone is 12.5M daily rows). The verbose upstream CSVs are hundreds
of MB each; recompressed to parquet the whole model is ~74 MB. A curated set of
the most-used columns is kept per table. The data spans the pandemic period
(2020 through the dataset's final 2022 refresh).

The parquet tiles are **not committed to git** — they are built from the live
upstream CSVs and published to GCS by the ingest scripts (wired into the Refresh
Data CI workflow on merge to main).

## Structure

| File | Source table(s) | Grain | Description |
|------|-----------------|-------|-------------|
| `location.preql` | `index` + `geography` + `demographics` + `health` | `location_key` | Location dimension: identity (country / subregion1 / subregion2), geography, population and health-system indicators |
| `epidemiology.preql` | `epidemiology` | `location_key` × `date` | Daily new / cumulative confirmed, deceased, recovered, tested |
| `vaccinations.preql` | `vaccinations` | `location_key` × `date` | Daily new / cumulative persons vaccinated and doses administered |
| `hospitalizations.preql` | `hospitalizations` | `location_key` × `date` | Daily admissions and current hospital / ICU census |
| `dates.preql` | — | `date` | Shared observation-date concept plus `observation_year` / `observation_month_start` helpers |

Each fact imports the `location` dimension, so location attributes (country
name, population, life expectancy, …) are queryable alongside any metric — for
example `epidemiology.location.population` enables per-capita analysis.

## Usage

```python
from trilogy_public_models import get_executor

executor = get_executor("duckdb.covid19_open_data")

results = executor.execute_text("""
SELECT
    epidemiology.location.country_name,
    sum(epidemiology.new_confirmed) -> total_confirmed
WHERE
    epidemiology.location.aggregation_level = 0
ORDER BY
    total_confirmed desc
LIMIT 10;
""")

for row in results[0].fetchall():
    print(row)
```

See `examples/duckdb/covid19_open_data/` for more queries.

## Rebuilding & publishing the tiles

`data/` is a Trilogy ingest model over the live upstream CSVs: one root
datasource per upstream table reading its URL directly, and one published
datasource per tile writing straight to `gcs://`. `trilogy refresh` builds and
publishes in a single step — DuckDB writes the parquet to GCS itself, so there
is no separate upload.

The relationships are declared rather than hand-written. `location.preql` owns
`location_key` and its attributes; every other file imports it and binds its own
`location_key` against that concept, so the tiles relate to each other the same
way the query model above does. The one real join — `index` left joined to
`geography`, keeping locations with no known centroid — falls out of
`geography_raw`'s partial (`~`) key binding rather than a SQL string.

```bash
cd trilogy_public_models/duckdb/covid19_open_data/data

# what would be rebuilt, and why
trilogy refresh . --dry-run

# build + publish (needs GOOGLE_HMAC_KEY / GOOGLE_HMAC_SECRET for the gcs:// write)
trilogy refresh . -e /path/to/.env
```

Staleness is anchored on `ingest_update_date.py`, which HEADs the upstream CSVs
and reports the newest `Last-Modified` as `data_updated_through`; every tile
carries that stamp and declares `freshness by data_updated_through`. Upstream's
final refresh was 2022-09-16, so once the tiles are built a rerun exits 2 ("all
assets up to date") and rewrites nothing. Force a rebuild with `-f <tile>`.

This runs on trilogy-cloud as the `covid-refresh` job (org `trilogy-data`,
`operation=refresh`), triggered ad-hoc — there is no schedule, for the same
reason the CI workflow skips its daily cron: the dataset is static.

The older `ingest/build_extract.py` + `ingest/publish_extract.py` pair does the
same job from CI using ADC instead of HMAC, and is kept as a fallback path.

```bash
# build locally to this directory, no credentials needed (tiles are git-ignored)
uv run trilogy_public_models/duckdb/covid19_open_data/ingest/build_extract.py

# build + publish to GCS (needs GCS write credentials / ADC)
uv run trilogy_public_models/duckdb/covid19_open_data/ingest/refresh_and_publish.py
```

To query the model locally without GCS, build the tiles and use the dev config,
which reads them from the checkout:

```bash
trilogy run query.preql duck_db --config trilogy_public_models/duckdb/covid19_open_data/trilogy_dev.toml
```

## License

The COVID-19 Open Data is published by Google under CC BY 4.0; see the
[upstream repository](https://github.com/GoogleCloudPlatform/covid-19-open-data)
for full source attribution.
