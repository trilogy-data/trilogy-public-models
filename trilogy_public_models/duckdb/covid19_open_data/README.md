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

`ingest/build_extract.py` downloads the live COVID-19 Open Data CSVs and writes
the parquet tiles into this directory (git-ignored). `ingest/publish_extract.py`
uploads them to GCS. `ingest/refresh_and_publish.py` runs both and is the entry
point used by the Refresh Data workflow.

```bash
# build locally (no credentials needed)
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
