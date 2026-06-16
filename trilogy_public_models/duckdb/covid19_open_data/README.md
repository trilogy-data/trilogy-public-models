# COVID-19 Open Data

A Trilogy model over the [COVID-19 Open Data](https://github.com/GoogleCloudPlatform/covid-19-open-data)
dataset — Google Cloud Platform's large, harmonized collection of COVID-19
epidemiology, vaccination, hospitalization, demographic and health data covering
countries and their first-level subdivisions worldwide.

## Engine

DuckDB. The model reads compact parquet extracts committed alongside it; no
external warehouse credentials are required.

## Scope

The upstream per-table CSVs include county / locality detail and run to many
hundreds of MB each. This model keeps the **country (aggregation_level 0)** and
**state / province (aggregation_level 1)** rows, which covers every national and
sub-national trend while keeping the extracts to ~13 MB total. The data spans the
pandemic period (2020 through the dataset's final 2022 refresh).

## Structure

| File | Source table(s) | Grain | Description |
|------|-----------------|-------|-------------|
| `location.preql` | `index` + `geography` + `demographics` + `health` | `location_key` | Location dimension: identity, geography, population and health-system indicators |
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

## Rebuilding the extracts

The parquet files are produced from the live COVID-19 Open Data CSVs by
`ingest/build_extract.py`:

```bash
uv run trilogy_public_models/duckdb/covid19_open_data/ingest/build_extract.py
```

## License

The COVID-19 Open Data is published by Google under CC BY 4.0; see the
[upstream repository](https://github.com/GoogleCloudPlatform/covid-19-open-data)
for full source attribution.
