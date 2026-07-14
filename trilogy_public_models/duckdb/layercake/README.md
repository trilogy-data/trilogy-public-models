# OSM LayerCake

A Trilogy model over [LayerCake](https://openstreetmap.us/our-work/layercake/),
OpenStreetMap US's thematic extracts of the full OSM planet published as
cloud-native GeoParquet at `https://data.openstreetmap.us/layercake/`.

## Engine

DuckDB. `setup.sql` creates one view per layer directly over the remote
parquet URLs (httpfs); nothing is downloaded up front. DuckDB pushes column
selection and bbox predicates down to HTTP range reads, so queries against
even the planet-scale layers only fetch the row groups they need — but a
query with no bbox filter on `buildings` or `highways` is a full scan of a
multi-GB remote file. Filter on `bbox_*` bounds whenever working with the two
large layers.

## Structure

One namespace per layer, all at grain `(type, id)` — the OSM element type
(`node` / `way` / `relation`) plus element id, which is only unique within a
type.

| File | Layer | Rows (approx.) | Description |
|------|-------|----------------|-------------|
| `buildings.preql` | buildings | 700M | Building footprints with address, height and roof tags |
| `highways.preql` | highways | 298M | Roads, paths and related ways with surface, lanes, access tags |
| `boundaries.preql` | boundaries | 804K | Administrative and place boundaries with ISO 3166 codes |
| `settlements.preql` | settlements | 4.6M | Populated places (city / town / village / ...) with population |
| `parks.preql` | parks | 1.5M | Parks and protected areas with protection class and operator |

Every layer exposes the element bounding box (`bbox_xmin` / `bbox_ymin` /
`bbox_xmax` / `bbox_ymax`) plus a computed centroid `latitude` / `longitude`
for mapping. The raw WKB `geometry` column is not modeled — pull it from the
source parquet directly if you need true geometries.

Most OSM tag values are free-text strings by design (`building_levels`,
`height`, `lanes`, `maxspeed`, ...); use `try_cast` for arithmetic. The views
in `setup.sql` rename namespaced tag columns (`building:levels` →
`building_levels`), flatten the `bbox` struct, and take the primary entry of
the multilingual name arrays on `boundaries` and `parks`. The per-language
name maps and the boundary dispute arrays (`disputed_by`, `claimed_by`, ...)
are not modeled.

## Usage

```python
from trilogy_public_models import get_executor

executor = get_executor("duckdb.layercake")

results = executor.execute_text("""
SELECT
    settlements.name,
    settlements.population,
WHERE
    settlements.place = 'city'
ORDER BY
    settlements.population desc
LIMIT 10;
""")

for row in results[0].fetchall():
    print(row)
```

See `examples/duckdb/layercake/` for more queries.

## Testing

The model is excluded from the validation suites (`test_models.py`,
`test_examples.py`) **in CI only** (gated on the `CI` env var): the data is
hosted by a third party, and the grain-uniqueness scans stream ~2GB of the
buildings/highways `type`/`id` columns over HTTP. Local runs of the test
suites include the model — expect the model validation to take a while on
the two planet-scale layers; the example queries finish in seconds.

## Data notes

- Layers are regenerated from the OSM planet by OpenStreetMap US; row counts
  above are as of July 2026.
- OpenStreetMap data is © OpenStreetMap contributors, available under the
  [ODbL](https://www.openstreetmap.org/copyright).
