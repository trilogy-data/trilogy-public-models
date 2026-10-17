# Overture Places

Points of interest from the [Overture Maps](https://overturemaps.org) Places theme,
read from the live release by `vgi-overture.rusty-bb6.workers.dev`.

| File | Grain | Contents |
|---|---|---|
| `place.preql` | place_id | Name, basic category, confidence, operating status, locality, country, website and bounding box |

**Every query needs a bounding box.** The server refuses a scan without filters on all
four of `min_lon`, `max_lon`, `min_lat` and `max_lat`. They are pushed down to the
server, so a small box reads only that area.

```trilogy
import place;

where min_lon >= 4.880 and max_lon <= 4.900 and min_lat >= 52.368 and max_lat <= 52.378
select category, count(place_id) as places
order by places desc
limit 10;
```

## Requirements

The data is served live by a public [Query.Farm VGI](https://query.farm/vgi/docs/python/)
worker, which DuckDB reads through the `vgi` extension. Only `haybarn`, Query.Farm's
DuckDB build, can load it: `pip install haybarn` (or `pytrilogy[vgi]`). `trilogy.toml`
selects it with `python_datasource_transport = "vgi"`, and `setup.sql` attaches the worker.
Every query reads the live source, so results change from run to run.

The model is kept out of the browser Studio catalog (`.unpublished`): duckdb-wasm
cannot load `vgi`.
