# USGS Earthquakes

Earthquakes and other seismic events from the [USGS](https://earthquake.usgs.gov),
served by `vgi-earthquakes.rusty-bb6.workers.dev`.

| File | Grain | Contents |
|---|---|---|
| `event.preql` | event_id | The event concepts both sources share: time, magnitude, place, depth, location, type, PAGER alert, felt reports, significance |
| `recent.preql` | event_id | The live feed: every event worldwide from the past 30 days, all magnitudes (~10k rows) |
| `catalog.preql` | event_id | The full ComCat catalog, searched live per query |

`recent` and `catalog` bind the same concepts under separate namespaces, so a query
picks its source explicitly. **Always bound a `catalog` query by `event_time` and/or
`magnitude`.** The filters are pushed to the USGS search, and an unbounded query asks
for the whole history and does not finish.

```trilogy
import catalog;

where event_time >= '2011-03-11'::datetime and event_time < '2011-03-12'::datetime
    and magnitude >= 6
select event_time, magnitude, place
order by magnitude desc;
```

`magnitude`, `place`, `alert` and `felt_reports` can be null. Most events have no
PAGER alert and no felt reports.

## Requirements

The data is served live by a public [Query.Farm VGI](https://query.farm/vgi/docs/python/)
worker, which DuckDB reads through the `vgi` extension. Only `haybarn`, Query.Farm's
DuckDB build, can load it: `pip install haybarn` (or `pytrilogy[vgi]`). `trilogy.toml`
selects it with `python_datasource_transport = "vgi"`, and `setup.sql` attaches the worker.
Every query reads the live source, so results change from run to run.

The model is kept out of the browser Studio catalog (`.unpublished`): duckdb-wasm
cannot load `vgi`.
