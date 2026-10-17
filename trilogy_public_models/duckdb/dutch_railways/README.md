# Dutch Railways

Live departure and arrival boards from NS, the Dutch national railway, served by
`vgi-trains-ts.rusty-bb6.workers.dev`.

| File | Grain | Contents |
|---|---|---|
| `station.preql` | station_code | The ~770 stations NS serves, in the Netherlands and across the border |
| `departure.preql` | station/train | Trains leaving each station in roughly the next hour: destination, stops, planned and actual time, delay, track, cancellation |
| `arrival.preql` | station/train | Trains arriving at each station in roughly the next hour |

The boards cover only stations with trains due, so their `station_code` is partial
(`~`). Board sizes follow the timetable: thousands of trains by day, a few hundred
at night.

```trilogy
import departure;

select station.station_name, count(train_number) as departures, avg(delay_minutes) as avg_delay
order by departures desc
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
