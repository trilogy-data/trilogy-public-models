# Open-Meteo

Weather forecasts and place-name geocoding from [Open-Meteo](https://open-meteo.com),
served by `vgi-open-meteo.rusty-bb6.workers.dev`.

Open-Meteo answers per coordinate, so each forecast datasource is a table function
of `latitude` and `longitude`. A query pins both with `= <literal>` and those values
become the call's arguments. A query that leaves one out fails with a message naming
the filter to add. Use `geocoding` to find the coordinates of a place.

| File | Grain | Contents |
|---|---|---|
| `daily.preql` | latitude/longitude/date | Seven-day forecast: temperature range, feels-like, precipitation, snow, wind, sunrise/sunset |
| `hourly.preql` | latitude/longitude/hour | 168-hour forecast: temperature, humidity, precipitation, cloud cover, wind, UV, day/night |
| `current.preql` | latitude/longitude | Conditions now |
| `geocoding.preql` | search/place | Up to ten places matching a name (pin `search`), with coordinates and population |
| `weather_code.preql` | weather_code | WMO weather codes with a description and emoji; joined from every forecast |

Times are local to the coordinate (`timezone := 'auto'`); temperatures are °C,
precipitation mm and wind km/h.

```trilogy
import daily;

where latitude = 52.374 and longitude = 4.890
select forecast_date, emoji, condition, temp_min, temp_max
order by forecast_date asc;
```

## Requirements

The data is served live by a public [Query.Farm VGI](https://query.farm/vgi/docs/python/)
worker, which DuckDB reads through the `vgi` extension. Only `haybarn`, Query.Farm's
DuckDB build, can load it: `pip install haybarn` (or `pytrilogy[vgi]`). `trilogy.toml`
selects it with `python_datasource_transport = "vgi"`, and `setup.sql` attaches the worker.
Every query reads the live source, so results change from run to run.

The model is kept out of the browser Studio catalog (`.unpublished`): duckdb-wasm
cannot load `vgi`.
