## FAA Data

Normalized US-domestic flight, carrier, aircraft, model, and airport data,
distributed as parquet for DuckDB. Two flight entrypoints share the same
dimensions:

- `flight.preql` — the last two complete calendar years (~14M rows). The
  default for interactive use.
- `flight_all.preql` — the full BTS history, October 1987 through the most
  recent complete month (~200M rows across ~40 yearly files).

### Entities

| Entity           | Grain                                 | Source                                                       |
| ---------------- | ------------------------------------- | ------------------------------------------------------------ |
| `flight`         | one row per scheduled flight (`id`)   | BTS Reporting Carrier On-Time Performance                    |
| `carrier`        | 2-letter IATA code                    | distinct codes from `flight`, names from in-script lookup    |
| `aircraft`       | FAA registration record (`id`)        | FAA Releasable Aircraft Database — MASTER + DEREG            |
| `aircraft_tail_num` | tail number (`tail_num`)           | same as `aircraft`, exposed at the join key                  |
| `aircraft_model` | FAA make/model code                   | FAA Releasable Aircraft Database — ACFTREF                   |
| `airport`        | 3-char FAA location identifier (LID)  | FAA NFDC NASR 28-day airport subscription                    |

### Joins

```
flight.carrier      → carrier.code
flight.origin       → airport.code
flight.destination  → airport.code
flight.tail_num     → aircraft.tail_num   (best-effort; BTS may omit)
aircraft.aircraft_model_code → aircraft_model.code
```

`aircraft` includes the FAA DEREG (deregistration) history back to ~1945,
which is what makes the tail-number join useful against historical flight
years — without it, retired N-numbers wouldn't match.

### Notable measures and properties

- `flight`: `dep_delay`, `arr_delay`, `taxi_in`, `taxi_out`, `flight_time`,
  `distance`, `cancelled`, `diverted`, plus `flight_date` (always
  populated, including for cancelled rows — natural partition key).
- `flight.preql` ships pre-computed aggregate parquets next to the row-level
  fact (counts by year, carrier, origin, date, etc.) so common count
  queries skip scanning the full files.
- `airport`: lat/lon, elevation, ATCT presence, FAA region/district, owner
  type. ~13k AIRPORT facilities plus heliports, seaplane bases, etc.
- `aircraft_model`: manufacturer, model, engines, seats, type/engine
  category. `weight` and `speed` are present in the FAA feed but largely
  unpopulated.

### Freshness

Each root datasource declares a `freshness by` probe (under `probes/`) and
a `refresh` script (under `ingest/`). The flight model also publishes a
single-row `flight_watermark` parquet recording `data_through` — the last
month included in the build — which the pre-aggregated datasources use as
their freshness root.

## Architecture

Every entity is published as a parquet under
`gs://trilogy_public_models/duckdb/faa/` and read directly from GCS by the
`*.preql` datasources, so consumers don't need a database — DuckDB reads
the parquets in place. Refresh is driven by the per-entity scripts under
`ingest/`; see `ingest/README.md` for run instructions, source-fallback
behavior (BTS PREZIP vs on-demand CSV), and publish details.
