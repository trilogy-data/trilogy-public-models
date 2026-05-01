# FAA flights refresh

`refresh_flights.py` downloads the BTS Reporting Carrier On-Time Performance
dataset and produces one parquet per calendar year under
`flights/flights_{YEAR}.parquet`, shaped to match `faa/flight.preql`.

Source: <https://transtats.bts.gov/DatabaseInfo.asp?QO_VQ=EFD>

## Run it

```
uv run trilogy_public_models/duckdb/faa/ingest/refresh_flights.py
```

Defaults to the two most recent complete calendar years. Override with:

```
uv run trilogy_public_models/duckdb/faa/ingest/refresh_flights.py \
    --start 2024-01 --end 2024-12
```

Pull the full BTS history (1987-10 through the last complete month) with:

```
uv run trilogy_public_models/duckdb/faa/ingest/refresh_flights.py --full-history
```

A full pull is ~460 monthly zips (~20 GB of compressed CSV downloads) and
produces ~40 yearly parquet files totalling ~4 GB (each year ~100 MB). The
writer streams one row-group per month into that year's file, so the run
does not need to hold the whole dataset in memory; monthly zips are cached
under `_raw/` so re-runs are incremental. `id2` is assigned chronologically
across the whole range, so each yearly file owns a disjoint contiguous
range.

### Source fallback

BTS does not package the 1990-1999 decade under the `PREZIP/` directory —
the URL returns the TranStats homepage (HTML) with a `200 OK` and
`content-type: text/html` instead of a zip. `download_zip` sniffs the
first two bytes of the response and, when it sees HTML instead of `PK…`,
falls back to `bts_ondemand.download_month`, which drives the
`DL_SelectFields.aspx` form POST to assemble a fresh CSV. On-demand CSVs
use snake-case uppercase headers (`FL_DATE`, `OP_CARRIER`, …) and
`M/D/YYYY` dates; `read_month_csv` renames columns and re-formats dates so
downstream code doesn't need to care which source produced a month.

Any HTML files cached from earlier runs are auto-evicted on the next run.

## Probe locally

`../flight_probe.preql` declares a `flight_local_probe` datasource bound
to `./ingest/flights/*.parquet` so you can validate the output against the
flight concepts in `faa/flight.preql` before uploading.

## Publish

Upload the yearly parquet files to the public models bucket. Uses
Application Default Credentials — run `gcloud auth application-default
login` once if you haven't:

```
uv run trilogy_public_models/duckdb/faa/ingest/publish_flights.py
```

Uploads run concurrently (default 4 workers) and skip objects that already
exist at the same byte size. Pass `--force` to re-upload regardless.

The `flight` datasource in `faa/flight.preql` reads from
`gs://trilogy_public_models/duckdb/faa/flights/*.parquet` directly, so the
model picks up new years automatically after publish.
