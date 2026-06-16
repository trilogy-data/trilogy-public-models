-- Local-development variant of setup.sql: reads the parquet tiles from the
-- checkout instead of GCS. Run ingest/build_extract.py first to produce them,
-- then execute against this config, e.g.:
--   trilogy integration trilogy_public_models/duckdb/covid19_open_data duckdb \
--     --config trilogy_public_models/duckdb/covid19_open_data/trilogy_dev.toml

CREATE OR REPLACE TABLE location AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/location.parquet');

CREATE OR REPLACE TABLE demographics AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/demographics.parquet');

CREATE OR REPLACE TABLE health AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/health.parquet');

CREATE OR REPLACE TABLE epidemiology AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/epidemiology.parquet');

CREATE OR REPLACE TABLE vaccinations AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/vaccinations.parquet');

CREATE OR REPLACE TABLE hospitalizations AS
SELECT * FROM read_parquet('./trilogy_public_models/duckdb/covid19_open_data/hospitalizations.parquet');
