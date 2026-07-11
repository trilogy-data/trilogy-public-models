INSTALL httpfs;

LOAD httpfs;

-- Compact zstd-parquet extracts of the COVID-19 Open Data tables, built by
-- ingest/build_extract.py and published to GCS by ingest/publish_extract.py.
-- The tiles are not committed to git; see ../.gitignore. For local development
-- without GCS, use trilogy_dev.toml (setup_dev.sql), which reads the same files
-- from the local checkout.

CREATE OR REPLACE TABLE location AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/location.parquet');

CREATE OR REPLACE TABLE demographics AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/demographics.parquet');

CREATE OR REPLACE TABLE health AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/health.parquet');

CREATE OR REPLACE TABLE epidemiology AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/epidemiology.parquet');

CREATE OR REPLACE TABLE vaccinations AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/vaccinations.parquet');

CREATE OR REPLACE TABLE hospitalizations AS
SELECT * FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/covid19_open_data/hospitalizations.parquet');
