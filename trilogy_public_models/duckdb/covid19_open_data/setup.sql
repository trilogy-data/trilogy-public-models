INSTALL httpfs;

LOAD httpfs;

-- Compact parquet extracts of the COVID-19 Open Data tables, built by
-- ingest/build_extract.py and committed to the repo. The github.io path is
-- rewritten to the local checkout by trilogy_public_models.main at load time.

CREATE OR REPLACE TABLE location AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/location.parquet');

CREATE OR REPLACE TABLE demographics AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/demographics.parquet');

CREATE OR REPLACE TABLE health AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/health.parquet');

CREATE OR REPLACE TABLE epidemiology AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/epidemiology.parquet');

CREATE OR REPLACE TABLE vaccinations AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/vaccinations.parquet');

CREATE OR REPLACE TABLE hospitalizations AS
SELECT * FROM read_parquet('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/covid19_open_data/hospitalizations.parquet');
