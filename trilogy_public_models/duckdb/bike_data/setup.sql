INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE t_202501_boulder_stats AS
SELECT *

FROM read_parquet('https://storage.googleapis.com/trilogy_public_models/duckdb/bike_data/202501-boulder-stats.parquet');