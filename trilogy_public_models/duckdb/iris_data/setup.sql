

INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE  genus_info AS 
SELECT * FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/iris_data/distribution.csv',
sample_size=10000);
