
INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE wealth AS 
SELECT * FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/titanic/richest.csv',
sample_size=10000);



CREATE OR REPLACE TABLE raw_titanic AS 
SELECT * FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/titanic/train.csv',
sample_size=10000);
