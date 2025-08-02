
INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE data_all_112224 AS 
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);


CREATE OR REPLACE TABLE  genus_info AS 
SELECT * FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/mobs/genus_data_processed.csv',
sample_size=10000);
