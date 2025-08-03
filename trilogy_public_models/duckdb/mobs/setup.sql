
INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE data_all_112224 AS
SELECT
    column00,
    Order_Added,
    valid_aphiaID,
    phylum,
    class,
    "order",
    family,
    genus,
    scientificName,
    scientificNameAuthorship,
CASE WHEN length_cm = 'NA' then cast(null as float) else cast(length_cm as float) END length_cm,
    diameter_width_cm,
CASE WHEN height_cm = 'NA' then cast(null as float) else cast(height_cm as float) END height_cm,
    Notes,
    Size_Ref,
    Date_Added
FROM read_csv_auto(
    'https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
    sample_size = -1
);



CREATE OR REPLACE TABLE  genus_info AS 
SELECT * FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/mobs/genus_data_processed.csv',
sample_size=10000);
