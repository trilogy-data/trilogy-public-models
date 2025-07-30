

INSTALL httpfs;

LOAD httpfs;

CREATE TABLE data_all_112224 AS 
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);
