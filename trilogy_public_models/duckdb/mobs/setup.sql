

INSTALL httpfs;

LOAD httpfs;

CREATE TABLE data_all_112224 AS 
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);

CREATE TABLE genus_info AS 
SELECT * FROM read_csv_auto('https://gist.githubusercontent.com/greenmtnboy/a06ddb9e7b943f072049af43f9f83194/raw/85214d2394090695a9cf518af1153fcebf7e9a4e/example_data.csv',
sample_size=10000);
