INSTALL httpfs;

LOAD httpfs;

-- The Lahman CSVs name doubles and triples "2B" and "3B", which are not legal
-- concept identifiers, so they are renamed at load rather than in the model.

CREATE OR REPLACE TABLE player AS
SELECT * FROM read_csv_auto('https://storage.googleapis.com/trilogy_public_models/duckdb/lahman/People.csv',
sample_size=-1);

CREATE OR REPLACE TABLE team AS
SELECT * EXCLUDE ("2B", "3B"), "2B" AS doubles, "3B" AS triples
FROM read_csv_auto('https://storage.googleapis.com/trilogy_public_models/duckdb/lahman/Teams.csv',
sample_size=-1);

CREATE OR REPLACE TABLE batting AS
SELECT * EXCLUDE ("2B", "3B"), "2B" AS doubles, "3B" AS triples
FROM read_csv_auto('https://storage.googleapis.com/trilogy_public_models/duckdb/lahman/Batting.csv',
sample_size=-1);
