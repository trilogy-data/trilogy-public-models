INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE launch_info AS 
SELECT 
    trim(Launch_Tag) Launch_Tag,
    Launch_JD,
    Launch_Date,
    LV_Type,
    Variant,
    Flight_ID,
    Flight,
    Mission,
    FlightCode,
    Platform,
    Launch_Site,
    Launch_Pad,
    Ascent_Site,
    Ascent_Pad,
    Apogee,
    Apoflag,
    Range,
    RangeFlag,
    Dest,
    OrbPay::float OrbPay,
    Agency,
    LaunchCode,
    FailCode,
    "Group",
    Category,
    LTCite,
    Cite,
    Notes
FROM read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/tables/launch_cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE platform_info as
SELECT * 
from read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/tables/platforms.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lv_info as
SELECT *
from read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/tables/lv.cleaned.tsv',
sample_size=-1);


CREATE OR REPLACE TABLE launch_sites as
SELECT *, cast(case when latitude='-' then null else latitude end as float) llat, cast(case when longitude='-' then null else longitude end as float) llong
from read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/tables/sites.cleaned.tsv',
sample_size=-1);


CREATE OR REPLACE TABLE organizations as
SELECT *
from read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/tables/orgs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE satcat as
SELECT *
from read_csv_auto('https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/221de04c0cd1b2f3459d3ca8c217fef923cfcb48/trilogy_public_models/duckdb/gcat_space/tsv/cat/satcat.cleaned.tsv',
sample_size=-1);

