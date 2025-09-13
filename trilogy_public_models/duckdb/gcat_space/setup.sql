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
    split(Agency, '/')[1] FirstAgency,
    LaunchCode,
    FailCode,
    "Group",
    Category,
    LTCite,
    Cite,
    Notes
FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/launch_cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE platform_info as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/platforms.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lv_info as
SELECT * 

from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/lv.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE lvs_info as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/lvs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE stages as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/stages.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE engines AS
SELECT 
    * EXCLUDE (Fuel, isp), 
    CASE 
        WHEN name = 'Raptor SL'  and isp is null    THEN 350.0
        WHEN name = 'Raptor 2 Vac' and isp is null THEN 380.0
        WHEN name = 'Raptor 3 SL'  and isp is null    THEN 350.0
        WHEN name = 'Raptor 3 Vac' and isp is null THEN 380.0
        ELSE isp
    END AS isp,
    COALESCE(Fuel, 'Unspecified') AS Fuel
FROM read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/engines.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE launch_sites as
SELECT * 
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/sites.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE organizations as
SELECT *
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/tables/orgs.cleaned.tsv',
sample_size=-1);

CREATE OR REPLACE TABLE satcat as
SELECT *
from read_csv_auto('https://trilogy-data.github.io/trilogy-public-models/trilogy_public_models/duckdb/gcat_space/tsv/cat/satcat.cleaned.tsv',
sample_size=-1);

