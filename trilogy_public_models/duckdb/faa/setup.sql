create or replace view airport as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/airports.parquet'
;

create or replace view flight as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/flights.parquet'
;


create or replace view aircraft as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/aircraft.parquet'
;

create or replace view aircraft_model as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/aircraft_models.parquet'
;

create or replace view carrier as SELECT *
FROM 'https://raw.githubusercontent.com/malloydata/malloy-samples/refs/heads/main/data/carriers.parquet'
;
