## FAA Data

For DuckDB. Parquet files. USA only. Normalized and contains high level flight, carrier, aircraft, model, and airport information.

Flight data is derived from the BTS Reporting Carrier On-Time Performance dataset
(<https://transtats.bts.gov/DatabaseInfo.asp?QO_VQ=EFD>) and refreshed via
`ingest/refresh_flights.py`. Carrier, aircraft, aircraft-model, and airport
tables are sourced from the malloy-samples snapshot and reflect ~2000-2005; joins
from modern flight rows to those tables are best-effort (IATA code / FAA airport
code / tail number).
