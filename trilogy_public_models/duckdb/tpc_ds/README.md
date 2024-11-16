## TPC-DS dataset

Usable with the [duckdb extension](https://duckdb.org/docs/extensions/tpcds.html).

Requires generating data first.

Scale factor 1 example:
```sql
CALL dsdgen(sf = 1);
```