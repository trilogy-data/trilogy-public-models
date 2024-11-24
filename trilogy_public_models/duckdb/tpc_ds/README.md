## TPC-DS dataset

Usable with the [duckdb extension](https://duckdb.org/docs/extensions/tpcds.html).

Requires generating data first.

Trilogy:
```sql
# intialize the TPC-DS schema
RAW_SQL('''
INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=.25);
''');

SQL:
```
Scale factor 1 example:
```sql
INSTALL tpcds;
LOAD tpcds;
CALL dsdgen(sf = 1);
```