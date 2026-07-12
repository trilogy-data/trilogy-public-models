# GCAT: General Catalog of Artificial Space Objects

Full open source satellite database, plus ancillary catalogs.

This model reads parquet files published to GCS by
[space_reporting](https://github.com/greenmtnboy/space_reporting), which
ingests and cleans the upstream GCAT TSVs. The parquet files are refreshed
automatically; each datasource tracks its currency via the
`data_updated_through` column (`freshness by`).

McDowell, Jonathan C., 2020. General Catalog of Artificial Space Objects, Release 1.7.3, https://planet4589.org/space/gcat
