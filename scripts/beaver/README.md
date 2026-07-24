# Beaver benchmark extraction

The [BEAVER benchmark](https://github.com/beaverbench/beaver) publishes four
MySQL evaluation splits:

| Split | Public questions | Schema |
| --- | ---: | --- |
| `dw` | 5,787 | `dw` |
| `dw_real` | 121 | `dw` (shared) |
| `neutron` | 1,017 | `neutron` |
| `nova` | 1,053 | `nova` |

The question and table datasets are gated on Hugging Face. Accept their access
conditions for
[`beaver-query`](https://huggingface.co/datasets/beaverbench/beaver-query) and
[`beaver-table`](https://huggingface.co/datasets/beaverbench/beaver-table),
authenticate, then run the upstream downloader from the root of a Beaver clone:

```console
python data/download_hf.py
```

That creates `data/<split>/dev.json` and `dev_tables.json`. Pass its `data`
directory to this repository's dependency-free extractor:

```console
python scripts/beaver/extract.py /path/to/beaver/data \
  --output data/beaver
```

Outputs:

- `questions.jsonl`: one stable, script-friendly record per public question.
- `questions.txt`: IDs and natural-language questions for quick inspection.
- `schema.json`: tables, columns, MySQL types, and supplied examples by DB.
- `join_keys.json`: join pairs observed in the released gold annotations.
- `manifest.json`: counts and SHA-256 hashes for reproducibility.

`dev_tables.json` is useful model metadata, but is not complete relational DDL:
it has table/column names and types, not declared primary/foreign-key
constraints. `join_keys.json` recovers the relationships exercised by released
questions. The anonymized MySQL dump in `beaver-table` is the authoritative
source if constraints or executable evaluation data are needed.

The official execution evaluator runs both gold and predicted SQL against
MySQL, converts values to stripped strings, ignores column names, row order,
and duplicate rows, but requires the same column order and column count. Both
empty results count as a match. Queries time out after 10 seconds.

## Generate Trilogy models

Download `beaver_db.zip` from the gated `beaver-table` repository, then generate
models using both the released schema metadata and primary keys recovered from
the dump:

```console
python scripts/beaver/generate_models.py \
  --ddl-zip /path/to/beaver_db.zip
```

This creates `mysql.beaver_dw`, `mysql.beaver_neutron`, and
`mysql.beaver_nova`. Trilogy currently has no native MySQL dialect/executor, so
these are semantic schema models for prompt construction and inspection. The
benchmark evaluator should execute generated SQL directly against MySQL.

## Run MySQL locally

The cheapest faithful backend is the official MySQL 8 Docker image. Preparing
the dump requires roughly 2.2 GB of free disk:

```powershell
python scripts/beaver/prepare_mysql.py C:\path\to\beaver_db.zip
$env:BEAVER_MYSQL_INIT_DIR = (Resolve-Path .cache\beaver\mysql-init)
docker compose -f scripts\beaver\docker-compose.yml up -d
```

The default connection is `127.0.0.1:3306`, user `root`, password `beaver`.
Set `BEAVER_MYSQL_ROOT_PASSWORD` before the first `docker compose up` to use a
different local password. MySQL executes the init files only when its named
data volume is empty.
