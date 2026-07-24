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
python -m pip install -e C:\Users\ethan\coding_projects\pytrilogy[mysql]
python scripts/beaver/generate_models.py \
  --ddl-zip /path/to/beaver_db.zip
```

This creates `mysql.beaver_dw`, `mysql.beaver_neutron`, and
`mysql.beaver_nova`. They use the MySQL dialect currently available in the
local PyTrilogy checkout.

Declared foreign keys are modeled by importing the referenced dimension into
the child module and assigning the physical FK column directly to that
dimension concept. `MERGE` is retained only for annotation-only relationships
where the released database metadata does not establish a canonical side. The
anonymized `dw` dump declares no foreign keys, so its directed relationships
require corroboration between annotations and value-based inference.

### Relationship defense in depth

Relationship evidence is applied in this order:

1. Declared MySQL foreign keys are authoritative.
2. Full ingest inference is accepted only when the same pair appears in a
   released BEAVER gold-query join annotation.
3. Accepted inferred relationships become directed table-local imports. Their
   complete/partial coverage is preserved; partial relationships use `~`.
4. Remaining annotation-only pairs stay as compatibility `MERGE` statements
   until another source can establish direction.
5. Uncorroborated inference remains in the evidence file with
   `"accepted": false` and never changes the generated models.

Collect inference evidence after running full ingest:

```console
python scripts/beaver/collect_inferred_relationships.py
python scripts/beaver/generate_models.py
```

The checked-in `data/beaver/inferred_relationships.json` records every
candidate, its coverage, corroborating sources, and acceptance decision.

### Inference test rollout

Use the datasets in this order:

1. **Neutron** is the calibration suite. Its 163 declared foreign keys provide
   ground truth for precision and recall, and it is much cheaper to scan than
   Nova. Report exact directed matches, wrong-target matches, and missed FKs.
2. **DW, then `dw_real`**, tests discovery where the dump has no declared FKs.
   Score against released join annotations and query execution coverage. Use
   `dw_real` as a small, human-readable regression slice over the same schema.
3. **Nova** is the scale suite. Start with declared-FK neighborhoods such as
   instances/actions and aggregates/hosts; run a full scan only as an offline
   performance test. Its exhaustive all-table inference is too expensive for a
   normal integration test.

Promotion thresholds should be measured on Neutron before changing the policy:
target at least 95% precision for automatically accepted relationships.
Recall is secondary because annotations and declared constraints remain
available as fallback layers.

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

The model executor reads `BEAVER_MYSQL_HOST`, `BEAVER_MYSQL_PORT`,
`BEAVER_MYSQL_USER`, and `BEAVER_MYSQL_PASSWORD`, with the local defaults shown
above.
