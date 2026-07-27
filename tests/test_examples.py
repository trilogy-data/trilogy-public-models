import os
import traceback
from collections.abc import Iterator
from copy import deepcopy
from json import loads
from pathlib import Path

import pytest

from trilogy_public_models import data_models, get_executor
from trilogy_public_models.validator import example_path, validate_query

# duckdb.covid19_open_data tiles live on GCS (not committed); it is validated by
# a dedicated build + `trilogy integration` step instead. See test_models.py.
SKIPPED_MODELS = {
    "bigquery.age_of_empires_2",
    "duckdb.titanic",
    "duckdb.covid19_open_data",
}

# duckdb.layercake examples run real scans against third-party-hosted OSM
# parquet over HTTP. They finish in seconds and pass locally; keep the
# external dependency and bandwidth out of CI.
if os.environ.get("CI"):
    SKIPPED_MODELS.add("duckdb.layercake")

# (model key, source label) pairs to skip; for queries under active investigation.
SKIPPED_QUERIES = {
    ("duckdb.faa", "flights.json#0"),
}


def _with_terminator(query: str) -> str:
    stripped = query.rstrip()
    if not stripped.endswith(";"):
        stripped = stripped + ";"
    return stripped


def _extract_queries(example_dir: Path) -> Iterator[tuple[str, str]]:
    for preql in sorted(example_dir.glob("*.preql")):
        yield preql.name, preql.read_text()
    for dashboard in sorted(example_dir.glob("*.json")):
        content = loads(dashboard.read_text())
        imports = content.get("imports", [])
        imp_prefix = " ".join(f"import {x['name']};" for x in imports)
        for grid_key, item in content.get("gridItems", {}).items():
            inner = item.get("content")
            query: str | None = None
            if isinstance(inner, dict) and inner.get("query"):
                query = inner["query"]
            elif (
                item.get("type") in ("chart", "table")
                and isinstance(inner, str)
                and inner.strip()
            ):
                query = inner
            if query:
                yield f"{dashboard.name}#{grid_key}", imp_prefix + " " + _with_terminator(
                    query
                )


def _discover_example_keys() -> list[tuple[str, Path]]:
    keys: list[tuple[str, Path]] = []
    for type_dir in sorted(example_path.iterdir()):
        if not type_dir.is_dir():
            continue
        for name_dir in sorted(type_dir.iterdir()):
            if name_dir.is_dir():
                keys.append((f"{type_dir.name}.{name_dir.name}", name_dir))
    return keys


def test_example_queries(bq_client, bq_executor, snowflake_executor):
    failures: list[tuple[str, str, str]] = []
    for key, example_dir in _discover_example_keys():
        if key in SKIPPED_MODELS:
            continue
        if key not in data_models:
            failures.append(
                (key, "<model>", "example folder has no matching model in data_models")
            )
            continue
        queries = list(_extract_queries(example_dir))
        if not queries:
            continue
        try:
            if "bigquery" in key:
                executor = get_executor(key, executor=bq_executor())
                dry_run = bq_client()
            elif "duckdb" in key:
                executor = get_executor(key)
                dry_run = None
            elif "snowflake" in key:
                executor = get_executor(key, executor=snowflake_executor())
                dry_run = None
            elif "sqlite" in key:
                executor = get_executor(key)
                dry_run = None
            else:
                failures.append(
                    (key, "<setup>", f"no executor path for dialect in {key}")
                )
                continue
        except Exception as e:  # noqa: BLE001 - collect every setup failure, don't abort the run
            failures.append(
                (key, "<setup>", f"{type(e).__name__}: {e}\n{traceback.format_exc()}")
            )
            continue
        environment = deepcopy(data_models[key].environment)
        for source, query in queries:
            if (key, source) in SKIPPED_QUERIES:
                continue
            try:
                validate_query(query, environment, executor, dry_run)
            except Exception as e:  # noqa: BLE001 - collect every query failure, don't abort the run
                failures.append((key, source, f"{type(e).__name__}: {e}"))
    if failures:
        lines = [f"  {k} [{src}]: {err.splitlines()[0]}" for k, src, err in failures]
        pytest.fail(
            f"Broken examples ({len(failures)} failures):\n" + "\n".join(lines)
        )
