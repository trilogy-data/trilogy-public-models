"""Generate Trilogy schema models from BEAVER metadata and its MySQL dump."""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DATABASES = ("dw", "neutron", "nova")
CREATE_START_RE = re.compile(r"CREATE TABLE `(?P<table>[^`]+)` \(", re.IGNORECASE)
PRIMARY_RE = re.compile(r"PRIMARY KEY \((?P<columns>[^)]+)\)", re.IGNORECASE)
FOREIGN_RE = re.compile(
    r"FOREIGN KEY \((?P<local>[^)]+)\) REFERENCES "
    r"`(?P<table>[^`]+)` \((?P<remote>[^)]+)\)",
    re.IGNORECASE,
)


def quoted_columns(value: str) -> tuple[str, ...]:
    return tuple(re.findall(r"`([^`]+)`", value))


def read_dump_metadata(
    archive: Path,
) -> tuple[
    dict[str, dict[str, tuple[str, ...]]],
    dict[str, list[tuple[str, str, str, str]]],
]:
    primary_keys: dict[str, dict[str, tuple[str, ...]]] = defaultdict(dict)
    foreign_keys: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
    with zipfile.ZipFile(archive) as bundle:
        for db in DATABASES:
            table: str | None = None
            body_lines: list[str] = []
            for raw_line in bundle.open(f"beaver_db/{db}.sql"):
                line = raw_line.decode("utf-8", "replace")
                if table is None:
                    start = CREATE_START_RE.search(line)
                    if start:
                        table = start.group("table")
                        body_lines = []
                    continue
                if line.startswith(") ENGINE="):
                    body = "".join(body_lines)
                else:
                    body_lines.append(line)
                    continue
                primary = PRIMARY_RE.search(body)
                if primary:
                    primary_keys[db][table.lower()] = quoted_columns(
                        primary.group("columns")
                    )
                for foreign in FOREIGN_RE.finditer(body):
                    local = quoted_columns(foreign.group("local"))
                    remote = quoted_columns(foreign.group("remote"))
                    if len(local) == len(remote):
                        foreign_keys[db].extend(
                            (
                                table,
                                local_column,
                                foreign.group("table"),
                                remote_column,
                            )
                            for local_column, remote_column in zip(local, remote)
                        )
                table = None
                body_lines = []
    return dict(primary_keys), dict(foreign_keys)


def trilogy_type(mysql_type: str) -> str:
    normalized = mysql_type.upper()
    if any(token in normalized for token in ("INT", "NUMBER")):
        return "int"
    if any(
        token in normalized
        for token in ("DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL")
    ):
        return "float"
    if "BOOL" in normalized:
        return "bool"
    if "TIMESTAMP" in normalized or "DATETIME" in normalized:
        return "datetime"
    if normalized == "DATE":
        return "date"
    return "string"


def concept_name(column: str) -> str:
    # Prefixing preserves the physical column in the datasource mapping while
    # avoiding collisions with Trilogy keywords such as LIMIT and ORDER.
    return "col_" + column.lower()


def table_name_for_mysql(db: str, metadata_name: str) -> str:
    return metadata_name if db == "dw" else metadata_name.lower()


def property_declaration(
    parents: tuple[str, ...], column: str, datatype: str
) -> str:
    if len(parents) == 1:
        return f"property {parents[0]}.{column} {datatype};"
    return f"property <{', '.join(parents)}>.{column} {datatype};"


def render_table(
    db: str,
    table: dict,
    primary_keys: dict[str, dict[str, tuple[str, ...]]],
) -> str:
    metadata_name = table["table_name"]
    mysql_name = table_name_for_mysql(db, metadata_name)
    columns = [concept_name(value) for value in table["column_names"]]
    types = [trilogy_type(value) for value in table["column_types"]]
    primary = tuple(
        concept_name(value)
        for value in primary_keys.get(db, {}).get(mysql_name.lower(), ())
    )
    if not primary:
        # The anonymized DW dump omits most PK constraints. A full-row grain is
        # conservative: it does not invent uniqueness for one arbitrary column.
        primary = tuple(columns)

    lines: list[str] = []
    for column, datatype in zip(columns, types):
        if column in primary:
            lines.append(f"key {column} {datatype};")
        else:
            lines.append(property_declaration(primary, column, datatype))

    lines.extend(("", "datasource source ("))
    for original, column in zip(table["column_names"], columns):
        lines.append(f"    `{original}`:{column},")
    lines.extend(
        (
            ")",
            f"grain ({', '.join(primary)})",
            f"address `{db}.{mysql_name}`;",
            "",
        )
    )
    return "\n".join(lines)


def module_name(table_name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", table_name.lower()).strip("_")


def render_entrypoint(
    db: str,
    tables: Iterable[dict],
    annotated_joins: dict[str, list[list[str]]],
    foreign_keys: dict[str, list[tuple[str, str, str, str]]],
) -> str:
    table_list = list(tables)
    imports = "\n".join(
        f"import {module_name(table['table_name'])} as {module_name(table['table_name'])};"
        for table in sorted(table_list, key=lambda item: item["table_name"])
    )
    known = {
        (
            table["table_name"].lower(),
            column.lower(),
        ): trilogy_type(datatype)
        for table in table_list
        for column, datatype in zip(table["column_names"], table["column_types"])
    }
    relationships: set[tuple[str, str, str, str]] = set()
    for left, right in annotated_joins.get(db, []):
        if "." not in left or "." not in right:
            continue
        left_table, left_column = left.rsplit(".", 1)
        right_table, right_column = right.rsplit(".", 1)
        relationships.add(
            (
                left_table.lower(),
                left_column.lower(),
                right_table.lower(),
                right_column.lower(),
            )
        )
    for table, column, remote_table, remote_column in foreign_keys.get(db, []):
        relationships.add(
            (
                table.lower(),
                column.lower(),
                remote_table.lower(),
                remote_column.lower(),
            )
        )

    merges: list[str] = []
    for left_table, left_column, right_table, right_column in sorted(relationships):
        left_type = known.get((left_table, left_column))
        right_type = known.get((right_table, right_column))
        if not left_type or left_type != right_type:
            continue
        merges.append(
            f"MERGE {module_name(left_table)}.{concept_name(left_column)} "
            f"into ~{module_name(right_table)}.{concept_name(right_column)};"
        )
    return imports + ("\n\n" + "\n".join(merges) if merges else "") + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, default=Path("data/beaver/schema.json"))
    parser.add_argument(
        "--joins", type=Path, default=Path("data/beaver/join_keys.json")
    )
    parser.add_argument("--ddl-zip", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("trilogy_public_models/mysql")
    )
    args = parser.parse_args()

    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    annotated_joins = json.loads(args.joins.read_text(encoding="utf-8"))
    primary_keys: dict[str, dict[str, tuple[str, ...]]] = {}
    foreign_keys: dict[str, list[tuple[str, str, str, str]]] = {}
    if args.ddl_zip:
        primary_keys, foreign_keys = read_dump_metadata(args.ddl_zip)

    for db in DATABASES:
        model_dir = args.output / f"beaver_{db}"
        model_dir.mkdir(parents=True, exist_ok=True)
        tables = list(schema[db].values())
        for table in tables:
            path = model_dir / f"{module_name(table['table_name'])}.preql"
            path.write_text(
                render_table(db, table, primary_keys),
                encoding="utf-8",
                newline="\n",
            )
        (model_dir / "entrypoint.preql").write_text(
            render_entrypoint(db, tables, annotated_joins, foreign_keys),
            encoding="utf-8",
            newline="\n",
        )
        (model_dir / "README.md").write_text(
            f"# BEAVER {db}\n\n"
            f"Generated semantic schema for the BEAVER `{db}` MySQL database.\n"
            "Regenerate with `scripts/beaver/generate_models.py`; do not edit "
            "individual table files manually.\n",
            encoding="utf-8",
            newline="\n",
        )

    metadata = {
        "primary_keys": {
            db: {table: list(columns) for table, columns in sorted(tables.items())}
            for db, tables in sorted(primary_keys.items())
        },
        "foreign_keys": {
            db: [list(item) for item in sorted(items)]
            for db, items in sorted(foreign_keys.items())
        },
    }
    metadata_path = args.schema.parent / "ddl_metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
