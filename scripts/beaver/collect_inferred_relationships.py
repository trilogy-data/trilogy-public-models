"""Collect and classify relationships emitted by ``trilogy ingest``.

Inference is intentionally not accepted on its own. A candidate is marked
accepted only when it is corroborated by a declared FK or a released BEAVER
gold-query join annotation.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

IMPORT_RE = re.compile(r"^import\s+(?P<table>\w+)\s+as\s+(?P<alias>\w+);", re.MULTILINE)
MAPPING_RE = re.compile(
    r"^\s*(?P<column>\w+):\s*(?P<weak>~?)(?P<alias>\w+)\.(?P<target>\w+),",
    re.MULTILINE,
)


def endpoint(value: str) -> tuple[str, str]:
    table, column = value.rsplit(".", 1)
    return table.lower(), column.lower()


def undirected(
    source_table: str, source_column: str, target_table: str, target_column: str
) -> frozenset[tuple[str, str]]:
    return frozenset(
        (
            (source_table.lower(), source_column.lower()),
            (target_table.lower(), target_column.lower()),
        )
    )


def collect(
    ingest_root: Path,
    joins_path: Path,
    ddl_path: Path,
) -> dict[str, list[dict[str, object]]]:
    joins = json.loads(joins_path.read_text(encoding="utf-8"))
    ddl = json.loads(ddl_path.read_text(encoding="utf-8"))
    output: dict[str, list[dict[str, object]]] = {}

    for db in ("dw", "neutron", "nova"):
        annotated = {
            frozenset((endpoint(left), endpoint(right)))
            for left, right in joins.get(db, [])
            if "." in left and "." in right
        }
        declared = {
            (
                source_table.lower(),
                source_column.lower(),
                target_table.lower(),
                target_column.lower(),
            )
            for source_table, source_column, target_table, target_column in ddl[
                "foreign_keys"
            ].get(db, [])
        }
        candidates: list[dict[str, object]] = []
        model_dir = ingest_root / f"{db}_full"
        for path in sorted(model_dir.glob("*.preql")):
            text = path.read_text(encoding="utf-8")
            aliases = {
                match.group("alias").lower(): match.group("table").lower()
                for match in IMPORT_RE.finditer(text)
            }
            for match in MAPPING_RE.finditer(text):
                alias = match.group("alias").lower()
                if alias not in aliases:
                    continue
                candidate = (
                    path.stem.lower(),
                    match.group("column").lower(),
                    aliases[alias],
                    match.group("target").lower(),
                )
                declared_match = candidate in declared
                annotation_match = undirected(*candidate) in annotated
                evidence = [
                    source
                    for source, present in (
                        ("declared", declared_match),
                        ("annotation", annotation_match),
                    )
                    if present
                ]
                candidates.append(
                    {
                        "source_table": candidate[0],
                        "source_column": candidate[1],
                        "target_table": candidate[2],
                        "target_column": candidate[3],
                        "coverage": ("partial" if match.group("weak") else "complete"),
                        "corroborated_by": evidence,
                        "accepted": bool(evidence),
                    }
                )
        output[db] = candidates
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingest-root", type=Path, default=Path(".cache/beaver/ingest")
    )
    parser.add_argument(
        "--joins", type=Path, default=Path("data/beaver/join_keys.json")
    )
    parser.add_argument(
        "--ddl", type=Path, default=Path("data/beaver/ddl_metadata.json")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/beaver/inferred_relationships.json"),
    )
    args = parser.parse_args()
    result = collect(args.ingest_root, args.joins, args.ddl)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
