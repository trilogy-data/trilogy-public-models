"""Extract stable question and schema artifacts from a BEAVER data download."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


SPLITS = ("dw", "dw_real", "neutron", "nova")
JSON_FIELDS = (
    "tables",
    "column_mapping",
    "join_keys",
    "domain_knowledge",
    "sub_questions",
    "sub_sqls",
)


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def parsed(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def normalize_question(row: dict[str, Any], split: str) -> dict[str, Any]:
    result = dict(row)
    for field in JSON_FIELDS:
        result[field] = parsed(
            result.get(field), {} if field == "column_mapping" else []
        )
    result["split"] = split
    return result


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract questions and schema from Beaver's generated JSON files."
    )
    parser.add_argument(
        "beaver_data",
        type=Path,
        help="The Beaver repository's data directory (containing split folders).",
    )
    parser.add_argument("--output", type=Path, default=Path("data/beaver"))
    args = parser.parse_args()

    questions: list[dict[str, Any]] = []
    schema: dict[str, dict[str, Any]] = {}
    joins: dict[str, set[tuple[str, str]]] = {}
    missing: list[str] = []

    for split in SPLITS:
        split_dir = args.beaver_data / split
        question_path = split_dir / "dev.json"
        if not question_path.exists():
            missing.append(str(question_path))
            continue

        split_questions = [
            normalize_question(row, split) for row in read_json(question_path)
        ]
        questions.extend(split_questions)

        for row in split_questions:
            db = row.get("db") or ("dw" if split == "dw_real" else split)
            db_joins = joins.setdefault(db, set())
            for pair in row["join_keys"]:
                if isinstance(pair, list) and len(pair) == 2:
                    db_joins.add(tuple(sorted((str(pair[0]), str(pair[1])))))

        # dw_real intentionally shares dw's schema.
        table_path = split_dir / "dev_tables.json"
        if table_path.exists() and split != "dw_real":
            raw_tables = read_json(table_path)
            tables = raw_tables.values() if isinstance(raw_tables, dict) else raw_tables
            for table in tables:
                db = table.get("db") or split
                table_name = table["table_name"]
                normalized = dict(table)
                for field in (
                    "column_names",
                    "column_types",
                    "example_rows",
                    "example_columns",
                ):
                    normalized[field] = parsed(normalized.get(field), [])
                schema.setdefault(db, {})[table_name] = normalized

    if missing:
        parser.error(
            "Missing Beaver downloads. Accept the Hugging Face access conditions, "
            "run Beaver's data/download_hf.py, then retry. Missing: "
            + ", ".join(missing)
        )

    questions.sort(key=lambda row: (row["split"], str(row.get("id", ""))))
    args.output.mkdir(parents=True, exist_ok=True)

    questions_jsonl = args.output / "questions.jsonl"
    with questions_jsonl.open("w", encoding="utf-8", newline="\n") as handle:
        for row in questions:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    questions_txt = args.output / "questions.txt"
    with questions_txt.open("w", encoding="utf-8", newline="\n") as handle:
        for row in questions:
            handle.write(f"[{row['split']}/{row.get('id', '')}] {row['question']}\n")

    schema_path = args.output / "schema.json"
    schema_path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    joins_path = args.output / "join_keys.json"
    serializable_joins = {
        db: [list(pair) for pair in sorted(pairs)] for db, pairs in sorted(joins.items())
    }
    joins_path.write_text(
        json.dumps(serializable_joins, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    split_counts = {
        split: sum(row["split"] == split for row in questions) for split in SPLITS
    }
    manifest_path = args.output / "manifest.json"
    artifacts = (questions_jsonl, questions_txt, schema_path, joins_path)
    manifest = {
        "question_count": len(questions),
        "question_counts_by_split": split_counts,
        "database_table_counts": {
            db: len(tables) for db, tables in sorted(schema.items())
        },
        "files": {
            path.name: {"sha256": sha256(path), "bytes": path.stat().st_size}
            for path in artifacts
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
