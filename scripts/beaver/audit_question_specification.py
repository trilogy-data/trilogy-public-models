"""Conservatively identify BEAVER questions with hidden gold dependencies."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+`?(?P<table>[A-Za-z_][A-Za-z0-9_]*)`?",
    re.IGNORECASE,
)
CTE_RE = re.compile(
    r"(?:\bWITH|,)\s*`?(?P<name>[A-Za-z_][A-Za-z0-9_]*)`?\s+AS\s*\(",
    re.IGNORECASE,
)
STRING_LITERAL_RE = re.compile(r"'(?:''|\\'|[^'])*'")


def normalized_table(value: str) -> str:
    return value.rsplit(".", 1)[0].upper()


def sql_physical_tables(sql: str) -> set[str]:
    structural_sql = STRING_LITERAL_RE.sub("''", sql)
    ctes = {
        match.group("name").upper() for match in CTE_RE.finditer(structural_sql)
    }
    return {
        match.group("table").upper()
        for match in TABLE_REF_RE.finditer(structural_sql)
        if match.group("table").upper() not in ctes
    }


def audit_question(question: dict[str, Any]) -> dict[str, Any]:
    declared = {str(value).upper() for value in question.get("tables", [])}
    join_tables = {
        normalized_table(str(endpoint))
        for pair in question.get("join_keys", [])
        for endpoint in pair
    }
    sql_tables = sql_physical_tables(str(question.get("sql", "")))
    hidden_join_tables = sorted(join_tables - declared)
    hidden_sql_tables = sorted(sql_tables - declared)
    reasons: list[dict[str, Any]] = []
    if hidden_join_tables:
        reasons.append(
            {
                "code": "hidden_join_table",
                "tables": hidden_join_tables,
                "detail": (
                    "Gold join annotations require tables absent from the "
                    "question's declared table set."
                ),
            }
        )
    if hidden_sql_tables:
        reasons.append(
            {
                "code": "hidden_sql_table",
                "tables": hidden_sql_tables,
                "detail": (
                    "Gold SQL reads physical tables absent from the question's "
                    "declared table set."
                ),
            }
        )
    return {
        "id": question["id"],
        "split": question["split"],
        "status": "unspecified" if reasons else "specified",
        "reasons": reasons,
        "declared_tables": sorted(declared),
        "gold_sql_tables": sorted(sql_tables),
    }


def audit_questions(questions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [audit_question(question) for question in questions]


def write_report(path: Path, audited: list[dict[str, Any]]) -> None:
    by_split: dict[str, Counter[str]] = {}
    for row in audited:
        by_split.setdefault(row["split"], Counter())[row["status"]] += 1
    lines = [
        "# BEAVER question specification audit",
        "",
        "A question is conservatively marked `unspecified` when its gold SQL or "
        "gold join annotations require a physical table absent from the released "
        "`tables` annotation. This detects hidden dependencies; it does not prove "
        "that every remaining question is perfectly worded.",
        "",
        "| Split | Specified | Unspecified | Total |",
        "| --- | ---: | ---: | ---: |",
    ]
    for split, counts in sorted(by_split.items()):
        total = counts["specified"] + counts["unspecified"]
        lines.append(
            f"| {split} | {counts['specified']} | "
            f"{counts['unspecified']} | {total} |"
        )
    lines.extend(("", "## Unspecified questions", ""))
    for row in audited:
        if row["status"] != "unspecified":
            continue
        reason_text = "; ".join(
            f"{reason['code']}: {', '.join(reason['tables'])}"
            for reason in row["reasons"]
        )
        lines.append(f"- `{row['id']}` — {reason_text}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--questions", type=Path, default=Path("data/beaver/questions.jsonl")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/beaver/question_specification.jsonl"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("data/beaver/question_specification.md"),
    )
    args = parser.parse_args()
    questions = [
        json.loads(line)
        for line in args.questions.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    audited = audit_questions(questions)
    args.output.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in audited),
        encoding="utf-8",
        newline="\n",
    )
    write_report(args.report, audited)


if __name__ == "__main__":
    main()
