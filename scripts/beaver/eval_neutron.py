"""Scrappy DeepSeek agent evaluation for the BEAVER Neutron split."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from trilogy import Dialects, Environment
from trilogy.dialect.config import MySQLConfig


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUESTIONS = REPO_ROOT / "data/beaver/questions.jsonl"
DEFAULT_SPECIFICATION = REPO_ROOT / "data/beaver/question_specification.jsonl"
DEFAULT_MODEL_DIR = REPO_ROOT / "trilogy_public_models/mysql/beaver_neutron"
DEFAULT_RESULTS = REPO_ROOT / ".cache/beaver/evals/neutron"
DEFAULT_ENV_FILE = REPO_ROOT / ".env"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_questions(path: Path) -> list[dict[str, Any]]:
    questions: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("split") == "neutron":
                questions.append(row)
    return questions


def load_specification(path: Path) -> dict[str, dict[str, Any]]:
    return {
        row["id"]: row
        for row in (
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }


def canonicalize_rows(rows: list[tuple[Any, ...]]) -> set[tuple[str, ...]]:
    """Match BEAVER: ignore row order, duplicates, and column names."""
    return {tuple(str(value).strip() for value in row) for row in rows}


def wilson_interval(passed: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total == 0:
        return 0.0, 0.0
    proportion = passed / total
    denominator = 1 + z**2 / total
    center = (proportion + z**2 / (2 * total)) / denominator
    margin = (
        z
        * math.sqrt(proportion * (1 - proportion) / total + z**2 / (4 * total**2))
        / denominator
    )
    return center - margin, center + margin


def mysql_config(args: argparse.Namespace) -> MySQLConfig:
    return MySQLConfig(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database="neutron",
    )


def execute_reference(sql: str, args: argparse.Namespace) -> list[tuple[Any, ...]]:
    connection = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.username,
        password=args.password,
        database="neutron",
        read_timeout=args.query_timeout,
        write_timeout=args.query_timeout,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SET SESSION MAX_EXECUTION_TIME={args.query_timeout * 1000}"
            )
            cursor.execute(sql)
            return list(cursor.fetchall())
    finally:
        connection.close()


def execute_candidate(
    candidate: Path, args: argparse.Namespace
) -> list[tuple[Any, ...]]:
    environment = Environment(working_path=candidate.parent)
    executor = Dialects.MYSQL.default_executor(
        environment=environment,
        conf=mysql_config(args),
    )
    executor.execute_raw_sql(
        f"SET SESSION MAX_EXECUTION_TIME={args.query_timeout * 1000}"
    )
    queries = executor.parse_text(
        candidate.read_text(encoding="utf-8"),
        root=candidate,
    )
    rows: list[tuple[Any, ...]] | None = None
    for query in queries:
        result = executor.execute_query(query)
        if result is not None:
            rows = list(result.fetchall())
    if rows is None:
        raise ValueError("Candidate contains no executable query")
    return rows


def write_workspace(
    workspace: Path,
    model_dir: Path,
    args: argparse.Namespace,
) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    raw = workspace / "raw"
    if raw.exists():
        shutil.rmtree(raw)
    shutil.copytree(model_dir, raw)
    config = {
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "password": args.password,
        "database": "neutron",
    }
    config_lines = "\n".join(
        f"{key} = {json.dumps(value)}" for key, value in config.items()
    )
    (workspace / "trilogy.toml").write_text(
        "[engine]\n"
        'dialect = "mysql"\n\n'
        "[engine.config]\n"
        f"{config_lines}\n\n"
        "[agent]\n"
        'provider = "deepseek"\n'
        f"model = {json.dumps(args.model)}\n"
        'api_key_env = "DEEPSEEK_API_KEY"\n'
        f"max_iterations = {args.max_iterations}\n"
        "tool_output_limit = 32768\n"
        "quiet = true\n"
        "disable_todo = true\n"
        "force_tool_choice = false\n"
        "allow_database_introspection = false\n"
        "allow_file_read = false\n"
        "disable_reviewer = true\n",
        encoding="utf-8",
    )


def build_task(question: dict[str, Any]) -> str:
    return (
        "Answer this data question using the prebuilt Trilogy model under raw/. "
        "Call `trilogy agent-info` first, explore the relevant model concepts, "
        "and write the final executable query to answer.preql in the workspace "
        "root, never inside raw/. Imports in answer.preql must therefore begin "
        "with raw., for example `import raw.core.networks as networks;`. Run "
        "`trilogy run answer.preql` and check the result before finishing. "
        "Use the backend configured in trilogy.toml; "
        "do not pass a dialect to `trilogy run`. Do not use raw SQL or inspect "
        f"the database schema directly.\n\nQuestion:\n{question['question']}"
    )


def parse_usage(log_path: Path) -> dict[str, int]:
    usage = {
        "iterations": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "tool_calls": 0,
    }
    if not log_path.exists():
        return usage
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "llm_response":
            usage["iterations"] += 1
            event_usage = event.get("usage") or {}
            for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                usage[key] += event_usage.get(key) or 0
        elif event.get("type") == "tool_call":
            usage["tool_calls"] += 1
    return usage


def run_question(
    question: dict[str, Any],
    run_dir: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    question_id = question["id"]
    case_dir = run_dir / question_id
    workspace = case_dir / "workspace"
    write_workspace(workspace, args.model_dir, args)
    task = build_task(question)
    (case_dir / "task.txt").write_text(task, encoding="utf-8")
    log_path = case_dir / "agent_log.jsonl"
    started = time.perf_counter()
    timed_out = False
    try:
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "trilogy.scripts.trilogy",
                "agent",
                "--provider",
                "deepseek",
                "--model",
                args.model,
                "--toolset",
                "trilogy",
                "--log-file",
                str(log_path),
                task,
            ],
            cwd=workspace,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=args.agent_timeout,
            check=False,
        )
        exit_code = process.returncode
        output = process.stdout + process.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        exit_code = -1
        stdout = (
            exc.stdout.decode("utf-8", "replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or "")
        )
        stderr = (
            exc.stderr.decode("utf-8", "replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        output = stdout + stderr
    duration = time.perf_counter() - started
    (case_dir / "agent_output.txt").write_text(output, encoding="utf-8")

    candidate = workspace / "answer.preql"
    status = "missing"
    detail = ""
    reference_rows = candidate_rows = 0
    if timed_out and not candidate.exists():
        status = "timeout"
    elif not candidate.exists():
        if exit_code == 2:
            status = "exhausted"
        detail = f"agent exit={exit_code}; answer.preql was not created"
    else:
        shutil.copy2(candidate, case_dir / "answer.preql")
        try:
            expected = execute_reference(question["sql"], args)
            actual = execute_candidate(candidate, args)
            reference_rows = len(expected)
            candidate_rows = len(actual)
            status = (
                "pass"
                if canonicalize_rows(expected) == canonicalize_rows(actual)
                else "fail"
            )
            if status == "fail":
                detail = (
                    f"result mismatch: reference={reference_rows} rows, "
                    f"candidate={candidate_rows} rows"
                )
            if timed_out:
                detail = (
                    "agent timed out after writing answer.preql"
                    + (f"; {detail}" if detail else "")
                )
        except Exception as exc:
            status = "error"
            detail = f"{type(exc).__name__}: {exc}"

    return {
        "id": question_id,
        "status": status,
        "detail": detail,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "duration_seconds": round(duration, 2),
        "reference_rows": reference_rows,
        "candidate_rows": candidate_rows,
        **parse_usage(log_path),
    }


def write_report(
    run_dir: Path, results: list[dict[str, Any]], args: argparse.Namespace
) -> None:
    passed = sum(row["status"] == "pass" for row in results)
    interval_low, interval_high = wilson_interval(passed, len(results))
    report = {
        "model": args.model,
        "provider": "deepseek",
        "pass_count": passed,
        "total": len(results),
        "pass_rate": passed / len(results) if results else 0,
        "pass_rate_wilson_95": [interval_low, interval_high],
        "seed": args.seed,
        "max_iterations": args.max_iterations,
        "agent_timeout": args.agent_timeout,
        "query_timeout": args.query_timeout,
        "question_ids": [row["id"] for row in results],
        "total_tokens": sum(row["total_tokens"] for row in results),
        "results": results,
    }
    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# BEAVER Neutron eval",
        "",
        f"- Model: `deepseek/{args.model}`",
        f"- Passed: {passed}/{len(results)} ({report['pass_rate']:.1%})",
        f"- 95% Wilson interval: {interval_low:.1%}–{interval_high:.1%}",
        f"- Sampling seed: `{args.seed}`",
        f"- Agent budget: `{args.max_iterations}` iterations, "
        f"`{args.agent_timeout}`s timeout",
        f"- Tokens: {report['total_tokens']:,}",
        "",
        "| Question | Status | Tokens | Seconds | Detail |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    lines.extend(
        f"| {row['id']} | {row['status']} | {row['total_tokens']:,} | "
        f"{row['duration_seconds']:.1f} | {row['detail'].replace('|', '/')} |"
        for row in results
    )
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", type=Path, default=DEFAULT_QUESTIONS)
    parser.add_argument("--specification", type=Path, default=DEFAULT_SPECIFICATION)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--model", default="deepseek-v4-flash")
    parser.add_argument(
        "--query-ids", help="comma-separated IDs, e.g. neutron_0,neutron_10"
    )
    parser.add_argument(
        "--num-queries",
        "--sample-size",
        dest="num_queries",
        type=int,
        default=3,
        help="seeded random sample size from the specified pool",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-iterations", type=int, default=40)
    parser.add_argument("--agent-timeout", type=int, default=600)
    parser.add_argument("--query-timeout", type=int, default=10)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--username", default="root")
    parser.add_argument("--password", default="beaver")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--include-unspecified",
        action="store_true",
        help="include questions flagged with hidden gold dependencies",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env(args.env_file)
    questions = load_questions(args.questions)
    specification = load_specification(args.specification)
    excluded = {
        row["id"]
        for row in questions
        if specification.get(row["id"], {}).get("status") == "unspecified"
    }
    if not args.include_unspecified:
        questions = [row for row in questions if row["id"] not in excluded]
    if args.query_ids:
        wanted = {value.strip() for value in args.query_ids.split(",") if value.strip()}
        selected = [row for row in questions if row["id"] in wanted]
        missing = wanted - {row["id"] for row in selected}
        if missing:
            hidden = missing & excluded
            if hidden:
                raise ValueError(
                    f"Excluded unspecified Neutron question IDs: {sorted(hidden)}. "
                    "Pass --include-unspecified to run them explicitly."
                )
            raise ValueError(f"Unknown Neutron question IDs: {sorted(missing)}")
    else:
        if args.num_queries > len(questions):
            raise ValueError(
                f"Requested {args.num_queries} questions from a pool of "
                f"{len(questions)}"
            )
        selected = random.Random(args.seed).sample(questions, args.num_queries)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "model": args.model,
                    "questions": [row["id"] for row in selected],
                    "seed": args.seed,
                    "specified_pool_size": len(questions),
                    "api_key_available": bool(os.environ.get("DEEPSEEK_API_KEY")),
                    "model_dir": str(args.model_dir),
                },
                indent=2,
            )
        )
        return 0
    if not os.environ.get("DEEPSEEK_API_KEY"):
        raise ValueError(
            f"DEEPSEEK_API_KEY is unset and was not found in {args.env_file}"
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = args.results_dir / timestamp
    run_dir.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for index, question in enumerate(selected, 1):
        print(f"[{index}/{len(selected)}] {question['id']}", flush=True)
        result = run_question(question, run_dir, args)
        results.append(result)
        write_report(run_dir, results, args)
        print(
            f"  {result['status']} in {result['duration_seconds']:.1f}s, "
            f"{result['total_tokens']:,} tokens",
            flush=True,
        )
    print(f"Report: {run_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
