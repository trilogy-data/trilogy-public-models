"""Check process self imports against a fixed snapshot, without running osquery."""

from pathlib import Path

import pytest
from trilogy import Dialects, Environment
from trilogy.core.enums import ValidationScope
from trilogy.core.validation.environment import validate_environment

ROOT = Path(__file__).parents[1]
MODEL = ROOT / "trilogy_public_models/duckdb/osquery"
EXAMPLE = ROOT / "examples/duckdb/osquery/memory_by_application.preql"


def test_entrypoint_has_no_unbound_concepts():
    environment = Environment(working_path=MODEL)
    environment.parse("import entrypoint;")
    results = validate_environment(environment, scope=ValidationScope.CONCEPTS)
    assert not [result.result for result in results if result.result]


@pytest.fixture
def processes(tmp_path):
    for path in MODEL.glob("*.preql"):
        source = path.read_text(encoding="utf-8")
        if path.name == "process.preql":
            source = source.replace(
                "file `./ingest/osquery.py#process`;",
                """query '''
                select pid, parent_pid, app_parent_pid, name,
                    memory_mib * 1048576 as resident_bytes,
                    1000 as user_cpu_ms, 500 as system_cpu_ms
                from (values
                    (10, 999, null, 'editor', 1),
                    (11, 10, 10, 'worker', 2),
                    (12, 11, 11, 'helper', 3),
                    (20, 999, null, 'editor', 6),
                    (30, 999, null, 'browser', 2),
                    (31, 30, 30, 'helper', 3),
                    (40, null, null, 'protected', null),
                    (50, 999, null, 'singleton', 4),
                    (60, 999, null, 'tiny', 1),
                    (70, 999, null, 'idle', 0)
                ) as snapshot(pid, parent_pid, app_parent_pid, name, memory_mib)
                ''';""",
            )
        (tmp_path / path.name).write_text(source, encoding="utf-8")
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    try:
        yield executor
    finally:
        executor.close()


def test_application_memory_example(processes):
    rows = processes.execute_text(EXAMPLE.read_text(encoding="utf-8"))[-1].fetchall()
    # Grandchildren count toward their root, same-name roots stay separate,
    # ties use PID, and null memory sorts after known totals before LIMIT 5.
    assert rows == [
        (10, "editor", 3, 6),
        (20, "editor", 1, 6),
        (30, "browser", 2, 5),
        (50, "singleton", 1, 4),
        (60, "tiny", 1, 1),
    ]


def test_parent_role_keeps_root_processes(processes):
    rows = processes.execute_text(
        "import process as p; where p.pid <= 12 "
        "select p.pid, p.parent.name order by p.pid;"
    )[-1].fetchall()
    assert rows == [(10, None), (11, "editor"), (12, "worker")]


def test_entrypoint_exposes_application_totals(processes):
    rows = processes.execute_text(
        "import entrypoint; select process.app.pid, process.total_cpu_seconds "
        "order by process.app.pid limit 3;"
    )[-1].fetchall()
    assert rows == [(10, 4.5), (20, 1.5), (30, 3)]
