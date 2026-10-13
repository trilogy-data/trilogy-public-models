"""Offline ingestion contract and semantic model tests; no live API or GCS writes."""

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from trilogy import Dialects, Environment, parse

BASE = Path(__file__).parents[1] / "trilogy_public_models/duckdb/danish_democracy"
SPEC = importlib.util.spec_from_file_location(
    "danish_oda_source", BASE / "data/ingest/source.py"
)
source = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = source
SPEC.loader.exec_module(source)
STAMP = "2026-10-01T10:00:00.1234567"


def ballot(id, vote=1, actor=1, type=1, stamp=STAMP):
    return {
        "id": id,
        "afstemningid": vote,
        "aktørid": actor,
        "typeid": type,
        "opdateringsdato": stamp,
    }


class FakeAPI:
    def __init__(self, rows, changed=None, marker_after=None, stop=False):
        self.rows = rows
        self.changed = rows if changed is None else changed
        self.marker_after = marker_after
        self.marker_calls = 0
        self.stop = stop
        self.since = []
        self.id_scans = 0

    def marker(self, endpoint):
        self.marker_calls += 1
        if self.marker_calls > 1 and self.marker_after:
            return self.marker_after
        return source.Marker(
            len(self.rows), max((r["opdateringsdato"] for r in self.rows), default="")
        )

    def pages(self, endpoint, columns, since="", after_id=-1):
        self.since.append(since)
        if columns == ["id"]:
            self.id_scans += 1
        if endpoint == "Stemmetype":
            yield [
                {"id": 1, "type": "For"},
                {"id": 2, "type": "Imod"},
                {"id": 3, "type": "Fravær"},
                {"id": 4, "type": "Hverken for eller imod"},
            ]
            return
        rows = self.rows if columns == ["id"] or not since else self.changed
        for row in rows:
            if row["id"] > after_id:
                yield ({key: row[key] for key in columns},)
                if self.stop:
                    raise source.IncompleteExtract("budget exhausted")


def checkpoint(tmp_path):
    return source.Checkpoint(tmp_path / "state.sqlite")


def test_same_timestamp_updates_and_deletions_with_equal_counts(tmp_path):
    state = checkpoint(tmp_path)
    initial = source.extract("ballot", state, FakeAPI([ballot(1), ballot(2)]))
    assert initial.num_rows == 2
    # ID 1 disappears, ID 3 arrives, ID 2 changes at the exact cursor timestamp.
    api = FakeAPI([ballot(2, type=4), ballot(3, type=3)])
    result = source.extract("ballot", state, api).to_pylist()
    assert [r["id"] for r in result] == [2, 3]
    assert [r["type_name"] for r in result] == ["Hverken for eller imod", "Fravær"]
    assert STAMP in api.since
    assert result[0]["source_updated_at"] == STAMP
    state.close()


def test_changed_source_fails_without_advancing_watermark(tmp_path):
    state = checkpoint(tmp_path)
    source.extract("ballot", state, FakeAPI([ballot(1)]))
    newer = "2026-10-02T10:00:00.1234567"
    api = FakeAPI([ballot(1, stamp=newer)], marker_after=source.Marker(2, newer))
    with pytest.raises(source.IncompleteExtract, match="source changed"):
        source.extract("ballot", state, api)
    assert state.get("watermark") == STAMP
    state.close()


def test_backfill_resumes_after_budget_and_replays_updates(tmp_path):
    state = checkpoint(tmp_path)
    with pytest.raises(source.IncompleteExtract):
        source.extract("ballot", state, FakeAPI([ballot(1), ballot(2)], stop=True))
    assert state.get("last_id") == "1"
    state.close()
    state = checkpoint(tmp_path)
    result = source.extract("ballot", state, FakeAPI([ballot(1, type=2), ballot(2)]))
    assert result.num_rows == 2
    assert result.to_pylist()[0]["type_id"] == 2
    state.close()


def test_allowlist_excludes_biography_and_personal_details():
    actor = {
        "id": 1,
        "navn": "Example member",
        "typeid": 5,
        "gruppenavnkort": None,
        "startdato": None,
        "slutdato": None,
        "opdateringsdato": STAMP,
        "biografi": "excluded",
        "email": "excluded",
    }
    result = source.normalize("actor", actor, {"type": {5: "Medlem"}})
    assert "biografi" not in result and "email" not in result
    assert set(result) == set(source.schema("actor").names)


def test_pagination_rejects_repeat_and_keeps_seven_digit_cursor():
    api = source.OData()
    calls = []

    def get(endpoint, **params):
        calls.append(params)
        return {"value": [ballot(2)], "odata.nextLink": "irrelevant"}

    api.get = get
    rows = api.pages("Stemme", ["id"], since=STAMP)
    next(rows)
    with pytest.raises(source.IncompleteExtract, match="non-monotonic"):
        next(rows)
    assert STAMP in calls[0]["$filter"]
    assert "id gt 2" in calls[1]["$filter"]
    assert "$top" not in calls[0]


def test_request_budget_includes_transient_retries(monkeypatch):
    calls = []

    def unavailable(request, **kwargs):
        calls.append(request.full_url)
        raise source.URLError("temporary failure")

    monkeypatch.setattr(source, "urlopen", unavailable)
    monkeypatch.setattr(source.time, "sleep", lambda _: None)
    api = source.OData(max_pages=2)
    with pytest.raises(source.IncompleteExtract, match="request budget exhausted"):
        api.get("Periode")
    assert len(calls) == 2
    assert api.remaining == 0


def fixtures():
    def row(entity, id, **values):
        result = {
            name: None
            if nullable
            else 1
            if kind == "int"
            else False
            if kind == "bool"
            else "2026-10-01T10:00:00"
            if kind == "datetime"
            else "fixture"
            for _, name, kind, nullable in source.ENTITIES[entity]["fields"]
        }
        result.update(
            id=id, source_updated_at=STAMP, updated_at=source._datetime(STAMP)
        )
        result.update(values)
        for name in source.ENTITIES[entity]["lookups"]:
            result[name + "_name"] = "fixture"
        for _, name, kind, _ in source.ENTITIES[entity]["fields"]:
            if kind == "datetime" and isinstance(result[name], str):
                result[name] = source._datetime(result[name])
        return result

    return {
        "period": [row("period", 1, title="2026-27")],
        "meeting": [row("meeting", 1, period_id=1, held_at=source._datetime(STAMP))],
        "case": [row("case", 1, period_id=1, short_title="Example bill")],
        "case_step": [row("case_step", 1, case_id=1)],
        "actor": [
            row("actor", 1, name="Member one"),
            row("actor", 2, name="Member two"),
        ],
        "vote": [
            row(
                "vote",
                1,
                meeting_id=1,
                case_step_id=None,
                approved=False,
                comment="Correction noted",
            ),
            row("vote", 2, meeting_id=1, case_step_id=1, approved=True),
        ],
        "ballot": [
            row("ballot", i, vote_id=(i + 1) // 2, actor_id=1 + (i % 2), type_id=i)
            for i in range(1, 5)
        ],
    }


def local_environment(tmp_path):
    for entity, records in fixtures().items():
        import pyarrow.parquet as pq

        path = tmp_path / f"{entity}.parquet"
        pq.write_table(
            pa.Table.from_pylist(records, schema=source.schema(entity)), path
        )
        preql = (
            (BASE / f"{entity}.preql")
            .read_text()
            .replace(source.PUBLIC + f"{entity}.parquet", str(path))
        )
        (tmp_path / f"{entity}.preql").write_text(preql)
    (tmp_path / "entrypoint.preql").write_text((BASE / "entrypoint.preql").read_text())
    return parse(
        (tmp_path / "entrypoint.preql").read_text(),
        Environment(working_path=str(tmp_path)),
    )[0]


def test_models_compile_and_examples_execute(tmp_path):
    env = local_environment(tmp_path)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        assert (
            executor.execute_text("SELECT count(ballot.id) -> ballots;")[-1].fetchone()[
                0
            ]
            == 4
        )
        assert (
            executor.execute_text("SELECT count(vote.id) -> votes;")[-1].fetchone()[0]
            == 2
        )
        # Nullable case-step must not discard the otherwise valid roll call.
        rows = executor.execute_text(
            "SELECT vote.id, vote.case_step.case.short_title ORDER BY vote.id;"
        )[-1].fetchall()
        assert rows == [(1, None), (2, "Example bill")]
        example_dir = BASE.parents[2] / "examples/duckdb/danish_democracy"
        assert len(list(example_dir.glob("*.preql"))) == 4
        for path in example_dir.glob("*.preql"):
            executor.execute_text(path.read_text())[-1].fetchall()
    finally:
        executor.close()


def test_ingest_declarations_and_cloud_gate():
    from trilogy.execution.config import load_config_file

    for path in (BASE / "data").glob("*.preql"):
        parse(path.read_text(), Environment(working_path=str(path.parent)))
    load_config_file(BASE / "data/trilogy.toml")
    assert "\n[cloud]" not in (BASE / "data/trilogy.toml").read_text()
    assert (
        'schedule = "0 0 5 * * *"'
        in (BASE / "data/trilogy_cloud.example.toml").read_text()
    )


def test_reconciliation_timestamp_survives_published_tile_round_trip(tmp_path):
    state = checkpoint(tmp_path / "first")
    initial = source.extract("ballot", state, FakeAPI([ballot(1)]))
    assert initial.column("ids_reconciled_at")[0].as_py() is not None
    restored = checkpoint(tmp_path / "restored")
    restored.seed("ballot", initial)
    api = FakeAPI([ballot(1)])
    source.extract("ballot", restored, api)
    assert api.id_scans == 0
    # A forced/expired full reconciliation does run, even with stable counts.
    source.extract("ballot", restored, api, reconciliation_days=0)
    assert api.id_scans == 1
    state.close()
    restored.close()


def test_unpublished_model_is_not_in_studio_catalog():
    import json

    repo = BASE.parents[2]
    assert (BASE / ".unpublished").exists()
    assert not (repo / "studio/danish_democracy.json").exists()
    index = json.loads((repo / "studio/index.json").read_text())
    assert "danish_democracy" not in {item["name"] for item in index["files"]}


class InterruptedAPI(FakeAPI):
    def __init__(self, rows, phase):
        super().__init__(rows)
        self.phase = phase
        self.starts = []

    def pages(self, endpoint, columns, since="", after_id=-1):
        self.starts.append((columns, since, after_id))
        for page in super().pages(endpoint, columns, since, after_id):
            yield page
            if (self.phase == "ids" and columns == ["id"]) or (
                self.phase == "delta" and since and columns != ["id"]
            ):
                raise source.IncompleteExtract("budget exhausted")


def test_incremental_replay_resumes_with_same_checkpoint(tmp_path):
    state = checkpoint(tmp_path)
    source.extract("ballot", state, FakeAPI([ballot(1)]))
    rows = [ballot(1, type=2), ballot(2), ballot(3)]
    with pytest.raises(source.IncompleteExtract):
        source.extract("ballot", state, InterruptedAPI(rows, "delta"))
    assert state.get("replay_last") == "1"
    state.close()
    state = checkpoint(tmp_path)
    api = InterruptedAPI(rows, "none")
    result = source.extract("ballot", state, api)
    assert result.num_rows == 3
    assert any(
        since == STAMP and after == 1
        for columns, since, after in api.starts
        if columns != ["id"]
    )
    state.close()


def test_id_reconciliation_resumes_with_same_checkpoint(tmp_path):
    state = checkpoint(tmp_path)
    rows = [ballot(1), ballot(2)]
    with pytest.raises(source.IncompleteExtract):
        source.extract("ballot", state, InterruptedAPI(rows, "ids"))
    assert state.get("id_scan_last") == "1"
    state.close()
    state = checkpoint(tmp_path)
    api = InterruptedAPI(rows, "none")
    result = source.extract("ballot", state, api)
    assert result.num_rows == 2
    assert any(after == 1 for columns, since, after in api.starts if columns == ["id"])
    state.close()
