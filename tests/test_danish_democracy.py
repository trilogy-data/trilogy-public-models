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
        'schedule = "0 0 5 * * *"' in (BASE / "trilogy_cloud.example.toml").read_text()
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


@pytest.fixture
def generation(monkeypatch):
    monkeypatch.setitem(sys.modules, "source", source)
    spec = importlib.util.spec_from_file_location(
        "danish_generation", BASE / "data/ingest/generation.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generation_seeds(directory):
    import pyarrow.parquet as pq

    directory.mkdir()
    for entity, records in fixtures().items():
        for row in records:
            row["ids_reconciled_at"] = source._datetime(STAMP)
        pq.write_table(
            pa.Table.from_pylist(records, schema=source.schema(entity)),
            directory / f"{entity}.parquet",
        )


def test_generation_pins_all_models_and_examples_execute(tmp_path, generation):
    import json

    seeds, bundles = tmp_path / "seeds", tmp_path / "generations"
    generation_seeds(seeds)
    bundle = generation.prepare(seeds, bundles)
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert set(manifest["files"]) == set(source.ENTITIES)
    pinned = source.PUBLIC + f"generations/{bundle.name}/"
    local = tmp_path / "query"
    local.mkdir()
    for path in (bundle / "model").glob("*.preql"):
        content = path.read_text()
        if path.name != "entrypoint.preql":
            assert pinned in content
        (local / path.name).write_text(content.replace(pinned, bundle.as_posix() + "/"))
    env = parse(
        (local / "entrypoint.preql").read_text(), Environment(working_path=str(local))
    )[0]
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        for example in (BASE.parents[2] / "examples/duckdb/danish_democracy").glob(
            "*.preql"
        ):
            executor.execute_text(example.read_text())[-1].fetchall()
    finally:
        executor.close()
    original = (bundle / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        generation.prepare(seeds, bundles)
    assert (bundle / "manifest.json").read_bytes() == original
    assert list(bundles.iterdir()) == [bundle]


@pytest.mark.parametrize("failure", ["missing", "duplicate", "unreconciled", "schema"])
def test_generation_fails_without_exposing_partial_bundle(
    tmp_path, generation, failure
):
    import pyarrow.parquet as pq

    seeds, bundles = tmp_path / "seeds", tmp_path / "generations"
    generation_seeds(seeds)
    path = seeds / "ballot.parquet"
    if failure == "missing":
        path.unlink()
    else:
        records = pq.read_table(path).to_pylist()
        if failure == "duplicate":
            records.append(records[0])
        elif failure == "unreconciled":
            records[0]["ids_reconciled_at"] = None
        pq.write_table(
            pa.Table.from_pylist(records, schema=source.schema("ballot")), path
        )
        if failure == "schema":
            pq.write_table(pq.read_table(path).drop(["source_updated_at"]), path)
    with pytest.raises((source.IncompleteExtract, FileNotFoundError)):
        generation.prepare(seeds, bundles)
    assert list(bundles.iterdir()) == []


@pytest.mark.parametrize(
    "missing",
    [
        None,
        "entrypoint",
        "actor",
        "period",
        "meeting",
        "case",
        "case_step",
        "vote",
        "ballot",
        "empty-entrypoint",
        "data-only",
    ],
)
def test_generation_from_cloud_bundle(tmp_path, missing):
    """Exercise the shipped helper in isolation from the repository models."""
    import json
    import subprocess
    import tomllib

    from trilogy.scripts.cloud import collect_files, discover_projects

    # The shipped configuration remains non-discoverable. Only the proposed
    # template defines a root bundle with a refresh entrypoint in data/.
    assert discover_projects(BASE) == []
    template = tomllib.loads((BASE / "trilogy_cloud.example.toml").read_text())
    assert template["cloud"]["operation"] == "refresh"
    assert template["cloud"]["entrypoint"] == "data"
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    bundle_root = BASE / "data" if missing == "data-only" else BASE
    for item in collect_files(bundle_root):
        path = deployment / item["name"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(item["content"])
    if missing and missing != "data-only":
        if missing == "empty-entrypoint":
            (deployment / "entrypoint.preql").write_text("\n")
        else:
            (deployment / f"{missing}.preql").unlink()
    seeds, generations = tmp_path / "seeds", tmp_path / "generations"
    generation_seeds(seeds)
    helper = deployment / (
        "ingest/generation.py"
        if missing == "data-only"
        else "data/ingest/generation.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            "--source-dir",
            str(seeds),
            "--output-dir",
            str(generations),
        ],
        check=False,
        cwd=deployment,
        capture_output=True,
        text=True,
    )
    if missing:
        assert result.returncode != 0
        assert "missing or empty required query model:" in result.stderr
        assert not generations.exists() or not list(generations.iterdir())
    else:
        assert result.returncode == 0, result.stderr
        generation_dir = Path(result.stdout.strip())
        manifest = json.loads((generation_dir / "manifest.json").read_text())
        expected = {
            "entrypoint.preql",
            *(f"{entity}.preql" for entity in source.ENTITIES),
        }
        assert set(manifest["models"]) == expected
        assert (generation_dir / manifest["entrypoint"]).is_file()
        assert set(manifest["files"]) == set(source.ENTITIES)
        for name, record in manifest["models"].items():
            content = (generation_dir / record["path"]).read_text()
            assert content.strip()
            if name != "entrypoint.preql":
                assert source.PUBLIC + f"generations/{generation_dir.name}/" in content


class MemoryCheckpointStore:
    def __init__(self):
        self.objects = {}
        self.history = []
        self.fail_save = False

    def fetch(self, entity, path):
        if entity not in self.objects:
            return False
        path.write_bytes(self.objects[entity])
        return True

    def save(self, entity, path):
        if self.fail_save:
            raise OSError("storage unavailable")
        data = path.read_bytes()
        self.objects[entity] = data
        self.history.append((entity, data))


@pytest.fixture
def remote_store(tmp_path, monkeypatch):
    # Initialization uses a complete seed only on NoSuchKey, then every run
    # must use the remote SQLite rows and cursor even if publication failed.
    local = checkpoint(tmp_path)
    seed = source.extract("ballot", local, FakeAPI([ballot(1)]))
    local.close()
    seed = seed.set_column(
        seed.schema.get_field_index("ids_reconciled_at"),
        "ids_reconciled_at",
        pa.array(
            [source._datetime("2000-01-01T00:00:00")] * seed.num_rows,
            type=pa.timestamp("us"),
        ),
    )
    monkeypatch.setattr(source, "published_table", lambda _: seed)
    return MemoryCheckpointStore()


def test_remote_checkpoint_first_run_and_resume(remote_store, monkeypatch):
    result = source.remote_extract(
        "ballot", remote_store, FakeAPI([ballot(1), ballot(2)])
    )
    assert result.num_rows == 2
    monkeypatch.setattr(
        source, "published_table", lambda _: pytest.fail("must not reseed")
    )
    api = FakeAPI([ballot(1, type=4), ballot(2), ballot(3)])
    result = source.remote_extract("ballot", remote_store, api)
    assert result.num_rows == 3
    assert result.to_pylist()[0]["type_id"] == 4
    assert STAMP in api.since
    assert len({source.CheckpointStore.key(entity) for entity in source.ENTITIES}) == 7
    assert all(
        "checkpoints/v1/" in source.CheckpointStore.key(e) for e in source.ENTITIES
    )


@pytest.mark.parametrize(
    "phase,cursor", [("delta", "replay_last"), ("ids", "id_scan_last")]
)
def test_remote_interrupted_pages_resume(tmp_path, remote_store, phase, cursor):
    rows = [ballot(1), ballot(2), ballot(3)]
    with pytest.raises(source.IncompleteExtract, match="budget"):
        source.remote_extract("ballot", remote_store, InterruptedAPI(rows, phase))
    saved = tmp_path / "restored.sqlite"
    saved.write_bytes(remote_store.objects["ballot"])
    state = source.Checkpoint(saved)
    assert state.get(cursor) == "1"
    if phase == "ids":
        assert state.db.execute("SELECT id FROM live_ids").fetchall() == [(1,)]
    state.close()
    api = InterruptedAPI(rows, "none")
    assert source.remote_extract("ballot", remote_store, api).num_rows == 3
    assert any(
        after == 1 and (columns == ["id"]) == (phase == "ids")
        for columns, _, after in api.starts
    )


def test_remote_full_backfill_checkpoint_resume(tmp_path, remote_store):
    # An explicitly bootstrapped full-phase snapshot can be resumed too.
    path = tmp_path / "full.sqlite"
    state = source.Checkpoint(path)
    state.set("entity", "ballot")
    state.set("checkpoint_format", "1")
    state.set("phase", "full")
    state.db.commit()
    state.close()
    remote_store.objects["ballot"] = path.read_bytes()
    with pytest.raises(source.IncompleteExtract):
        source.remote_extract(
            "ballot", remote_store, FakeAPI([ballot(1), ballot(2)], stop=True)
        )
    path.write_bytes(remote_store.objects["ballot"])
    state = source.Checkpoint(path)
    assert state.get("last_id") == "1"
    assert state.db.execute("SELECT count(*) FROM rows").fetchone() == (1,)
    state.close()
    assert (
        source.remote_extract(
            "ballot", remote_store, FakeAPI([ballot(1), ballot(2)])
        ).num_rows
        == 2
    )


def test_remote_snapshot_wal_and_transaction_consistency(
    tmp_path, remote_store, monkeypatch
):
    def interrupted(entity, state, api):
        state.db.execute("PRAGMA journal_mode=WAL")
        state.db.execute("PRAGMA wal_autocheckpoint=0")
        state.upsert([ballot(2)])
        state.set("replay_last", "2")
        state.commit()
        state.upsert([ballot(3)])
        state.set("replay_last", "3")
        raise KeyboardInterrupt()

    monkeypatch.setattr(source, "extract", interrupted)
    with pytest.raises(KeyboardInterrupt):
        source.remote_extract("ballot", remote_store, FakeAPI([]))
    path = tmp_path / "snapshot.sqlite"
    path.write_bytes(remote_store.objects["ballot"])
    source.validate_checkpoint(path, "ballot")
    state = source.Checkpoint(path)
    assert state.get("replay_last") == "2"
    assert state.db.execute("SELECT id FROM rows ORDER BY id").fetchall() == [
        (1,),
        (2,),
    ]
    state.close()


def test_remote_periodic_snapshots(tmp_path, remote_store, monkeypatch):
    tick = iter(range(0, 100000, 301))
    monkeypatch.setattr(source.time, "monotonic", lambda: next(tick))
    source.remote_extract("ballot", remote_store, FakeAPI([ballot(1), ballot(2)]))
    cursors = []
    for i, (_, data) in enumerate(remote_store.history):
        path = tmp_path / f"snapshot-{i}.sqlite"
        path.write_bytes(data)
        source.validate_checkpoint(path, "ballot")
        state = source.Checkpoint(path)
        cursors.append(state.get("replay_last"))
        state.close()
    assert "1" in cursors and "2" in cursors


@pytest.mark.parametrize(
    "failure", ["corrupt", "wrong-entity", "unavailable", "upload"]
)
def test_remote_storage_failures_do_not_reset_or_emit(
    tmp_path, remote_store, monkeypatch, failure
):
    source.remote_extract("ballot", remote_store, FakeAPI([ballot(1)]))
    if failure == "corrupt":
        remote_store.objects["ballot"] = b"corrupt sqlite"
    elif failure == "wrong-entity":
        path = tmp_path / "wrong.sqlite"
        path.write_bytes(remote_store.objects["ballot"])
        state = source.Checkpoint(path)
        state.set("entity", "actor")
        state.db.commit()
        state.close()
        remote_store.objects["ballot"] = path.read_bytes()
    elif failure == "unavailable":

        def unavailable(*args):
            raise OSError("download unavailable")

        remote_store.fetch = unavailable
    else:
        remote_store.fail_save = True
    previous = remote_store.objects.copy()
    monkeypatch.setattr(
        source, "published_table", lambda _: pytest.fail("must not reseed")
    )
    monkeypatch.setattr(source, "CheckpointStore", lambda: remote_store)
    monkeypatch.setattr(source, "OData", lambda _: FakeAPI([ballot(1), ballot(2)]))
    monkeypatch.delenv("DANISH_ODA_BACKFILL_DIR", raising=False)
    monkeypatch.setattr(
        source.pa.ipc, "new_stream", lambda *args: pytest.fail("must not emit")
    )
    with pytest.raises((source.IncompleteExtract, OSError)):
        source.emit("ballot")
    assert remote_store.objects == previous


def test_remote_missing_seed_does_not_initialize_checkpoint(remote_store, monkeypatch):
    def missing(_):
        raise source.IncompleteExtract("published tile is absent")

    monkeypatch.setattr(source, "published_table", missing)
    with pytest.raises(source.IncompleteExtract, match="published tile"):
        source.remote_extract("ballot", remote_store, FakeAPI([]))
    assert remote_store.objects == {}


def test_checkpoint_storage_protocol(tmp_path, monkeypatch):
    import base64
    import hashlib
    import io

    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
    from botocore.stub import Stubber

    monkeypatch.setenv("GOOGLE_HMAC_KEY", "test-key")
    monkeypatch.setenv("GOOGLE_HMAC_SECRET", "test-secret")
    store = source.CheckpointStore()
    assert store.client.meta.endpoint_url == "https://storage.googleapis.com"
    assert store.client.meta.config.signature_version == "s3v4"
    assert store.client.meta.config.request_checksum_calculation == "when_required"
    args = {
        "Bucket": "trilogy_public_models",
        "Key": source.CheckpointStore.key("ballot"),
    }
    path = tmp_path / "download.sqlite"
    with Stubber(store.client) as stub:
        stub.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params=args,
        )
        assert not store.fetch("ballot", path)
        for code, status in [
            ("AccessDenied", 403),
            ("NoSuchBucket", 404),
            ("InternalError", 500),
        ]:
            stub.add_client_error(
                "get_object",
                service_error_code=code,
                http_status_code=status,
                expected_params=args,
            )
            with pytest.raises(ClientError):
                store.fetch("ballot", path)
        stub.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(b"data"), 4), "ContentLength": 4},
            args,
        )
        assert store.fetch("ballot", path)
        assert path.read_bytes() == b"data"
    requests = []

    def capture(**kwargs):
        requests.append({**kwargs, "Body": kwargs["Body"].read()})

    monkeypatch.setattr(store.client, "put_object", capture)
    store.save("ballot", path)
    assert requests == [
        {
            **args,
            "Body": b"data",
            "ContentLength": 4,
            "ContentMD5": base64.b64encode(hashlib.md5(b"data").digest()).decode(),
            "ContentType": "application/vnd.sqlite3",
            "CacheControl": "no-store",
        }
    ]


def test_remote_deletion_reconciliation_survives_interruption(remote_store):
    # The seed contains ID 1, which disappears upstream. Partial live IDs must
    # survive without prematurely deleting rows before the scan finishes.
    rows = [ballot(2), ballot(3)]
    with pytest.raises(source.IncompleteExtract):
        source.remote_extract("ballot", remote_store, InterruptedAPI(rows, "ids"))
    api = InterruptedAPI(rows, "none")
    result = source.remote_extract("ballot", remote_store, api)
    assert result.column("id").to_pylist() == [2, 3]
    assert any(columns == ["id"] and after == 2 for columns, _, after in api.starts)


def test_checkpoint_download_truncation_and_missing_binding(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace

    monkeypatch.delenv("GOOGLE_HMAC_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_HMAC_SECRET", raising=False)
    with pytest.raises(source.IncompleteExtract, match="HMAC binding"):
        source.CheckpointStore()
    store = source.CheckpointStore.__new__(source.CheckpointStore)
    store.client = SimpleNamespace(
        get_object=lambda **_: {"Body": io.BytesIO(b"short"), "ContentLength": 20}
    )
    with pytest.raises(
        source.IncompleteExtract, match="incomplete checkpoint download"
    ):
        store.fetch("ballot", tmp_path / "truncated.sqlite")
