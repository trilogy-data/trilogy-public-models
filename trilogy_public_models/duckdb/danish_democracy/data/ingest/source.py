# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17", "boto3>=1.36"]
# ///
"""Allowlisted Folketinget OData extraction with resumable local backfill.

No output is returned until pagination, ID reconciliation and source stability
checks pass. Public parquet tiles are replaced by Trilogy only after this script
successfully emits a complete Arrow stream. There is no assumed tombstone feed:
the documented Slettet endpoint is not exposed by the live API.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

BASE = "https://oda.ft.dk/api/"
PUBLIC = "https://storage.googleapis.com/trilogy_public_models/duckdb/danish_democracy/"
ENTITIES = json.loads(
    Path(__file__).with_name("entities.json").read_text(encoding="utf-8")
)
TYPES = {
    "int": pa.int64(),
    "bool": pa.bool_(),
    "string": pa.string(),
    "datetime": pa.timestamp("us"),
}


class IncompleteExtract(RuntimeError):
    """A staged extract must not be published."""


class TileAbsent(IncompleteExtract):
    """No published tile exists yet for this entity."""


@dataclass(frozen=True)
class Marker:
    count: int
    updated_at: str


class OData:
    def __init__(
        self,
        max_pages: int = 1000,
        delay: float = 0.05,
        max_seconds: float | None = None,
    ):
        self.remaining = max_pages
        self.delay = delay
        # A wall-clock budget lets a bounded Cloud run stop cleanly, saving its
        # checkpoint, before the job timeout kills it.
        self.deadline = None if max_seconds is None else time.monotonic() + max_seconds

    def get(self, entity: str, **params: str) -> dict[str, Any]:
        url = BASE + quote(entity, safe="") + "?" + urlencode(params)
        for attempt in range(5):
            if self.remaining <= 0:
                raise IncompleteExtract(
                    "request budget exhausted; resume the same checkpoint, not the published tile"
                )
            if self.deadline is not None and time.monotonic() >= self.deadline:
                raise IncompleteExtract(
                    "time budget exhausted; the next run resumes this checkpoint"
                )
            self.remaining -= 1
            try:
                request = Request(
                    url,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "trilogy-public-models/danish-democracy",
                    },
                )
                with urlopen(request, timeout=60) as response:
                    body = json.load(response)
                if not isinstance(body.get("value"), list):
                    raise IncompleteExtract(
                        "invalid OData response: expected value array"
                    )
                time.sleep(self.delay)
                return body
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                retry_after = exc.headers.get("Retry-After", "")
                wait = (
                    min(float(retry_after), 60) if retry_after.isdigit() else 2**attempt
                )
                time.sleep(wait)
            except (URLError, TimeoutError):
                if attempt == 4:
                    raise
                time.sleep(2**attempt)
        raise IncompleteExtract("request retries exhausted")

    def marker(self, endpoint: str) -> Marker:
        body = self.get(
            endpoint,
            **{
                "$select": "id,opdateringsdato",
                "$orderby": "opdateringsdato desc,id desc",
                "$top": "1",
                "$inlinecount": "allpages",
            },
        )
        if "odata.count" not in body:
            raise IncompleteExtract("upstream did not return an authoritative count")
        count = int(body["odata.count"])
        return Marker(count, body["value"][0]["opdateringsdato"] if count else "")

    def pages(
        self, endpoint: str, columns: list[str], since: str = "", after_id: int = -1
    ) -> Iterator[list[dict[str, Any]]]:
        # Keyset pagination avoids $skip shifting under concurrent deletions.
        # Never set $top globally: OData nextLink decrements it across pages.
        while True:
            clauses = [f"id gt {after_id}"]
            if since:
                clauses.append(f"opdateringsdato ge datetime'{since}'")
            body = self.get(
                endpoint,
                **{
                    "$select": ",".join(columns),
                    "$orderby": "id",
                    "$filter": " and ".join(clauses),
                },
            )
            rows = body["value"]
            if not rows:
                return
            ids = [row["id"] for row in rows]
            if (
                any(not isinstance(i, int) for i in ids)
                or ids != sorted(set(ids))
                or ids[0] <= after_id
            ):
                raise IncompleteExtract("non-monotonic or duplicate IDs in OData page")
            yield rows
            after_id = ids[-1]
            if not body.get("odata.nextLink"):
                return


def schema(entity: str) -> pa.Schema:
    spec = ENTITIES[entity]
    fields = [
        pa.field(name, TYPES[kind], nullable=nullable)
        for _, name, kind, nullable in spec["fields"]
    ]
    fields += [
        pa.field("updated_at", pa.timestamp("us"), nullable=False),
        pa.field("ids_reconciled_at", pa.timestamp("us")),
    ]
    fields += [pa.field(name + "_name", pa.string()) for name in spec["lookups"]]
    return pa.schema(fields)


def _datetime(value: str | None) -> datetime | None:
    # Keep the unmodified seven-fraction-digit source string in source_updated_at
    # for incremental cursors; Arrow timestamps are only microsecond precision.
    return datetime.fromisoformat(value.rstrip("Z")) if value else None


def normalize(
    entity: str, row: dict[str, Any], labels: dict[str, dict[int, str]]
) -> dict[str, Any]:
    spec = ENTITIES[entity]
    result = {}
    for upstream, name, kind, nullable in spec["fields"]:
        value = row.get(upstream)
        if value is None and not nullable:
            raise IncompleteExtract(
                f"{spec['endpoint']} {row.get('id')}: missing {upstream}"
            )
        result[name] = _datetime(value) if kind == "datetime" else value
    result["updated_at"] = _datetime(row["opdateringsdato"])
    result["ids_reconciled_at"] = None
    for name, (_, fk, _) in spec["lookups"].items():
        result[name + "_name"] = (
            labels[name].get(row[fk]) if row.get(fk) is not None else None
        )
    return result


class Checkpoint:
    def __init__(self, path: Path):
        self.on_commit: Callable[[], None] | None = None
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )

    def get(self, key: str, default: str = "") -> str:
        found = self.db.execute(
            "SELECT value FROM state WHERE key=?", (key,)
        ).fetchone()
        return found[0] if found else default

    def set(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))

    def upsert(self, rows: list[dict[str, Any]]) -> None:
        self.db.executemany(
            "INSERT OR REPLACE INTO rows VALUES (?, ?)",
            [(r["id"], json.dumps(r, ensure_ascii=False)) for r in rows],
        )

    def seed(self, entity: str, table: pa.Table) -> None:
        spec = ENTITIES[entity]
        # Only allowlisted physical fields are retained; no upstream biography,
        # document bodies, emails, birthdays, photographs or current-party XML.
        for batch in table.to_batches(max_chunksize=10000):
            rows = [
                {
                    upstream: item[name].isoformat()
                    if isinstance(item[name], datetime)
                    else item[name]
                    for upstream, name, _, _ in spec["fields"]
                }
                for item in batch.to_pylist()
            ]
            self.upsert(rows)
        watermark = max(table.column("source_updated_at").to_pylist(), default="")
        self.set("watermark", watermark)
        if "ids_reconciled_at" in table.column_names:
            reconciled = [
                value
                for value in table.column("ids_reconciled_at").to_pylist()
                if value is not None
            ]
            if reconciled:
                self.set("ids_reconciled_at", min(reconciled).isoformat())
        self.set("phase", "incremental")
        self.db.commit()

    def commit(self) -> None:
        self.db.commit()
        if self.on_commit is not None:
            self.on_commit()

    def close(self) -> None:
        self.db.close()


def extract(
    entity: str, state: Checkpoint, api: OData, reconciliation_days: int = 7
) -> pa.Table:
    spec = ENTITIES[entity]
    endpoint = spec["endpoint"]
    before = api.marker(endpoint)
    phase = state.get("phase", "full")
    if phase == "full":
        # Record the pre-scan high-water mark once. If a lengthy initial scan
        # changes underneath us, the next attempt replays from this point.
        if not state.get("scan_started"):
            state.set("scan_started", before.updated_at)
            state.commit()
        for page in api.pages(
            endpoint,
            [f[0] for f in spec["fields"]],
            after_id=int(state.get("last_id", "-1")),
        ):
            state.upsert(page)
            state.set("last_id", str(page[-1]["id"]))
            state.commit()
        state.set("phase", "incremental")
        state.set("watermark", state.get("scan_started"))
        state.commit()
    # Replay from the pre-backfill watermark too: earlier pages may have
    # changed while a resumable full history scan was in progress. Persist the
    # replay ID so a large delta can also progress across bounded invocations.
    replay_key = json.dumps([state.get("watermark"), before.count, before.updated_at])
    if state.get("replay_key") != replay_key:
        state.set("replay_key", replay_key)
        state.set("replay_last", "-1")
        state.commit()
    for page in api.pages(
        endpoint,
        [f[0] for f in spec["fields"]],
        since=state.get("watermark"),
        after_id=int(state.get("replay_last", "-1")),
    ):
        state.upsert(page)
        state.set("replay_last", str(page[-1]["id"]))
        state.commit()

    # Fast daily merges detect ordinary deletions via the post-upsert count.
    # An unconditional ID scan at least weekly catches coincident changes that
    # aggregate checks cannot prove absent. This is eventual reconciliation,
    # not a transactional snapshot guarantee from the OData service.
    now = datetime.now(UTC).replace(tzinfo=None)
    last_reconciled = _datetime(state.get("ids_reconciled_at"))
    local_count = state.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0]
    reconcile = (
        last_reconciled is None
        or now - last_reconciled >= timedelta(days=reconciliation_days)
        or local_count != before.count
    )
    if reconcile:
        marker_key = json.dumps([before.count, before.updated_at])
        state.db.execute("CREATE TABLE IF NOT EXISTS live_ids (id INTEGER PRIMARY KEY)")
        if state.get("id_scan_marker") != marker_key:
            state.db.execute("DELETE FROM live_ids")
            state.set("id_scan_marker", marker_key)
            state.set("id_scan_last", "-1")
            state.commit()
        for page in api.pages(
            endpoint, ["id"], after_id=int(state.get("id_scan_last", "-1"))
        ):
            state.db.executemany(
                "INSERT OR IGNORE INTO live_ids VALUES (?)",
                [(row["id"],) for row in page],
            )
            state.set("id_scan_last", str(page[-1]["id"]))
            state.commit()
        missing = state.db.execute(
            "SELECT COUNT(*) FROM live_ids LEFT JOIN rows USING(id) WHERE rows.id IS NULL"
        ).fetchone()[0]
        live_count = state.db.execute("SELECT COUNT(*) FROM live_ids").fetchone()[0]
    else:
        missing, live_count = 0, local_count
    after = api.marker(endpoint)
    if before != after or live_count != after.count or missing:
        raise IncompleteExtract(
            "source changed during extraction or IDs are missing; retry without publishing"
        )
    if reconcile:
        state.db.execute("DELETE FROM rows WHERE id NOT IN (SELECT id FROM live_ids)")
    reconciled_at = now if reconcile else last_reconciled

    labels = {}
    for name, (lookup, _, label) in spec["lookups"].items():
        labels[name] = {
            r["id"]: r[label] for page in api.pages(lookup, ["id", label]) for r in page
        }
    batches = []
    cursor = state.db.execute("SELECT payload FROM rows ORDER BY id")
    output_schema = schema(entity)
    while records := cursor.fetchmany(10000):
        batch = [
            normalize(entity, json.loads(payload), labels) for (payload,) in records
        ]
        for row in batch:
            row["ids_reconciled_at"] = reconciled_at
        batches.append(pa.RecordBatch.from_pylist(batch, schema=output_schema))
    result = pa.Table.from_batches(batches, schema=output_schema)
    state.set("watermark", after.updated_at)
    if reconciled_at is not None:
        state.set("ids_reconciled_at", reconciled_at.isoformat())
    state.set("id_scan_marker", "")
    state.set("replay_key", "")
    state.commit()
    return result


def published_table(entity: str) -> pa.Table:
    request = Request(
        PUBLIC + entity + ".parquet", headers={"Cache-Control": "no-cache"}
    )
    try:
        with urlopen(request, timeout=120) as response:
            return pq.read_table(pa.BufferReader(response.read()))
    except HTTPError as exc:
        if exc.code == 404:
            raise TileAbsent("published tile is absent") from exc
        raise


CHECKPOINT_BUCKET = "trilogy_public_models"
CHECKPOINT_PREFIX = "duckdb/danish_democracy/checkpoints/v1"


class CheckpointStore:
    """GCS XML API using the existing HMAC binding; no default credential lookup."""

    def __init__(self) -> None:
        import boto3
        from botocore.config import Config

        key, secret = os.getenv("GOOGLE_HMAC_KEY"), os.getenv("GOOGLE_HMAC_SECRET")
        if not key or not secret:
            raise IncompleteExtract(
                "checkpoint storage requires the existing HMAC binding"
            )
        self.client = boto3.client(
            "s3",
            endpoint_url="https://storage.googleapis.com",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                connect_timeout=30,
                read_timeout=120,
                retries={"mode": "standard", "total_max_attempts": 3},
            ),
        )

    @staticmethod
    def key(entity: str) -> str:
        if entity not in ENTITIES:
            raise ValueError(f"unknown entity: {entity}")
        return f"{CHECKPOINT_PREFIX}/{entity}.sqlite"

    def fetch(self, entity: str, path: Path) -> bool:
        from botocore.exceptions import ClientError

        try:
            response = self.client.get_object(
                Bucket=CHECKPOINT_BUCKET, Key=self.key(entity)
            )
        except ClientError as exc:
            # A missing object permits first-run initialization. Access failures,
            # missing buckets and transport errors must never reset a cursor.
            if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
                return False
            raise
        body = response["Body"]
        try:
            with path.open("wb") as output:
                shutil.copyfileobj(body, output)
        finally:
            body.close()
        if path.stat().st_size != response["ContentLength"]:
            raise IncompleteExtract("incomplete checkpoint download")
        return True

    def save(self, entity: str, path: Path) -> None:
        # One PUT replaces one object atomically; MD5 verifies transfer integrity.
        # No multipart transfer or background uploader can outlive this call.
        with path.open("rb") as data:
            checksum = base64.b64encode(
                hashlib.file_digest(data, "md5").digest()
            ).decode()
            data.seek(0)
            self.client.put_object(
                Bucket=CHECKPOINT_BUCKET,
                Key=self.key(entity),
                Body=data,
                ContentLength=path.stat().st_size,
                ContentMD5=checksum,
                ContentType="application/vnd.sqlite3",
                CacheControl="no-store",
            )


def validate_checkpoint(path: Path, entity: str) -> None:
    """Reject incomplete/wrong-entity snapshots before creating or changing tables."""
    try:
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise IncompleteExtract("checkpoint integrity check failed")
            metadata = dict(db.execute("SELECT key, value FROM state"))
            db.execute("SELECT id, payload FROM rows LIMIT 0")
            if (
                metadata.get("checkpoint_format") != "1"
                or metadata.get("entity") != entity
            ):
                raise IncompleteExtract("checkpoint format/entity mismatch")
            if metadata.get("phase") not in {"full", "incremental"}:
                raise IncompleteExtract("checkpoint phase is missing or invalid")
            if metadata.get("phase") == "incremental" and "watermark" not in metadata:
                raise IncompleteExtract(
                    "incremental checkpoint is missing its watermark"
                )
            for field in ("last_id", "replay_last", "id_scan_last"):
                if field in metadata and int(metadata[field]) < -1:
                    raise IncompleteExtract(f"invalid checkpoint cursor: {field}")
            if metadata.get("id_scan_marker"):
                db.execute("SELECT id FROM live_ids LIMIT 0")
        finally:
            db.close()
    except (sqlite3.Error, ValueError) as exc:
        raise IncompleteExtract(
            "invalid SQLite checkpoint; refusing to reset progress"
        ) from exc


def remote_extract(entity: str, store: CheckpointStore, api: OData) -> pa.Table:
    """Single-writer download/update/snapshot cycle, separate from public tiles.

    A hard kill can lose work since the last upload, which is replayed next run.
    A failed upload prevents Arrow output; the previous remote snapshot is retained.
    """
    interval = int(os.getenv("DANISH_ODA_CHECKPOINT_SECONDS", "300"))
    if interval <= 0:
        raise ValueError("DANISH_ODA_CHECKPOINT_SECONDS must be positive")
    with tempfile.TemporaryDirectory() as work:
        local = Path(work) / "checkpoint.sqlite"
        exists = store.fetch(entity, local)
        if exists:
            validate_checkpoint(local, entity)
        state = Checkpoint(local)
        last_saved = time.monotonic()

        def persist() -> None:
            nonlocal last_saved
            # Discard only an unfinished transaction; rows and paging state are
            # committed together. Backup also includes committed WAL pages.
            state.db.rollback()
            snapshot = Path(work) / "snapshot.sqlite"
            snapshot.unlink(missing_ok=True)
            backup = sqlite3.connect(snapshot)
            try:
                state.db.backup(backup)
            finally:
                backup.close()
            validate_checkpoint(snapshot, entity)
            store.save(entity, snapshot)
            last_saved = time.monotonic()

        def periodic_snapshot() -> None:
            if time.monotonic() - last_saved >= interval:
                persist()

        try:
            if not exists:
                # Missing state starts from the complete published seed. With no
                # tile either, this is the first bootstrap: an empty checkpoint
                # in the full phase, which pages the whole history from id 0
                # across as many bounded runs as it takes. Nothing is emitted
                # until that scan and its ID reconciliation complete.
                try:
                    state.seed(entity, published_table(entity))
                except TileAbsent:
                    state.set("phase", "full")
                state.set("checkpoint_format", "1")
                state.set("entity", entity)
                state.db.commit()
                persist()
            state.on_commit = periodic_snapshot
            try:
                return extract(entity, state, api)
            finally:
                # Also save committed partial pages, live_ids and their cursor
                # on budget exhaustion, source drift or a catchable interruption.
                persist()
        finally:
            state.close()


def emit(entity: str) -> None:
    seed_dir = os.getenv("DANISH_ODA_BACKFILL_DIR")
    if seed_dir:
        table = pq.read_table(
            Path(seed_dir) / (entity + ".parquet"), schema=schema(entity)
        )
    else:
        table = remote_extract(
            entity,
            CheckpointStore(),
            OData(
                int(os.getenv("DANISH_ODA_MAX_REQUESTS", "100000")),
                max_seconds=float(os.getenv("DANISH_ODA_MAX_SECONDS", "7200")),
            ),
        )
    with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("entity", choices=ENTITIES)
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-requests", type=int, default=1000)
    args = parser.parse_args()
    state = Checkpoint(args.checkpoint_dir / (args.entity + ".sqlite"))
    try:
        table = extract(args.entity, state, OData(args.max_requests))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(".parquet.tmp")
        pq.write_table(table, temporary, compression="zstd")
        temporary.replace(args.output)
        print(
            f"{args.entity}: {table.num_rows} reconciled rows -> {args.output}",
            file=sys.stderr,
        )
        return 0
    except IncompleteExtract as exc:
        print(f"NOT PUBLISHED: {exc}", file=sys.stderr)
        return 3
    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
