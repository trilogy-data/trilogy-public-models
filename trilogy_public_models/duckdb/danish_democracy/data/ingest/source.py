# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17"]
# ///
"""Allowlisted Folketinget OData extraction with resumable local backfill.

No output is returned until pagination, ID reconciliation and source stability
checks pass. Public parquet tiles are replaced by Trilogy only after this script
successfully emits a complete Arrow stream. There is no assumed tombstone feed:
the documented Slettet endpoint is not exposed by the live API.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
from collections.abc import Iterator
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
ENTITIES = json.loads(Path(__file__).with_name("entities.json").read_text())
TYPES = {
    "int": pa.int64(),
    "bool": pa.bool_(),
    "string": pa.string(),
    "datetime": pa.timestamp("us"),
}


class IncompleteExtract(RuntimeError):
    """A staged extract must not be published."""


@dataclass(frozen=True)
class Marker:
    count: int
    updated_at: str


class OData:
    def __init__(self, max_pages: int = 1000, delay: float = 0.05):
        self.remaining = max_pages
        self.delay = delay

    def get(self, entity: str, **params: str) -> dict[str, Any]:
        url = BASE + quote(entity, safe="") + "?" + urlencode(params)
        for attempt in range(5):
            if self.remaining <= 0:
                raise IncompleteExtract(
                    "request budget exhausted; resume the same checkpoint, not the published tile"
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
            state.db.commit()
        for page in api.pages(
            endpoint,
            [f[0] for f in spec["fields"]],
            after_id=int(state.get("last_id", "-1")),
        ):
            state.upsert(page)
            state.set("last_id", str(page[-1]["id"]))
            state.db.commit()
        state.set("phase", "incremental")
        state.set("watermark", state.get("scan_started"))
        state.db.commit()
    # Replay from the pre-backfill watermark too: earlier pages may have
    # changed while a resumable full history scan was in progress. Persist the
    # replay ID so a large delta can also progress across bounded invocations.
    replay_key = json.dumps([state.get("watermark"), before.count, before.updated_at])
    if state.get("replay_key") != replay_key:
        state.set("replay_key", replay_key)
        state.set("replay_last", "-1")
        state.db.commit()
    for page in api.pages(
        endpoint,
        [f[0] for f in spec["fields"]],
        since=state.get("watermark"),
        after_id=int(state.get("replay_last", "-1")),
    ):
        state.upsert(page)
        state.set("replay_last", str(page[-1]["id"]))
        state.db.commit()

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
            state.db.commit()
        for page in api.pages(
            endpoint, ["id"], after_id=int(state.get("id_scan_last", "-1"))
        ):
            state.db.executemany(
                "INSERT OR IGNORE INTO live_ids VALUES (?)",
                [(row["id"],) for row in page],
            )
            state.set("id_scan_last", str(page[-1]["id"]))
            state.db.commit()
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
    state.db.commit()
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
            raise IncompleteExtract(
                "published tile is absent; complete the resumable local backfill before enabling Cloud refresh"
            ) from exc
        raise


def emit(entity: str) -> None:
    seed_dir = os.getenv("DANISH_ODA_BACKFILL_DIR")
    if seed_dir:
        table = pq.read_table(
            Path(seed_dir) / (entity + ".parquet"), schema=schema(entity)
        )
    else:
        with tempfile.TemporaryDirectory() as work:
            state = Checkpoint(Path(work) / "checkpoint.sqlite")
            try:
                state.seed(entity, published_table(entity))
                table = extract(
                    entity,
                    state,
                    OData(int(os.getenv("DANISH_ODA_MAX_REQUESTS", "1000"))),
                )
            finally:
                state.close()
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
