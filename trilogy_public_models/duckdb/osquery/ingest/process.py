import datetime
import sys

import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

OSQUERY_SCHEMA = pa.schema(
    [
        ("pid", pa.int64()),
        ("parent_pid", pa.int64()),
        ("name", pa.string()),
        ("path", pa.string()),
        ("cmdline", pa.string()),
        ("uid", pa.int64()),
        ("state", pa.string()),
        ("threads", pa.int64()),
        ("resident_bytes", pa.int64()),
        ("virtual_bytes", pa.int64()),
        ("user_cpu_ms", pa.int64()),
        ("system_cpu_ms", pa.int64()),
        ("disk_bytes_read", pa.int64()),
        ("disk_bytes_written", pa.int64()),
        ("started_at", pa.timestamp("s")),
    ]
)
SCHEMA = OSQUERY_SCHEMA.append(pa.field("app_parent_pid", pa.int64())).append(
    pa.field("age_seconds", pa.int64())
)

# osquery reports -1 for a value it could not read (e.g. another user's process).
SQL = """
select pid, nullif(parent, -1) as parent_pid, name, path, cmdline,
    nullif(uid, -1) as uid, state, nullif(threads, -1) as threads,
    nullif(resident_size, -1) as resident_bytes,
    nullif(total_size, -1) as virtual_bytes,
    nullif(user_time, -1) as user_cpu_ms,
    nullif(system_time, -1) as system_cpu_ms,
    nullif(disk_bytes_read, -1) as disk_bytes_read,
    nullif(disk_bytes_written, -1) as disk_bytes_written,
    case when start_time > 0 then datetime(start_time, 'unixepoch') end as started_at
from processes
"""

# Processes that start other programs on the user's or the system's behalf. An
# application's process tree stops below them rather than rolling up into them.
LAUNCHERS = {
    "[system process]",
    "system",
    "smss.exe",
    "csrss.exe",
    "wininit.exe",
    "winlogon.exe",
    "services.exe",
    "svchost.exe",
    "userinit.exe",
    "explorer.exe",
    "sihost.exe",
    "launchd",
    "systemd",
    "init",
    "kthreadd",
}


def windows_counters(pids: list[int]) -> dict[int, dict]:
    """Counters for every process, read without the handle osquery needs.

    Unprivileged, osquery cannot open protected processes (Defender, csrss,
    ...) and reports their counters as unknown; psutil reads them from the
    system process list instead.
    """
    import psutil

    counters: dict[int, dict] = {}
    for pid in pids:
        # pid 0 is the System Idle Process: its CPU time is idle time.
        if pid == 0:
            continue
        try:
            proc = psutil.Process(pid)
            with proc.oneshot():
                cpu = proc.cpu_times()
                io = proc.io_counters()
                created = int(proc.create_time())
                counters[pid] = {
                    "threads": proc.num_threads(),
                    "resident_bytes": proc.memory_info().rss,
                    "user_cpu_ms": int(cpu.user * 1000),
                    "system_cpu_ms": int(cpu.system * 1000),
                    "disk_bytes_read": io.read_bytes,
                    "disk_bytes_written": io.write_bytes,
                    # The kernel's pseudo-processes report a creation time of 0.
                    "started_at": datetime.datetime.fromtimestamp(
                        created, datetime.UTC
                    ).replace(tzinfo=None)
                    if created > 0
                    else None,
                }
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return counters


def app_parent(row: dict, by_pid: dict[int, dict]) -> int | None:
    """The parent within the same application, or None at the top of its tree."""
    parent = by_pid.get(row["parent_pid"])
    if parent is None or parent["pid"] == row["pid"]:
        return None
    if (parent["name"] or "").lower() in LAUNCHERS:
        return None
    # A parent that started after its child is a reused pid, not the real parent.
    if (
        parent["started_at"]
        and row["started_at"]
        and parent["started_at"] > row["started_at"]
    ):
        return None
    return parent["pid"]


def processes() -> pa.Table:
    rows = query(SQL, OSQUERY_SCHEMA).to_pylist()
    if sys.platform == "win32":
        # Only the processes osquery could not open; psutil is slow per process.
        unread = [row["pid"] for row in rows if row["resident_bytes"] is None]
        counters = windows_counters(unread)
        for row in rows:
            for key, value in counters.get(row["pid"], {}).items():
                if row[key] is None:
                    row[key] = value
    by_pid = {row["pid"]: row for row in rows}
    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    for row in rows:
        row["app_parent_pid"] = app_parent(row, by_pid)
        started = row["started_at"]
        row["age_seconds"] = int((now - started).total_seconds()) if started else None
    return pa.Table.from_pylist(rows, schema=SCHEMA)

