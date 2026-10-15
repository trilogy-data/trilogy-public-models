# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy>=0.3.391"]
# ///
import pyarrow as pa
from osq import query  # type: ignore[import-not-found]
from trilogy.io import run

SCHEMA = pa.schema(
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


def processes() -> pa.Table:
    return query(SQL, SCHEMA)


if __name__ == "__main__":
    raise SystemExit(run(processes, schema=SCHEMA))
