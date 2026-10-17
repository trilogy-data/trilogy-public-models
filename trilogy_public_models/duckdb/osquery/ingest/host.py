import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

SCHEMA = pa.schema(
    [
        ("hostname", pa.string()),
        ("computer_name", pa.string()),
        ("cpu_brand", pa.string()),
        ("cpu_physical_cores", pa.int64()),
        ("cpu_logical_cores", pa.int64()),
        ("physical_memory", pa.int64()),
        ("hardware_vendor", pa.string()),
        ("hardware_model", pa.string()),
        ("os_name", pa.string()),
        ("os_version", pa.string()),
        ("os_platform", pa.string()),
        ("os_arch", pa.string()),
        ("uptime_seconds", pa.int64()),
        ("booted_at", pa.timestamp("s")),
        ("observed_at", pa.timestamp("s")),
    ]
)

SQL = """
select s.hostname, s.computer_name, s.cpu_brand, s.cpu_physical_cores,
    s.cpu_logical_cores, s.physical_memory, s.hardware_vendor, s.hardware_model,
    o.name as os_name, o.version as os_version, o.platform as os_platform,
    o.arch as os_arch, u.total_seconds as uptime_seconds,
    datetime(strftime('%s', 'now') - u.total_seconds, 'unixepoch') as booted_at,
    datetime('now') as observed_at
from system_info s, os_version o, uptime u
"""


def host() -> pa.Table:
    return query(SQL, SCHEMA)

