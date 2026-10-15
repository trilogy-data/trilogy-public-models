# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy>=0.3.391"]
# ///
import pyarrow as pa
from osq import query  # type: ignore[import-not-found]
from trilogy.io import run

SCHEMA = pa.schema(
    [
        ("listener_id", pa.string()),
        ("pid", pa.int64()),
        ("port", pa.int64()),
        ("protocol", pa.string()),
        ("family", pa.string()),
        ("address", pa.string()),
    ]
)

# Family numbers differ by OS: AF_INET6 is 10 on linux, 23 on windows, 30 on macos.
SQL = """
select distinct pid || '/' || protocol || '/' || address || '/' || port as listener_id,
    nullif(pid, -1) as pid, port,
    case protocol when 6 then 'tcp' when 17 then 'udp' else protocol end as protocol,
    case when family = 2 then 'ipv4' when family in (10, 23, 30) then 'ipv6'
        else family end as family,
    address
from listening_ports
where port > 0
"""


def listening_ports() -> pa.Table:
    return query(SQL, SCHEMA)


if __name__ == "__main__":
    raise SystemExit(run(listening_ports, schema=SCHEMA))
