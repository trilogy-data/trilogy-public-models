# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy>=0.3.391"]
# ///
import sys

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
FAMILY = """case when family = 2 then 'ipv4' when family in (10, 23, 30) then 'ipv6'
        else family end"""

SQL = f"""
select distinct pid || '/' || protocol || '/' || address || '/' || port as listener_id,
    nullif(pid, -1) as pid, port,
    case protocol when 6 then 'tcp' when 17 then 'udp' else protocol end as protocol,
    {FAMILY} as family, address
from listening_ports
where port > 0
"""

# On windows, listening_ports also returns TCP sockets bound for outbound
# connections, so TCP listeners come from sockets in the LISTEN state instead.
WINDOWS_SQL = f"""
select distinct pid || '/6/' || local_address || '/' || local_port as listener_id,
    nullif(pid, -1) as pid, local_port as port, 'tcp' as protocol,
    {FAMILY} as family, local_address as address
from process_open_sockets
where protocol = 6 and state = 'LISTEN' and local_port > 0
union
select distinct pid || '/17/' || address || '/' || port as listener_id,
    nullif(pid, -1) as pid, port, 'udp' as protocol, {FAMILY} as family, address
from listening_ports
where protocol = 17 and port > 0
"""


def listening_ports() -> pa.Table:
    return query(WINDOWS_SQL if sys.platform == "win32" else SQL, SCHEMA)


if __name__ == "__main__":
    raise SystemExit(run(listening_ports, schema=SCHEMA))
