import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

SCHEMA = pa.schema(
    [
        ("connection_id", pa.string()),
        ("pid", pa.int64()),
        ("protocol", pa.string()),
        ("family", pa.string()),
        ("local_address", pa.string()),
        ("local_port", pa.int64()),
        ("remote_address", pa.string()),
        ("remote_port", pa.int64()),
        ("state", pa.string()),
    ]
)

# Sockets with a remote end: established, closing and TIME_WAIT connections.
SQL = """
select distinct pid || '/' || protocol || '/' || local_address || ':' || local_port
        || '>' || remote_address || ':' || remote_port as connection_id,
    nullif(pid, -1) as pid,
    case protocol when 6 then 'tcp' when 17 then 'udp' else protocol end as protocol,
    case when family = 2 then 'ipv4' when family in (10, 23, 30) then 'ipv6'
        else family end as family,
    local_address, local_port, remote_address, remote_port, state
from process_open_sockets
where remote_port > 0
"""


def connections() -> pa.Table:
    return query(SQL, SCHEMA)

