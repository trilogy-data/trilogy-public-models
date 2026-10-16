import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

SCHEMA = pa.schema(
    [
        ("interface_address_id", pa.string()),
        ("interface", pa.string()),
        ("friendly_name", pa.string()),
        ("address", pa.string()),
        ("mask", pa.string()),
        ("family", pa.string()),
        ("type", pa.string()),
    ]
)

SQL = """
select distinct interface || '/' || address as interface_address_id,
    interface, friendly_name, address, mask,
    case when address like '%:%' then 'ipv6' else 'ipv4' end as family, type
from interface_addresses
"""


def interface_addresses() -> pa.Table:
    return query(SQL, SCHEMA)

