import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

SCHEMA = pa.schema(
    [
        ("uid", pa.int64()),
        ("username", pa.string()),
        ("description", pa.string()),
        ("directory", pa.string()),
        ("shell", pa.string()),
        ("type", pa.string()),
    ]
)

# Windows can list one account under several types; keep one row per uid.
SQL = """
select uid, username, description, directory, shell, type
from users group by uid
"""


def users() -> pa.Table:
    return query(SQL, SCHEMA)

