# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy>=0.3.391"]
# ///
import pyarrow as pa
from osq import query  # type: ignore[import-not-found]
from trilogy.io import run

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


if __name__ == "__main__":
    raise SystemExit(run(users, schema=SCHEMA))
