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
        ("autostart_id", pa.string()),
        ("kind", pa.string()),
        ("trigger", pa.string()),
        ("name", pa.string()),
        ("path", pa.string()),
        ("source", pa.string()),
        ("status", pa.string()),
        ("username", pa.string()),
    ]
)

# Run keys, Startup folders, login items and autostart entries: start at sign-in.
STARTUP_ITEMS = """
select distinct 'startup_item/' || source || '/' || name as autostart_id,
    'startup_item' as kind, 'sign_in' as trigger, name, path, source, status,
    username
from startup_items
where name != 'desktop.ini'
"""

# Automatic Windows services (drivers excluded): start at boot.
SERVICES = """
select 'service/' || name as autostart_id, 'service' as kind, 'boot' as trigger,
    coalesce(nullif(display_name, ''), name) as name, path, name as source,
    status, user_account as username
from services
where start_type = 'AUTO_START' and service_type not like '%DRIVER%'
"""


def autostarts() -> pa.Table:
    sql = STARTUP_ITEMS
    if sys.platform == "win32":
        sql = f"{STARTUP_ITEMS} union all {SERVICES}"
    return query(sql, SCHEMA)


if __name__ == "__main__":
    raise SystemExit(run(autostarts, schema=SCHEMA))
