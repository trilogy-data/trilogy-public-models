import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

SCHEMA = pa.schema(
    [
        ("extension_id", pa.string()),
        ("browser", pa.string()),
        ("name", pa.string()),
        ("identifier", pa.string()),
        ("version", pa.string()),
        ("profile", pa.string()),
        ("permissions", pa.string()),
        ("permission_count", pa.int64()),
    ]
)

# Chromium browsers (chrome, edge, brave, ...) report permissions as a
# comma-separated list; firefox_addons has no permissions column. Both tables
# are per-user, so they are joined to users and deduplicated.
SQL = """
select distinct browser_type || '/' || path as extension_id,
    browser_type as browser, name, identifier, version, profile, permissions,
    case when permissions = '' then 0
        else length(permissions) - length(replace(permissions, ',', '')) + 1
    end as permission_count
from users join chrome_extensions using (uid)
union
select distinct 'firefox/' || identifier || '/' || coalesce(path, '') as extension_id,
    'firefox' as browser, name,
    identifier, version, null as profile, null as permissions,
    null as permission_count
from users join firefox_addons using (uid)
"""


def browser_extensions() -> pa.Table:
    return query(SQL, SCHEMA)

