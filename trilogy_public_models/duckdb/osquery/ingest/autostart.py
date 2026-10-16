import sys

import pyarrow as pa
from osq import query  # type: ignore[import-not-found]

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

# Unprivileged, startup_items misses per-user Run keys, so on windows the Run
# and RunOnce keys are read from the registry directly. Task Manager records a
# disabled entry under Explorer\StartupApproved: an odd first byte is disabled.
WINDOWS_STARTUP = r"""
with approved as materialized (
    select key, name,
        substr(data, 2, 1) in ('1', '3', '5', '7', '9', 'b', 'd', 'f', 'B', 'D', 'F')
            as disabled
    from registry
    where key like 'HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\%'
        or key like 'HKEY_USERS\S-1-5-21-%\Software\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\%'
),
run_keys as materialized (
    select key, name, data
    from registry
    where key in (
            'HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\Run',
            'HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce',
            'HKEY_LOCAL_MACHINE\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Run'
        )
        or key like 'HKEY_USERS\S-1-5-21-%\Software\Microsoft\Windows\CurrentVersion\Run'
        or key like 'HKEY_USERS\S-1-5-21-%\Software\Microsoft\Windows\CurrentVersion\RunOnce'
),
user_names as materialized (
    select uuid, min(username) as username from users group by uuid
)
select distinct 'registry/' || r.key || '/' || r.name as autostart_id,
    'startup_item' as kind, 'sign_in' as trigger, r.name, r.data as path,
    r.key as source,
    case when a.disabled then 'disabled' else 'enabled' end as status,
    case when r.key like 'HKEY_USERS\%' then u.username else 'all users' end
        as username
from run_keys r
left join approved a
    on a.name = r.name
    and a.key = replace(
        replace(r.key, 'WOW6432Node\', ''),
        'CurrentVersion\Run', 'CurrentVersion\Explorer\StartupApproved\Run'
    ) || case when r.key like '%WOW6432Node%' then '32' else '' end
left join user_names u on r.key like 'HKEY_USERS\' || u.uuid || '\%'
where r.data != ''
union all
select distinct 'startup_item/' || s.source || '/' || s.name as autostart_id,
    'startup_item' as kind, 'sign_in' as trigger, s.name, s.path, s.source,
    case when max(coalesce(a.disabled, 0)) = 1 then 'disabled' else 'enabled' end
        as status,
    s.username
from startup_items s
left join approved a on a.key like '%\StartupFolder' and a.name = s.name
where s.name != 'desktop.ini' and s.source not like 'HKEY%'
group by s.source, s.name, s.path, s.username
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
    if sys.platform == "win32":
        return query(f"{WINDOWS_STARTUP} union all {SERVICES}", SCHEMA)
    return query(STARTUP_ITEMS, SCHEMA)

