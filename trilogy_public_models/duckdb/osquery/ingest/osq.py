"""Shared helper: run one osquery SQL statement through `osqueryi --json`.

osqueryi prints every value as a string; the declared arrow schema is what
types them. An empty string is osquery's "not available" and becomes null.
"""

import json
import os
import shutil
import subprocess
import sys

import pyarrow as pa

_DEFAULT_PATHS = {
    "win32": [r"C:\Program Files\osquery\osqueryi.exe"],
    "darwin": [
        "/usr/local/bin/osqueryi",
        "/opt/osquery/lib/osquery.app/Contents/MacOS/osqueryd",
    ],
    "linux": ["/usr/bin/osqueryi", "/opt/osquery/bin/osqueryi"],
}


def osqueryi() -> str:
    override = os.environ.get("OSQUERYI")
    if override:
        return override
    found = shutil.which("osqueryi")
    if found:
        return found
    for candidate in _DEFAULT_PATHS.get(sys.platform, []):
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError(
        "osqueryi not found: install osquery (https://osquery.io/downloads) "
        "or set OSQUERYI to the binary's path"
    )


def query(sql: str, schema: pa.Schema) -> pa.Table:
    binary = osqueryi()
    # The macOS app bundle ships osqueryd; it behaves as osqueryi when told to.
    args = [binary, "-S"] if binary.endswith("osqueryd") else [binary]
    out = subprocess.run(
        [*args, "--json", sql], capture_output=True, text=True, check=True
    ).stdout
    rows = [{k: v if v != "" else None for k, v in r.items()} for r in json.loads(out)]
    raw = pa.schema([pa.field(f.name, pa.string()) for f in schema])
    return pa.Table.from_pylist(rows, schema=raw).cast(schema)
