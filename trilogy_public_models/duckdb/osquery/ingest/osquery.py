# /// script
# requires-python = ">=3.11"
# dependencies = ["pytrilogy>=0.3.394", "psutil>=6"]
# ///
"""Every osquery source, served by one program (``file `./ingest/osquery.py#name```).

One script means one uv environment and, under the VGI transport, one resident
worker for the whole model rather than one per table.
"""

import autostart  # type: ignore[import-not-found]
import browser_extension  # type: ignore[import-not-found]
import connection  # type: ignore[import-not-found]
import host  # type: ignore[import-not-found]
import interface_address  # type: ignore[import-not-found]
import listening_port  # type: ignore[import-not-found]
import process  # type: ignore[import-not-found]
import user  # type: ignore[import-not-found]
from trilogy.io import run, source

SOURCES = {
    "autostart": source(autostart.autostarts, schema=autostart.SCHEMA),
    "browser_extension": source(
        browser_extension.browser_extensions, schema=browser_extension.SCHEMA
    ),
    "connection": source(connection.connections, schema=connection.SCHEMA),
    "host": source(host.host, schema=host.SCHEMA),
    "interface_address": source(
        interface_address.interface_addresses, schema=interface_address.SCHEMA
    ),
    "listening_port": source(listening_port.listening_ports, schema=listening_port.SCHEMA),
    "process": source(process.processes, schema=process.SCHEMA),
    "user": source(user.users, schema=user.SCHEMA),
}

if __name__ == "__main__":
    raise SystemExit(run(SOURCES))
