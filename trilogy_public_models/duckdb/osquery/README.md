# osquery

A live model of the machine you run it on, read through [osquery](https://osquery.io).
Every datasource is a `uv` python script that runs one `osqueryi --json` query and
streams the result to DuckDB, so each Trilogy query sees the machine as it is now.

| File | Grain | Contents |
|---|---|---|
| `host.preql` | hostname | Hardware, OS, uptime and boot time (one row) |
| `process.preql` | pid | Running processes: path, command line, memory, CPU time, owner |
| `process_tree.preql` | pid | Each process with its `parent` and its `app` (the top of its launch tree, via `recurse_edge`); per-application totals |
| `user.preql` | uid | Local and system accounts |
| `listening_port.preql` | pid/protocol/address/port | Listening TCP/UDP ports and the owning process |
| `connection.preql` | pid/protocol/local/remote | Sockets with a remote end (established, closing, TIME_WAIT) |
| `interface_address.preql` | interface/address | IP addresses on each network interface |
| `browser_extension.preql` | browser/install path | Browser extensions and add-ons, with permission counts for Chromium browsers |
| `autostart.preql` | kind/source/name | Programs started at sign-in (Run keys, Startup folders, login items) or boot (automatic Windows services) |

## Requirements

- [uv](https://docs.astral.sh/uv/), which installs each script's python dependencies.
- `osqueryi` from an [osquery release](https://osquery.io/downloads). It is found on
  `PATH`, at the platform's default install location, or at the path in `OSQUERYI`.
  No daemon is needed.

Run unprivileged, osquery cannot read some fields of other users' processes; those come
back null. Times are UTC.

## Querying

```bash
cd trilogy_public_models/duckdb/osquery
trilogy run "import process; select name, resident_mb order by resident_mb desc limit 5;"
trilogy serve . --mcp    # expose to an agent over MCP at http://localhost:8100/mcp
```

```trilogy
import process;
SELECT user.username, count(pid) -> processes, sum(resident_mb) -> total_resident_mb
ORDER BY total_resident_mb desc nulls last;
```

The model reads the local machine, so it is kept out of the browser Studio catalog
(`.unpublished`).
