#!/usr/bin/env python
"""Refresh + publish flights end-to-end. Used as the `refresh` script for
the `flight` and `flight_watermark` refreshable roots in flight.preql.

Runs ingest/refresh_flights.py then ingest/publish_flights.py. Exit 0 only
if both succeed.
"""
import subprocess
import sys
from pathlib import Path

INGEST_DIR = Path(__file__).resolve().parent
REFRESH = INGEST_DIR / "refresh_flights.py"
PUBLISH = INGEST_DIR / "publish_flights.py"


def _run(script: Path) -> int:
    print(f"--- {script.name} ---", flush=True)
    return subprocess.run(["uv", "run", str(script)], check=False).returncode


def main() -> int:
    rc = _run(REFRESH)
    if rc != 0:
        return rc
    return _run(PUBLISH)


if __name__ == "__main__":
    sys.exit(main())
