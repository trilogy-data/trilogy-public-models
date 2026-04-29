#!/usr/bin/env python
"""Refresh + publish aircraft (and aircraft_models) end-to-end. Used as the
`refresh` script for the `aircraft`, `aircraft_tail_num`, and
`aircraft_model` refreshable roots.

Runs ingest/refresh_aircraft.py then ingest/publish_dimensions.py. publish_dimensions
uploads everything in dimensions/, including the freshly-built aircraft
parquets.
"""
import subprocess
import sys
from pathlib import Path

INGEST_DIR = Path(__file__).resolve().parent
REFRESH = INGEST_DIR / "refresh_aircraft.py"
PUBLISH = INGEST_DIR / "publish_dimensions.py"


def _run(script: Path) -> int:
    print(f"--- {script.name} ---", flush=True)
    return subprocess.run(["uv", "run", str(script)]).returncode


def main() -> int:
    rc = _run(REFRESH)
    if rc != 0:
        return rc
    return _run(PUBLISH)


if __name__ == "__main__":
    sys.exit(main())
