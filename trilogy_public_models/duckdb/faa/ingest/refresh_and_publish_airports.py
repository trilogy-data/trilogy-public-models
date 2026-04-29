#!/usr/bin/env python
"""Refresh + publish airports end-to-end. Used as the `refresh` script for
the `airport` refreshable root.

Runs ingest/refresh_airports.py then ingest/publish_dimensions.py.
"""
import subprocess
import sys
from pathlib import Path

INGEST_DIR = Path(__file__).resolve().parent
REFRESH = INGEST_DIR / "refresh_airports.py"
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
