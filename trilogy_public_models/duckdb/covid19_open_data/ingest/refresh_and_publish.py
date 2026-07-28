#!/usr/bin/env python
"""Refresh + publish the COVID-19 Open Data tiles end-to-end.

Runs ingest/build_extract.py (download + recompress the upstream CSVs to local
parquet) then ingest/publish_extract.py (upload them to GCS). This is the entry
point wired into the Refresh Data CI workflow.
"""
import subprocess
import sys
from pathlib import Path

INGEST_DIR = Path(__file__).resolve().parent
BUILD = INGEST_DIR / "build_extract.py"
PUBLISH = INGEST_DIR / "publish_extract.py"


def _run(script: Path, args: list[str]) -> int:
    print(f"--- {script.name} ---", flush=True)
    return subprocess.run(["uv", "run", str(script), *args], check=False).returncode


def main() -> int:
    # build takes no args; forward any CLI args (e.g. --force) to publish only.
    rc = _run(BUILD, [])
    if rc != 0:
        return rc
    return _run(PUBLISH, sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
