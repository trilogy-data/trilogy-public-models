"""Extract BEAVER MySQL dumps into an ignored, Docker-ready init directory."""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

DATABASES = ("dw", "neutron", "nova")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path(".cache/beaver/mysql-init")
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    resolved_output = args.output.resolve()
    resolved_output.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.archive) as bundle:
        for index, db in enumerate(DATABASES, start=1):
            destination = resolved_output / f"{index:02d}-{db}.sql"
            if destination.exists() and destination.stat().st_size and not args.force:
                print(f"Keeping existing {destination}")
                continue
            print(f"Extracting {db} to {destination}")
            with bundle.open(f"beaver_db/{db}.sql") as source, destination.open(
                "wb"
            ) as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(chunk)
    print(resolved_output)


if __name__ == "__main__":
    main()
