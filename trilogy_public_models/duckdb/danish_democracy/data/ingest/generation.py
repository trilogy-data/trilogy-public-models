# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=17"]
# ///
"""Prepare a complete, immutable local generation. Never uploads or activates it.

Run with extraction stopped: copying seven files does not create an upstream
transactional snapshot. The eventual publisher must upload the whole bundle,
verify its hashes, then conditionally switch ONE pointer to its model entrypoint.
Readers must resolve that pointer once per session, not once per table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq
from source import (  # type: ignore[import-not-found]
    ENTITIES,
    PUBLIC,
    IncompleteExtract,
    schema,
)

MODEL = Path(__file__).resolve().parents[2]


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare(source_dir: Path, output_dir: Path) -> Path:
    """Validate staged copies and expose one complete directory by local rename.

    No current pointer is written. Existing generations are never overwritten.
    The generation ID includes both data and model source content.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".staging-", dir=output_dir) as work:
        staging = Path(work) / "generation"
        staging.mkdir()
        files = {}
        for entity in ENTITIES:
            target = staging / f"{entity}.parquet"
            shutil.copyfile(source_dir / target.name, target)
            table = pq.read_table(target)
            expected = schema(entity)
            if not table.schema.equals(expected, check_metadata=False):
                raise IncompleteExtract(f"{entity}: schema does not match extract")
            for field in expected:
                if not field.nullable and table[field.name].null_count:
                    raise IncompleteExtract(f"{entity}: null required {field.name}")
            if pc.count_distinct(table["id"]).as_py() != table.num_rows:
                raise IncompleteExtract(f"{entity}: duplicate IDs")
            if table["ids_reconciled_at"].null_count:
                raise IncompleteExtract(f"{entity}: missing reconciliation timestamp")
            files[entity] = {
                "path": target.name,
                "sha256": digest(target),
                "rows": table.num_rows,
                "bytes": target.stat().st_size,
            }
        models = {
            path.name: path.read_text()
            for path in sorted(MODEL.glob("*.preql"))
        }
        identity = {"format_version": 1, "files": files, "models": models}
        generation = hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode()
        ).hexdigest()
        model_dir = staging / "model"
        model_dir.mkdir()
        model_files = {}
        for name, content in models.items():
            path = model_dir / name
            path.write_text(content.replace(PUBLIC, PUBLIC + f"generations/{generation}/"))
            model_files[name] = {"path": f"model/{name}", "sha256": digest(path)}
        manifest = {
            "format_version": 1,
            "generation": generation,
            "files": files,
            "models": model_files,
            "entrypoint": "model/entrypoint.preql",
            "source": "Folketinget Open Data https://oda.ft.dk/",
            "conditions": "https://www.ft.dk/-/media/sites/ft/pdf/dokumenter/aabne-data/conditions_for_use_-of_the_danish_parliaments-_open_data.pdf",
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        destination = output_dir / generation
        if destination.exists():
            raise FileExistsError(f"generation already exists: {destination}")
        staging.rename(destination)
        return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(prepare(args.source_dir, args.output_dir))


if __name__ == "__main__":
    main()
