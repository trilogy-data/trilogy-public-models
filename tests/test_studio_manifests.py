import json
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
STUDIO_DIR = REPO_ROOT / "studio"
PAGES_BASE = "https://trilogy-data.github.io/trilogy-public-models/"


def _manifests() -> list[Path]:
    return sorted(p for p in STUDIO_DIR.glob("*.json") if p.name != "index.json")


def test_component_urls_resolve_to_repo_files():
    # GitHub Pages serves the repository tree, so a component URL is only live
    # if the path behind it exists here. build.py --check cannot catch a URL
    # that was generated wrong: it compares the manifest to its own output.
    missing: list[str] = []
    for manifest in _manifests():
        for component in json.loads(manifest.read_text(encoding="utf-8"))["components"]:
            url = component["url"]
            assert url.startswith(PAGES_BASE), f"{manifest.name}: unexpected host {url}"
            if not (REPO_ROOT / url[len(PAGES_BASE) :]).is_file():
                missing.append(f"{manifest.name}: {url}")
    assert not missing, "Manifest URLs with no file behind them:\n" + "\n".join(missing)


def test_component_names_are_unique():
    # Studio keys an imported editor on its name; a duplicate silently
    # overwrites the first file's contents with the second's.
    duplicated: list[str] = []
    for manifest in _manifests():
        components = json.loads(manifest.read_text(encoding="utf-8"))["components"]
        counts = Counter((c["type"], c["name"]) for c in components)
        duplicated.extend(
            f"{manifest.name}: {name} ({kind})"
            for (kind, name), count in counts.items()
            if count > 1
        )
    assert not duplicated, "Duplicate component names:\n" + "\n".join(duplicated)
