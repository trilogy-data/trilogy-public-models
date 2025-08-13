# ingest_gcat_whitelist.py
import asyncio
import httpx
import os
from pathlib import Path
from typing import List

BASE_URL = "https://planet4589.org/space/gcat/"
DOWNLOAD_DIR = Path(__file__).parent.parent
CONCURRENCY = 6
RETRIES = 3
CHUNK_SIZE = 32_768  # bytes

# Whitelist (real paths on the site). These preserve the server layout under tsv/
FILES_TO_DOWNLOAD: List[str] = [
    # Tables (organizations, sites, platforms, launch points, vehicles, engines, etc.)
    "tsv/tables/orgs.tsv",
    "tsv/tables/sites.tsv",
    "tsv/tables/lp.tsv",
    "tsv/tables/platforms.tsv",
    "tsv/tables/engines.tsv",
    "tsv/tables/family.tsv",
    "tsv/tables/lv.tsv",
    "tsv/tables/lvs.tsv",
    "tsv/tables/refs.tsv",
    "tsv/tables/stages.tsv",

    # Derived (full catalogs & logs)
    "tsv/derived/currentcat.tsv",
    "tsv/derived/launchlog.tsv",
    "tsv/derived/active.tsv",
    "tsv/derived/analyst.tsv",
    "tsv/derived/geotab.tsv",

    # Main / supporting / temporary / payload catalogs (catalog directory)
    "tsv/cat/satcat.tsv",
    "tsv/cat/auxcat.tsv",
    "tsv/cat/ftocat.tsv",
    "tsv/cat/tmpcat.tsv",
    "tsv/cat/csocat.tsv",
    "tsv/cat/lcat.tsv",
    "tsv/cat/rcat.tsv",
    "tsv/cat/ecat.tsv",
    "tsv/cat/deepcat.tsv",
    "tsv/cat/deepindex.tsv",
    "tsv/cat/hcocat.tsv",
    "tsv/cat/lprcat.tsv",
    "tsv/cat/landercat.tsv",
    "tsv/cat/psatcat.tsv",
    "tsv/cat/pauxcat.tsv",
    "tsv/cat/pftocat.tsv",
    "tsv/cat/ptmpcat.tsv",
    "tsv/cat/plcat.tsv",
    "tsv/cat/prcat.tsv",
    "tsv/cat/pdeepcat.tsv",
    "tsv/cat/usatcat.tsv",
    "tsv/cat/cargocat.tsv",
    "tsv/cat/xtlcat.tsv",
]

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

ENCODING = "utf-8"

def strip_comment_line(tsv_path: Path) -> Path:
    """
    Remove the second line if it starts with '#' (comment line).
    Creates a temporary cleaned file and returns its path.
    """
    cleaned_path = tsv_path.with_suffix('.cleaned.tsv')
    # with open(tsv_path, 'rb') as file:
    #     raw_data = file.read()
    #     encoding = chardet.detect(raw_data)['encoding']
    #     print(encoding)
    with open(tsv_path, 'rb') as infile, \
         open(cleaned_path, 'wb') as outfile:

        lines = infile.readlines()
        
        if len(lines) > 1 and lines[1].strip().startswith(b'#'):
            # Write header line and skip comment line
            outfile.write(lines[0].lstrip(b'#'))
            # Write remaining lines (starting from line 3)
            outfile.writelines(lines[2:])
            print(f"Stripped comment line from {tsv_path.name}")
        else:
            # No comment line to strip, write all lines
            outfile.writelines(lines)
    
    return cleaned_path

async def fetch_and_save(client: httpx.AsyncClient, rel_path: str, sem: asyncio.Semaphore):
    url = BASE_URL + rel_path
    out_path = DOWNLOAD_DIR.joinpath(rel_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip if file already exists
    # if out_path.exists():
    #     print(f"SKIP (exists): {rel_path}")
    #     return

    async with sem:
        for attempt in range(1, RETRIES + 1):
            try:
                print(f"Downloading ({attempt}/{RETRIES}): {url}")
                # stream the response to disk
                async with client.stream("GET", url, timeout=None) as resp:
                    resp.raise_for_status()
                    # write in binary chunks
                    with open(out_path, "wb") as fd:
                        async for chunk in resp.aiter_bytes(chunk_size=CHUNK_SIZE):
                            fd.write(chunk)
                print(f"Saved: {out_path}")
                strip_comment_line(out_path)
                out_path.unlink()
                parquet_path = out_path.with_suffix(".parquet")
                if parquet_path.exists():
                    parquet_path.unlink()
                return
            except Exception as e:
                # remove partial file if any
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except Exception:
                        pass
                print(f"Error downloading {rel_path} (attempt {attempt}): {e}")
                if attempt == RETRIES:
                    print(f"Failed to download after {RETRIES} attempts: {rel_path}")


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        tasks = [fetch_and_save(client, p, sem) for p in FILES_TO_DOWNLOAD]
        # run tasks, catching errors per task inside fetch_and_save
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
