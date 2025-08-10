import asyncio
import httpx
import os
from pathlib import Path
import polars as pl

BASE_URL = "https://planet4589.org/space/gcat/"
DOWNLOAD_DIR = Path(__file__).parent.parent
CONCURRENCY = 1
RETRIES = 3
CHUNK_SIZE = 32_768

FILES_TO_DOWNLOAD = [
    "tsv/tables/orgs.tsv",
    "tsv/tables/sites.tsv",
    "tsv/tables/lp.tsv",
    "tsv/tables/platforms.tsv",
    "tsv/derived/currentcat.tsv",
    "tsv/derived/launchlog.tsv",
    "tsv/derived/active.tsv",
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
]

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

def strip_comment_line(tsv_path: Path) -> Path:
    """
    Remove the second line if it starts with '#' (comment line).
    Creates a temporary cleaned file and returns its path.
    """
    cleaned_path = tsv_path.with_suffix('.cleaned.tsv')
    
    with open(tsv_path, 'r', encoding='utf-8') as infile, \
         open(cleaned_path, 'w', encoding='utf-8') as outfile:
        
        lines = infile.readlines()
        
        if len(lines) > 1 and lines[1].strip().startswith('#'):
            # Write header line and skip comment line
            outfile.write(lines[0])
            # Write remaining lines (starting from line 3)
            outfile.writelines(lines[2:])
            print(f"Stripped comment line from {tsv_path.name}")
        else:
            # No comment line to strip, write all lines
            outfile.writelines(lines)
    
    return cleaned_path

async def fetch_and_convert(client: httpx.AsyncClient, rel_path: str, sem: asyncio.Semaphore):
    parquet_path = DOWNLOAD_DIR.joinpath(rel_path).with_suffix(".parquet")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    
    if parquet_path.exists():
        print(f"SKIP (parquet exists): {rel_path}")
        return
    
    tsv_path = parquet_path.with_suffix(".tsv")
    
    async with sem:
        for attempt in range(1, RETRIES + 1):
            try:
                print(f"Downloading ({attempt}/{RETRIES}): {rel_path}")
                async with client.stream("GET", BASE_URL + rel_path, timeout=None) as resp:
                    resp.raise_for_status()
                    with open(tsv_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                            f.write(chunk)
                
                # Convert to parquet
                from time import sleep
                sleep(5)
                
                # Strip comment line if present
                cleaned_tsv_path = strip_comment_line(tsv_path)
                
                # Read the cleaned file
                df = pl.read_csv(
                    cleaned_tsv_path, 
                    separator="\t", 
                    try_parse_dates=True, 
                    infer_schema_length=100000, 
                    encoding="utf-8"
                )
                
                df.write_parquet(parquet_path, compression="zstd")
                
                # Clean up temporary files
                tsv_path.unlink()
                cleaned_tsv_path.unlink()
                
                print(f"Saved Parquet: {parquet_path}")
                return
                
            except Exception as e:
                print(f"Error processing {rel_path} (attempt {attempt}): {e}")
                
                # Clean up files on error
                for temp_file in [tsv_path, tsv_path.with_suffix('.cleaned.tsv')]:
                    if temp_file.exists():
                        temp_file.unlink()
                if parquet_path.exists():
                    parquet_path.unlink()
                
                if attempt == RETRIES:
                    print(f"FAILED: {rel_path}")
                    raise e

async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY)
    
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        tasks = [fetch_and_convert(client, path, sem) for path in FILES_TO_DOWNLOAD]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())