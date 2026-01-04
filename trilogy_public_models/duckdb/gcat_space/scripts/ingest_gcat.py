import asyncio
import csv
import httpx
import io
from pathlib import Path
from typing import List

BASE_URL = "https://planet4589.org/space/gcat/"
DOWNLOAD_DIR = Path(__file__).parent.parent
CONCURRENCY = 6
RETRIES = 3
CHUNK_SIZE = 32_768  # bytes
#https://planet4589.org/space/gcat/tsv/launch/launch.tsv
# Allowlist (real paths on the site). These preserve the server layout under tsv/
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
    "tsv/launch/launch.tsv",
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

# Post-processing rules per file
# Key: filename (without path), Value: function that takes (headers, all_rows) and modifies all_rows in place
POST_PROCESSING_RULES = {}


def apply_lvs_postprocessing(headers: List[str], all_rows: List[List[str]]):
    """
    Post-process lvs.cleaned.tsv to fix duplicate PKs:
    - Any row where Stage_Name = 'LES' gets Stage_No changed to 'E' (instead of 'F')
    """
    try:
        stage_name_idx = headers.index("Stage_Name")
        stage_no_idx = headers.index("Stage_No")
    except ValueError as e:
        print(f"Warning: Could not find required column for lvs post-processing: {e}")
        return

    modified_count = 0
    for row in all_rows[1:]:  # Skip header
        if len(row) > stage_name_idx and row[stage_name_idx] == "LES":
            if len(row) > stage_no_idx:
                row[stage_no_idx] = "E"
                modified_count += 1

    if modified_count > 0:
        print(
            f"Applied lvs post-processing: Changed Stage_No to 'E' for {modified_count} LES rows"
        )


POST_PROCESSING_RULES["lvs.cleaned.tsv"] = apply_lvs_postprocessing


def clean_and_process_tsv(tsv_path: Path) -> Path:
    """
    Clean TSV file by:
    1. Removing comment lines (lines starting with #) except the first line
    2. Stripping trailing/leading spaces from all fields
    3. Converting '-' to empty string in numeric columns
    """
    cleaned_path = tsv_path.with_suffix(".cleaned.tsv")

    try:
        # Read the raw file to handle comments manually
        with open(tsv_path, "r", encoding=ENCODING, errors="replace") as infile:
            lines = infile.readlines()

        if not lines:
            return cleaned_path

        # Process header - always use first line, strip # if present
        header_line = lines[0].lstrip("#").strip()

        # Collect non-comment data lines
        data_lines = []
        comment_count = 0

        for line in lines[1:]:
            if line.strip().startswith("#"):
                comment_count += 1
            else:
                data_lines.append(line.strip())

        if comment_count > 0:
            print(f"Stripped {comment_count} comment line(s) from {tsv_path.name}")

        # Parse with CSV reader to handle fields properly
        all_rows = []

        # Parse header
        header_reader = csv.reader(io.StringIO(header_line), delimiter="\t")
        headers = [col.strip() for col in next(header_reader)]
        all_rows.append(headers)

        # Parse data rows and strip whitespace
        for line in data_lines:
            if line:  # Skip empty lines
                row_reader = csv.reader(io.StringIO(line), delimiter="\t")
                row = [field.strip() for field in next(row_reader)]
                all_rows.append(row)

        if len(all_rows) <= 1:  # Only header, no data
            with open(cleaned_path, "w", encoding=ENCODING, newline="") as outfile:
                writer = csv.writer(outfile, delimiter="\t")
                writer.writerows(all_rows)
            return cleaned_path

        # Identify numeric columns (columns where all non-'-' values can be converted to float)
        numeric_columns = set()
        for col_idx, header in enumerate(headers):
            printFlag = False
            if header == "Latitude":
                printFlag = True
            if printFlag:
                print(f"Checking column: {header}")
            non_dash_values = []
            for row in all_rows[1:]:  # Skip header
                if col_idx < len(row) and row[col_idx] != "-" and row[col_idx] != "":
                    non_dash_values.append(row[col_idx])
            if printFlag:
                print(f"Non-dash values for {header}: {non_dash_values}")
            if non_dash_values:  # Only check if there are non-dash values
                try:
                    # Try to convert all non-dash values to float
                    for val in non_dash_values:
                        float(val)
                    numeric_columns.add(col_idx)

                    print(f"Converting '-' to empty in numeric column: {header}")
                except ValueError as e:
                    if printFlag:
                        print(f"Column '{header}' is not numeric: {e}")
                    # Not a numeric column
                    pass

        # Replace '-' with empty string in numeric columns
        for row in all_rows[1:]:  # Skip header
            for col_idx in numeric_columns:
                if col_idx < len(row) and row[col_idx] == "-":
                    row[col_idx] = ""

        # Apply file-specific post-processing rules
        filename = cleaned_path.name
        if filename in POST_PROCESSING_RULES:
            POST_PROCESSING_RULES[filename](headers, all_rows)

        # Write cleaned TSV
        with open(cleaned_path, "w", encoding=ENCODING, newline="") as outfile:
            writer = csv.writer(outfile, delimiter="\t")
            writer.writerows(all_rows)

        print(f"Processed and cleaned: {tsv_path.name} -> {cleaned_path.name}")

    except Exception as e:
        print(f"Error processing {tsv_path.name}: {e}")
        # Fallback to simple cleaning
        with open(tsv_path, "r", encoding=ENCODING, errors="replace") as infile:
            lines = infile.readlines()

        with open(cleaned_path, "w", encoding=ENCODING) as outfile:
            if lines:
                # Write header
                header = lines[0].lstrip("#").strip()
                outfile.write(header + "\n")

                # Write data lines, stripping whitespace from fields
                for line in lines[1:]:
                    if not line.strip().startswith("#") and line.strip():
                        # Split, strip each field, rejoin
                        fields = [field.strip() for field in line.strip().split("\t")]
                        outfile.write("\t".join(fields) + "\n")

    return cleaned_path


async def fetch_and_save(
    client: httpx.AsyncClient, rel_path: str, sem: asyncio.Semaphore
):
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

                # Clean and process the downloaded file
                clean_and_process_tsv(out_path)

                # Remove the original raw file to save space
                out_path.unlink()

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
    limits = httpx.Limits(
        max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY
    )
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        tasks = [fetch_and_save(client, p, sem) for p in FILES_TO_DOWNLOAD]
        # run tasks, catching errors per task inside fetch_and_save
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
