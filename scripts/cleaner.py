#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "click>=8.0.0",
#     "chardet>=5.0.0",
# ]
# ///
"""
DuckDB-Compatible CSV File Cleaner - Clean and validate CSV files for DuckDB parsing

Usage with UV:
    uv run csv_cleaner.py input.csv
    uv run csv_cleaner.py input.csv -o output.csv --delimiter ";" --quote "'"
    uv run csv_cleaner.py input.csv --dry-run --verbose --validate-only
"""

import click
import sys
import csv
import re
from pathlib import Path
from typing import List, Tuple, Optional
import chardet


def detect_encoding(file_path: Path) -> str:
    """Detect file encoding using chardet"""
    with open(file_path, "rb") as f:
        raw_data = f.read(1000000)  # Read first 10KB for detection
        result = chardet.detect(raw_data)
        return result["encoding"] or "utf-8"


def detect_csv_dialect(file_path: Path, encoding: str) -> Tuple[str, str, str]:
    """Detect CSV dialect (delimiter, quotechar, lineterminator)"""
    try:
        with open(file_path, "r", encoding=encoding, errors="ignore") as f:
            # Read a sample to detect dialect
            sample = f.read(8192)

        # Try to detect dialect
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
            delimiter = dialect.delimiter
            quotechar = dialect.quotechar
        except csv.Error:
            # Fallback to comma if detection fails
            delimiter = ","
            quotechar = '"'

        # Detect line terminator
        if "\r\n" in sample:
            lineterminator = "\r\n"
        elif "\r" in sample:
            lineterminator = "\r"
        else:
            lineterminator = "\n"

        return delimiter, quotechar, lineterminator

    except Exception:
        # Default values
        return ",", '"', "\n"


def clean_csv_field(field: str, mode: str) -> str:
    """Clean individual CSV field for DuckDB compatibility"""
    if field is None:
        return ""

    # Handle UTF-8 encoding issues
    if mode == "strict":
        # Will raise exception if invalid UTF-8
        field.encode("utf-8", errors="strict")
    elif mode == "replace":
        field = field.encode("utf-8", errors="replace").decode("utf-8")
    else:  # ignore
        field = field.encode("utf-8", errors="ignore").decode("utf-8")

    # Remove or replace problematic characters for DuckDB
    # Remove null bytes (not allowed in CSV)
    field = field.replace("\x00", "")

    # Replace other control characters (except newlines which should be quoted)
    field = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]", "", field)

    # Normalize line endings within fields
    field = field.replace("\r\n", "\n").replace("\r", "\n")

    return field


def validate_csv_structure(
    file_path: Path, delimiter: str, quotechar: str, encoding: str, verbose: bool
) -> Tuple[bool, List[str]]:
    """Validate CSV structure for DuckDB compatibility"""
    issues = []

    try:
        with open(file_path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)

            # Check header row
            try:
                header = next(reader)
                if not header:
                    issues.append("Empty header row")
                else:
                    # Check for duplicate column names
                    if len(header) != len(set(header)):
                        duplicates = [col for col in header if header.count(col) > 1]
                        issues.append(f"Duplicate column names: {set(duplicates)}")

                    # Check for empty column names
                    empty_cols = [i for i, col in enumerate(header) if not col.strip()]
                    if empty_cols:
                        issues.append(f"Empty column names at positions: {empty_cols}")

                expected_cols = len(header)

            except StopIteration:
                issues.append("File is empty")
                return False, issues

            # Check data rows
            row_num = 2  # Start from 2 (header is row 1)
            inconsistent_rows = []

            for row in reader:
                if len(row) != expected_cols:
                    inconsistent_rows.append(
                        f"Row {row_num}: {len(row)} columns (expected {expected_cols})"
                    )
                    if len(inconsistent_rows) >= 10:  # Limit error reporting
                        inconsistent_rows.append(
                            "... (more rows with column count issues)"
                        )
                        break
                row_num += 1

            if inconsistent_rows:
                issues.extend(inconsistent_rows)

    except csv.Error as e:
        issues.append(f"CSV parsing error: {e}")
    except UnicodeDecodeError as e:
        issues.append(f"Encoding error: {e}")

    is_valid = len(issues) == 0

    if verbose:
        if is_valid:
            click.echo("✓ CSV structure validation passed")
        else:
            click.echo("✗ CSV structure validation failed:")
            for issue in issues:
                click.echo(f"  - {issue}")

    return is_valid, issues


@click.command()
@click.argument("input_file", type=click.Path(exists=True, readable=True))
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Output file path. If not specified, overwrites input file.",
)
@click.option(
    "--mode",
    type=click.Choice(["ignore", "replace", "strict"]),
    default="replace",
    help="How to handle invalid UTF-8 characters: ignore (remove), replace (with �), or strict (fail)",
)
@click.option(
    "--delimiter",
    "-d",
    default=None,
    help="CSV delimiter (auto-detected if not specified)",
)
@click.option(
    "--quote",
    "-q",
    default=None,
    help="CSV quote character (auto-detected if not specified)",
)
@click.option(
    "--encoding", default=None, help="File encoding (auto-detected if not specified)"
)
@click.option(
    "--backup/--no-backup",
    default=True,
    help="Create backup of original file when overwriting (default: True)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be done without making changes"
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed output")
@click.option(
    "--validate-only", is_flag=True, help="Only validate CSV structure without cleaning"
)
@click.option(
    "--fix-headers",
    is_flag=True,
    help="Automatically fix common header issues (duplicates, empty names)",
)
def clean_csv(
    input_file,
    output,
    mode,
    delimiter,
    quote,
    encoding,
    backup,
    dry_run,
    verbose,
    validate_only,
    fix_headers,
):
    """
    Clean and validate CSV files for DuckDB compatibility.

    INPUT_FILE: Path to the CSV file to clean

    This tool:
    - Ensures UTF-8 encoding
    - Removes problematic control characters
    - Validates CSV structure
    - Fixes common DuckDB compatibility issues

    Examples:

        # Clean CSV with auto-detection
        uv run csv_cleaner.py data.csv

        # Clean with specific delimiter and quote char
        uv run csv_cleaner.py data.csv -d ";" -q "'" -o clean_data.csv

        # Validate only without cleaning
        uv run csv_cleaner.py data.csv --validate-only -v

        # Fix headers and clean data
        uv run csv_cleaner.py data.csv --fix-headers -v
    """
    input_path = Path(input_file)

    # Determine output path
    if output:
        output_path = Path(output)
        overwrite_input = False
    else:
        output_path = input_path
        overwrite_input = True

    if verbose:
        click.echo(f"Input file: {input_path}")
        if not validate_only:
            click.echo(f"Output file: {output_path}")
        click.echo(f"Mode: {mode}")
        click.echo(f"Dry run: {dry_run}")

    try:
        # Detect encoding
        if not encoding:
            encoding = detect_encoding(input_path)
            if verbose:
                click.echo(f"Detected encoding: {encoding}")

        # Detect CSV dialect
        if not delimiter or not quote:
            detected_delimiter, detected_quote, line_terminator = detect_csv_dialect(
                input_path, encoding
            )
            delimiter = delimiter or detected_delimiter
            quote = quote or detected_quote
            if verbose:
                click.echo(
                    f"Detected CSV format - delimiter: '{delimiter}', quote: '{quote}', line terminator: '{repr(line_terminator)}'"
                )
        else:
            line_terminator = "\n"

        # Validate CSV structure
        is_valid, issues = validate_csv_structure(
            input_path, delimiter, quote, encoding, verbose
        )

        if validate_only:
            if is_valid:
                click.echo("✓ CSV is valid for DuckDB")
                sys.exit(0)
            else:
                click.echo("✗ CSV has issues that may prevent DuckDB parsing:")
                for issue in issues:
                    click.echo(f"  - {issue}")
                sys.exit(1)

        # Read and process the CSV
        if verbose:
            click.echo("Processing CSV data...")

        rows_processed = 0
        fields_cleaned = 0
        header_fixed = False

        with open(input_path, "r", encoding=encoding, newline="") as infile:
            reader = csv.reader(infile, delimiter=delimiter, quotechar=quote)

            # Process header
            try:
                header = next(reader)
                original_header = header.copy()

                if fix_headers:
                    # Fix duplicate column names
                    seen = {}
                    for i, col in enumerate(header):
                        col = col.strip()
                        if not col:
                            col = f"column_{i+1}"
                            header_fixed = True
                        if col in seen:
                            seen[col] += 1
                            header[i] = f"{col}_{seen[col]}"
                            header_fixed = True
                        else:
                            seen[col] = 1
                            header[i] = col

                    if header_fixed and verbose:
                        click.echo("Fixed header issues")

                # Clean header fields
                cleaned_header = []
                for field in header:
                    cleaned_field = clean_csv_field(field, mode)
                    cleaned_header.append(cleaned_field)
                    if cleaned_field != field:
                        fields_cleaned += 1

                all_rows = [cleaned_header]
                rows_processed += 1

            except StopIteration:
                click.echo("Error: CSV file is empty", err=True)
                sys.exit(1)

            # Process data rows
            for row in reader:
                cleaned_row = []
                for field in row:
                    cleaned_field = clean_csv_field(field, mode)
                    cleaned_row.append(cleaned_field)
                    if cleaned_field != field:
                        fields_cleaned += 1

                all_rows.append(cleaned_row)
                rows_processed += 1

        if verbose or dry_run:
            click.echo(f"Rows processed: {rows_processed}")
            click.echo(f"Fields cleaned: {fields_cleaned}")
            if header_fixed:
                click.echo("Header issues fixed")

        if dry_run:
            click.echo("Dry run complete - no files modified")
            return

        # Create backup if overwriting and backup is enabled
        if overwrite_input and backup and input_path.exists():
            backup_path = input_path.with_suffix(input_path.suffix + ".backup")
            if verbose:
                click.echo(f"Creating backup: {backup_path}")
            import shutil

            shutil.copy2(input_path, backup_path)

        # Write cleaned CSV
        if verbose:
            click.echo(f"Writing cleaned CSV to: {output_path}")

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(
                outfile,
                delimiter=delimiter,
                quotechar=quote,
                lineterminator=line_terminator,
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writerows(all_rows)

        # Final validation
        is_valid_final, final_issues = validate_csv_structure(
            output_path, delimiter, quote, "utf-8", False
        )

        if fields_cleaned > 0 or header_fixed:
            click.echo(
                f"✓ Cleaned {input_path}: {fields_cleaned} fields cleaned, {rows_processed} rows processed"
            )
        else:
            click.echo(f"✓ CSV {input_path} was already clean")

        if is_valid_final:
            click.echo("✓ Output CSV is valid for DuckDB")
        else:
            click.echo("⚠ Warning: Output CSV may still have issues:")
            for issue in final_issues:
                click.echo(f"  - {issue}")

    except FileNotFoundError:
        click.echo(f"Error: Input file '{input_path}' not found", err=True)
        sys.exit(1)
    except PermissionError:
        click.echo(f"Error: Permission denied accessing '{input_path}'", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    clean_csv()
