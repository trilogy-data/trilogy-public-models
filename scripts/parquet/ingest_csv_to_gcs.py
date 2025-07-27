#!/usr/bin/env python3
"""
Script to download CSV from GitHub, convert to Parquet, and upload to Google Cloud Storage.
"""

import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse
import requests
import pandas as pd
import click
from google.cloud import storage


def download_csv(url: str, output_path: str) -> None:
    """Download CSV file from URL to local path."""
    print(f"Downloading CSV from: {url}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded to: {output_path}")


def convert_csv_to_parquet(csv_path: str, parquet_path: str) -> None:
    """Convert CSV file to Parquet format."""
    print("Converting CSV to Parquet...")

    # Read CSV with pandas
    df = pd.read_csv(csv_path)

    print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")

    # Write to Parquet
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    print(f"Converted to Parquet: {parquet_path}")


def upload_to_gcs(local_path: str, bucket_name: str, blob_path: str) -> None:
    """Upload file to Google Cloud Storage."""
    print(f"Uploading to GCS bucket '{bucket_name}' at path '{blob_path}'")

    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Upload file
    blob.upload_from_filename(local_path)

    print(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")


def get_filename_from_url(url: str) -> str:
    """Extract filename from URL."""
    parsed_url = urlparse(url)
    return Path(parsed_url.path).name


def process_csv_to_parquet_gcs(
    source_url: str, bucket_name: str, subdirectory: str
) -> None:
    """Main processing function."""
    # Create temporary directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Get original filename and create parquet filename
        original_filename = get_filename_from_url(source_url)
        if not original_filename.endswith(".csv"):
            raise ValueError(
                f"Source URL must point to a CSV file, got: {original_filename}"
            )

        parquet_filename = original_filename.replace(".csv", ".parquet")

        # Local paths
        csv_path = os.path.join(temp_dir, original_filename)
        parquet_path = os.path.join(temp_dir, parquet_filename)

        # GCS path
        gcs_blob_path = f"{subdirectory}/{parquet_filename}"

        try:
            # Download CSV
            download_csv(source_url, csv_path)

            # Convert to Parquet
            convert_csv_to_parquet(csv_path, parquet_path)

            # Upload to GCS
            upload_to_gcs(parquet_path, bucket_name, gcs_blob_path)

            print("\n✅ Process completed successfully!")
            print(f"   Source: {source_url}")
            print(f"   Destination: gs://{bucket_name}/{gcs_blob_path}")

        except Exception as e:
            print(f"\n❌ Error during processing: {e}")
            raise


@click.command()
@click.option(
    "--source-url",
    default="https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/main/data_all_112224.csv",
    help="URL of the CSV file to download",
)
@click.option(
    "--bucket",
    default="trilogy_public_ocean_data",
    help="Google Cloud Storage bucket name",
)
@click.option(
    "--subdirectory", default="mobs_data", help="Subdirectory in the GCS bucket"
)
def main(source_url: str, bucket: str, subdirectory: str):
    """
    Download CSV from URL, convert to Parquet, and upload to Google Cloud Storage.

    This script downloads a CSV file from a given URL (default: MOBS_OPEN dataset),
    converts it to Parquet format for better performance and compression,
    and uploads it to a specified Google Cloud Storage bucket.

    Examples:

        # Use defaults
        python script.py

        # Custom parameters
        python script.py --source-url "https://example.com/data.csv" --bucket "my-bucket" --subdirectory "my-data"

        # Help
        python script.py --help
    """
    try:
        process_csv_to_parquet_gcs(source_url, bucket, subdirectory)
    except Exception as e:
        print(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check if running with click arguments or directly
    if len(sys.argv) > 1:
        # Running with CLI arguments
        main()
    else:
        # Running directly - use defaults
        print("Running with default parameters...")
        print("Use --help to see available options")
        process_csv_to_parquet_gcs(
            source_url="https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/main/data_all_112224.csv",
            bucket_name="trilogy_public_ocean_data",
            subdirectory="mobs_data",
        )
