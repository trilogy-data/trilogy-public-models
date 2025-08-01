import wikipedia
import requests
from bs4 import BeautifulSoup
import duckdb
import csv
from pathlib import Path
import duckdb
import os


def get_genus_list():
    db = duckdb.connect(database=":memory:")
    db.execute(
        """INSTALL httpfs;
LOAD httpfs;
CREATE TABLE data_all_112224 AS
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);
"""
    )

    genus = db.execute(
        """
SELECT DISTINCT Genus FROM data_all_112224
WHERE Genus IS NOT NULL AND Genus != '';
"""
    ).fetchall()
    return [g[0] for g in genus]


def get_wikipedia_summary_and_image(genus_name):
    result = {
        "genus": genus_name,
        "summary": None,
        "image_url": None,
        "page_url": None,
    }
    try:
        # Search and get page title
        page_title = wikipedia.search(genus_name)[0]
        wiki_page = wikipedia.page(page_title, auto_suggest=False, redirect=True)
        result["summary"] = wikipedia.summary(page_title, sentences=3)
        result["page_url"] = wiki_page.url

        # Now parse the image from the actual HTML
        response = requests.get(wiki_page.url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Most top-right images are in the infobox
        infobox = soup.find("table", {"class": "infobox"})
        if infobox:
            img_tag = infobox.find("img")
            if img_tag and img_tag.has_attr("src"):
                result["image_url"] = "https:" + img_tag["src"]

    except Exception as e:
        result["error"] = str(e)

    return result


def read_csv_to_memory(csv_file):
    """
    Read CSV file into memory as a list of dictionaries
    
    Args:
        csv_file (str): Path to input CSV file
        
    Returns:
        list: List of dictionaries representing CSV rows
    """
    data = []
    
    if not os.path.exists(csv_file):
        print(f"Warning: {csv_file} not found, returning empty list")
        return data
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        
        print(f"Successfully read {len(data)} rows from {csv_file}")
        return data
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return data


import pandas as pd
import pyarrow

def csv_to_parquet(csv_file="genus.csv", parquet_file="genus.parquet"):
    """
    Convert CSV file to Parquet format using pandas and pyarrow
    First reads CSV data into memory, then creates DataFrame from the dictionaries

    Args:
        csv_file (str): Path to input CSV file
        parquet_file (str): Path to output Parquet file
    """

    # Read CSV data into memory as list of dictionaries
    csv_data = read_csv_to_memory(csv_file)
    
    if not csv_data:
        print(f"No data to convert from {csv_file}")
        return

    try:
        # Create DataFrame from the list of dictionaries
        df = pd.DataFrame(csv_data)
        
        # Set appropriate data types
        df = df.astype({
            'genus': 'string',
            'image_url': 'string', 
            'summary': 'string'
        })
        
        # Handle empty strings and NaN values
        df = df.replace('', None)  # Convert empty strings to None
        
        print(f"Successfully created DataFrame with {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        
        # Write to Parquet
        df.to_parquet(
            parquet_file,
            engine='pyarrow',
            index=False,  # Don't write row index
            compression='snappy'  # Good compression with fast read/write
        )
        
        print(f"Successfully converted {csv_file} to {parquet_file}")
        
        # Verify the conversion
        verification_df = pd.read_parquet(parquet_file)
        print(f"Parquet file contains {len(verification_df)} rows")
        
        # Show first few rows to verify structure
        print("\nFirst 3 rows of the Parquet file:")
        print(verification_df.head(3).to_string())
        
        # Show data types
        print(f"\nData types:")
        print(verification_df.dtypes)

    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    genus_list = get_genus_list()
    target = Path(__file__).parent.parent / "genus_data.csv"
    if target.exists():
        existing_data = set()
        with open(target, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                genus = row.get('genus', '').strip()
                if genus:
                    existing_data.add(genus)
    else:
        existing_data = set()
    print(f"Existing genera in CSV: {len(existing_data)}")
    # Filter out genera that already exist in the CSV
    final_genus_list = [genus for genus in genus_list if genus not in existing_data]
    print(f"Total genera to process: {len(final_genus_list)}")
    write_mode = "a" if target.exists() and len(existing_data) > 0 else "w"
    # Open CSV file for writing
    with open(target, write_mode, newline="", encoding="utf-8") as csvfile:
        fieldnames = ["genus", "image_url", "summary"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Process each genus and write to CSV
        processed_count = 0
        for genus in final_genus_list:
            data = get_wikipedia_summary_and_image(genus)

            # Write row with only the required fields
            writer.writerow(
                {
                    "genus": data["genus"],
                    "image_url": data["image_url"],  # Will be None if not found
                    "summary": data["summary"],  # Will be None if not found
                }
            )

            # Optional: Print progress
            if "error" in data:
                print(f"Error processing {genus}: {data['error']}")
            else:
                print(f"Processed: {data['genus']}")
            processed_count += 1

            # Print progress every 10 species
            if processed_count % 10 == 0:
                print(f"  Processed {processed_count}/{len(final_genus_list)} genera")
    print(f"\nData written to genus_data.csv for {len(genus_list)} genera")