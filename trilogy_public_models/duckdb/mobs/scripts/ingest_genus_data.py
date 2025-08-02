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
    e = None
    for search_phrase in [genus_name, genus_name + " genus", genus_name + " genus ocean"]:
        try:
            # Search and get page title
            print(f"Searching Wikipedia for genus: {search_phrase}")
            search = wikipedia.search(search_phrase)
            if not search:
                print(f"No Wikipedia page found for genus: {search_phrase}")
                return result
            page_title = search[0]
            
            wiki_page = wikipedia.page(page_title, auto_suggest=False, redirect=True)
            result["summary"] = wiki_page.summary
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
            return result
        except Exception as e:
            continue
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


def write_updated_csv(target_file, updated_data):
    """
    Write the complete updated data back to CSV file
    
    Args:
        target_file (Path): Path to the CSV file
        updated_data (list): List of dictionaries with all data
    """
    try:
        with open(target_file, 'w', newline="", encoding="utf-8") as csvfile:
            fieldnames = ["genus", "image_url", "summary"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updated_data)
        print(f"Successfully updated {target_file} with {len(updated_data)} rows")
    except Exception as e:
        print(f"Error writing CSV file: {e}")


def checkpoint_progress(target_file, data_dict):
    """
    Save current progress by overwriting the source file
    
    Args:
        target_file (Path): Target file path to overwrite
        data_dict (dict): Current data dictionary
    """
    updated_data_list = list(data_dict.values())
    
    try:
        write_updated_csv(target_file, updated_data_list)
        print(f"✓ Checkpoint saved: {target_file} ({len(updated_data_list)} records)")
    except Exception as e:
        print(f"✗ Failed to save checkpoint: {e}")


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
    
    # Read existing data into memory
    existing_data_list = []
    missing_image_genera = set()
    existing_genera = set()
    
    if target.exists():
        existing_data_list = read_csv_to_memory(target)
        for row in existing_data_list:
            genus = row.get('genus', '').strip()
            image = row.get('image_url', '').strip()
            if genus:
                existing_genera.add(genus)
                # Track genera that need image updates
                if not image or image in ['None', 'null', '']:
                    missing_image_genera.add(genus)
                    print(f"Missing image for genus: {genus}")
    
    print(f"Existing genera in CSV: {len(existing_genera)}")
    print(f"Genera missing images: {len(missing_image_genera)}")
    
    # Determine what needs to be processed
    new_genera = [genus for genus in genus_list if genus not in existing_genera]
    genera_to_process = list(missing_image_genera) + new_genera
    
    print(f"New genera to add: {len(new_genera)}")
    print(f"Total genera to process: {len(genera_to_process)}")
    
    if not genera_to_process:
        print("No genera to process. All data appears complete.")
        exit()
    
    # Create a dictionary for quick lookup and updates
    data_dict = {row['genus']: row for row in existing_data_list}
    
    # Process each genus with checkpointing
    processed_count = 0
    
    for genus in genera_to_process:
        data = get_wikipedia_summary_and_image(genus)
        
        # Update or add the data
        data_dict[genus] = {
            "genus": data["genus"],
            "image_url": data.get("image_url", ""),
            "summary": data.get("summary", ""),
        }
        
        # Print progress
        if "error" in data:
            print(f"Error processing {genus}: {data['error']}")
        else:
            status = "Updated" if genus in missing_image_genera else "Added"
            image_status = "with image" if data.get("image_url") else "no image found"
            print(f"{status}: {genus} ({image_status})")
        
        processed_count += 1
        
        # Checkpoint every 10 records by overwriting source file
        if processed_count % 10 == 0:
            checkpoint_progress(target, data_dict)
            print(f"  Progress: {processed_count}/{len(genera_to_process)} genera completed")
    
    # Final save to main file (redundant but ensures final state is saved)
    updated_data_list = list(data_dict.values())
    write_updated_csv(target, updated_data_list)
    
    print(f"\n✓ Completed processing {len(genera_to_process)} genera")
    print(f"Total records in final CSV: {len(updated_data_list)}")
    
    # Show final statistics
    final_missing = sum(1 for row in updated_data_list 
                       if not row.get('image_url') or row.get('image_url').strip() in ['', 'None', 'null'])
    print(f"Records still missing images: {final_missing}")