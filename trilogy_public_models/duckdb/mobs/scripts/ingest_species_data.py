#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "wikipedia",
#     "httpx",
#     "beautifulsoup4",
#     "duckdb",
#     "google-generativeai",
# ]
# ///

import wikipedia
import httpx
from bs4 import BeautifulSoup
import duckdb
import csv
from pathlib import Path
import google.generativeai as genai
import json
import time
import os
from typing import Dict

# Configure Gemini API - you'll need to set your API key
# genai.configure(api_key="YOUR_GEMINI_API_KEY_HERE")
# Or set environment variable: GEMINI_API_KEY


class SpeciesCategorizer:
    def __init__(self):
        # Categorization arrays and values as specified
        self.biome_habitat_options = [
            "Coastal / Intertidal Zone",
            "Estuarine (Brackish)",
            "Coral Reef",
            "Open Ocean (Pelagic)",
            "Deep Sea (Abyssal, Hadal)",
            "Benthic (Seafloor)",
            "Polar / Arctic / Antarctic",
            "Mangroves / Salt Marshes",
            "Kelp Forests",
            "Hydrothermal Vents / Cold Seeps",
        ]

        self.functional_group_options = [
            "Primary Producers",
            "Herbivores",
            "Planktivores",
            "Predators",
            "Scavengers",
            "Detritivores",
            "Filter Feeders",
            "Apex Predators",
        ]

        self.environmental_tolerance_options = [
            "Eurythermal",
            "Stenothermal",
            "Pelagic",
            "Benthic",
            "Nekton",
            "Plankton",
            "Migratory",
            "Resident",
            "Symbiotic",
            "Free-living",
            "Burrowers",
            "Surface Dwellers",
        ]

        self.conservation_status_options = [
            "Endangered / Vulnerable Species",
            "Invasive Species",
            "Keystone Species",
            "Commercial Species",
            "Bycatch Species",
        ]

        self.life_strategy_options = [
            "Broadcast Spawners",
            "Live-bearers",
            "Brooders / Egg Layers",
            "Planktonic Larvae",
            "Direct Developers",
            "R-Strategists",
            "K-Strategists",
        ]

        self.regional_ocean_options = [
            "Atlantic Ocean",
            "Pacific Ocean",
            "Indian Ocean",
            "Southern Ocean",
            "Arctic Ocean",
            "Regional Seas",
        ]

        # Initialize Gemini model
        try:
            api_key = os.getenv("GEMINI_API_KEY")
            if api_key:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel("gemini-2.0-flash")
            else:
                print(
                    "Warning: GEMINI_API_KEY not found. Set environment variable to enable AI categorization."
                )
                self.model = None
                raise ValueError("GEMINI_API_KEY not set")
        except Exception as e:
            print(f"Error initializing Gemini: {e}")
            self.model = None


def get_species_list():
    """Get distinct species from the database"""
    db = duckdb.connect(database=":memory:")
    db.execute(
        """INSTALL httpfs;
LOAD httpfs;
CREATE TABLE data_all_112224 AS
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);
"""
    )

    # Get species by combining Genus and Species columns
    species = db.execute(
        """
SELECT DISTINCT scientificName
FROM data_all_112224
WHERE scientificName IS NOT NULL AND scientificName != '';
"""
    ).fetchall()
    return [s[0] for s in species]


def extract_response_content(response: str):
    # may come in as ```json
    # {content}```
    if response.startswith("```json"):
        response = response[8:-3].strip()
    elif response.startswith("```"):
        response = response[3:-3].strip()
    return response


def get_wikipedia_summary_and_image(species_name: str) -> Dict:
    """Get Wikipedia summary and image for a species"""
    result = {
        "species": species_name,
        "summary": None,
        "image_url": None,
        "page_url": None,
        "error": None,
    }

    try:
        # Search and get page title
        search_results = wikipedia.search(species_name)
        if not search_results:
            result["error"] = "No Wikipedia page found"
            return result

        page_title = search_results[0]
        wiki_page = wikipedia.page(page_title, auto_suggest=False, redirect=True)
        result["summary"] = wikipedia.summary(page_title, sentences=3)
        result["page_url"] = wiki_page.url

        # Parse the image from the actual HTML using httpx
        with httpx.Client() as client:
            response = client.get(wiki_page.url)
            response.raise_for_status()  # Raise an exception for bad status codes
            soup = BeautifulSoup(response.content, "html.parser")

            # Most top-right images are in the infobox
            infobox = soup.find("table", {"class": "infobox"})
            if infobox:
                img_tag = infobox.find("img")  # type: ignore
                if img_tag and img_tag.has_attr("src"):
                    result["image_url"] = (
                        "https:" + img_tag["src"]
                        if isinstance(img_tag["src"], str)
                        else None
                    )

    except Exception as e:
        result["error"] = str(e)

    return result


def categorize_species_with_gemini(
    categorizer: SpeciesCategorizer, species_name: str, summary: str
) -> Dict:
    """Use Gemini API to categorize a species"""
    if not categorizer.model:
        return {
            "biome_habitat": [],
            "functional_group": [],
            "environmental_tolerance": [],
            "conservation_status": [],
            "life_strategy": [],
            "regional_ocean": [],
            "error": "Gemini API not available",
        }

    # Create the prompt for categorization
    prompt = f"""
    Analyze the following marine species and categorize it according to the provided options. 
    
    Species: {species_name}
    Description: {summary}
    
    Please categorize this species by selecting the most appropriate options from each category. 
    Return your response as a JSON object with the following structure:
    
    {{
        "biome_habitat": [list of applicable options from: {categorizer.biome_habitat_options}],
        "functional_group": [list of applicable options from: {categorizer.functional_group_options}],
        "environmental_tolerance": [list of applicable options from: {categorizer.environmental_tolerance_options}],
        "conservation_status": [list of applicable options from: {categorizer.conservation_status_options}],
        "life_strategy": [list of applicable options from: {categorizer.life_strategy_options}],
        "regional_ocean": [list of applicable options from: {categorizer.regional_ocean_options}]
    }}
    
    Only include options that clearly apply to this species. If uncertain, leave the array empty for that category.
    Respond only with the JSON object, no additional text.
    """

    # try:
    response = categorizer.model.generate_content(prompt)
    # Parse the JSON response
    response_json = response.candidates[0].content.parts[0].text.strip()
    categorization = json.loads(extract_response_content(response_json))
    return categorization
    # except Exception as e:
    #     return {
    #         "biome_habitat": [],
    #         "functional_group": [],
    #         "environmental_tolerance": [],
    #         "conservation_status": [],
    #         "life_strategy": [],
    #         "regional_ocean": [],
    #         "error": f"Gemini API error: {str(e)}"
    #     }


def load_existing_species(csv_path: Path) -> set:
    """Load existing species from CSV to avoid duplicates"""
    existing_species = set()
    if csv_path.exists():
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_species.add(row["species"])
        except Exception as e:
            print(f"Error reading existing CSV: {e}")
    return existing_species


def main():
    print("Fetching species list...")
    species_list = get_species_list()
    print(f"Found {len(species_list)} unique species")

    # Initialize categorizer
    categorizer = SpeciesCategorizer()

    # Set up CSV file
    target = Path(__file__).parent.parent / "species_data.csv"
    existing_species = load_existing_species(target)
    print(f"Existing species in CSV: {len(existing_species)}")

    # Filter out species that already exist
    new_species_list = [
        species for species in species_list if species not in existing_species
    ]
    print(f"New species to process: {len(new_species_list)}")

    # Define CSV fieldnames
    fieldnames = [
        "species",
        "image_url",
        "summary",
        "page_url",
        "biome_habitat",
        "functional_group",
        "environmental_tolerance",
        "conservation_status",
        "life_strategy",
        "regional_ocean",
        "categorization_error",
    ]

    # Process species and write to CSV
    write_mode = "a" if target.exists() and len(existing_species) > 0 else "w"

    with open(target, write_mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only if creating new file
        if write_mode == "w":
            writer.writeheader()

        processed_count = 0
        for full_species_name in new_species_list:
            try:
                # Get Wikipedia data
                print(f"Processing: {full_species_name}")
                wiki_data = get_wikipedia_summary_and_image(full_species_name)

                # Get AI categorization if summary is available
                categorization = {}
                if wiki_data["summary"] and categorizer.model:
                    print("  Categorizing with AI...")
                    categorization = categorize_species_with_gemini(
                        categorizer, full_species_name, wiki_data["summary"]
                    )
                    # Add small delay to respect API limits
                    time.sleep(1)

                # Prepare row data
                row_data = {
                    "species": full_species_name,
                    "image_url": wiki_data.get("image_url", ""),
                    "summary": wiki_data.get("summary", ""),
                    "page_url": wiki_data.get("page_url", ""),
                    "biome_habitat": json.dumps(
                        categorization.get("biome_habitat", [])
                    ),
                    "functional_group": json.dumps(
                        categorization.get("functional_group", [])
                    ),
                    "environmental_tolerance": json.dumps(
                        categorization.get("environmental_tolerance", [])
                    ),
                    "conservation_status": json.dumps(
                        categorization.get("conservation_status", [])
                    ),
                    "life_strategy": json.dumps(
                        categorization.get("life_strategy", [])
                    ),
                    "regional_ocean": json.dumps(
                        categorization.get("regional_ocean", [])
                    ),
                    "categorization_error": categorization.get("error", ""),
                }

                writer.writerow(row_data)
                processed_count += 1

                # Print progress every 10 species
                if processed_count % 10 == 0:
                    print(
                        f"  Processed {processed_count}/{len(new_species_list)} species"
                    )

            except Exception as e:
                raise ValueError("Error processing species data") from e
                print(f"Error processing {full_species_name}: {e}")
                # Write error row
                error_row = {field: "" for field in fieldnames}
                error_row.update(
                    {
                        "species": full_species_name,
                        "categorization_error": f"Processing error: {str(e)}",
                    }
                )
                writer.writerow(error_row)

    print(f"\nCompleted! Processed {processed_count} new species.")
    print(f"Data written to {target}")


if __name__ == "__main__":
    main()
