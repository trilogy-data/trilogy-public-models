import pandas as pd
import re
from pathlib import Path
import csv


def minimize_species_row(row):
    """
    Minimize a species row by removing verbose taxonomic information,
    species lists, references, and other content not suitable for dashboard display.

    Args:
        row (dict or pd.Series): A row containing 'genus', 'image_url', and 'summary' fields

    Returns:
        dict: Minimized row with cleaned summary
    """

    # Create a copy to avoid modifying original data
    minimized = dict(row) if isinstance(row, dict) else row.to_dict()

    summary = minimized.get("summary", "")

    if not summary or pd.isna(summary):
        return minimized
    description_match = re.search(
        r"== Distribution ==(.*?)(?===|$)", summary, flags=re.DOTALL
    )
    preserved_description = ""
    if description_match:
        preserved_description = description_match.group(1).strip()

    taxonomy_match = re.search(r"== Taxonomy ==(.*?)(?===|$)", summary, flags=re.DOTALL)
    preserved_taxonomy = ""
    if taxonomy_match:
        preserved_taxonomy = taxonomy_match.group(1).strip()

    # Remove section headers (== Header ==)
    # summary = re.sub(r'==\s*[^=]+\s*==', '', summary)

    # Remove species lists (typically start with genus name followed by species)
    # This removes patterns like "Genus species1\nGenus species2\n..."
    # genus_name = minimized.get('genus', '')
    # if genus_name:
    #     # Remove lines that start with the genus name followed by lowercase (species names)
    #     pattern = rf'^{re.escape(genus_name)}\s+[a-z].*$'
    #     summary = re.sub(pattern, '', summary, flags=re.MULTILINE)

    # Remove common taxonomic reference patterns
    summary = re.sub(r"Species brought into synonymy.*", "", summary, flags=re.DOTALL)
    summary = re.sub(r"== Species ==.*?(?===|$)", "", summary, flags=re.DOTALL)
    summary = re.sub(r"== References ==.*", "", summary, flags=re.DOTALL)
    summary = re.sub(r"== External links ==.*", "", summary, flags=re.DOTALL)
    # summary = re.sub(r'== Distribution ==.*?(?===|$)', '', summary, flags=re.DOTALL)

    # Remove citation patterns like "(Author, Year)"
    summary = re.sub(r"\([^)]*\d{4}[^)]*\)", "", summary)

    # Remove author names and dates in taxonomic format
    summary = re.sub(r",\s*\d{4}", "", summary)
    summary = re.sub(r"\b[A-Z][a-z]+\s*,\s*\d{4}", "", summary)

    # Remove article numbers and legal references
    summary = re.sub(r"\(Art\.\s*\d*\)", "", summary)
    summary = re.sub(r"Art\.\s*\d+", "", summary)

    # Clean up multiple spaces and newlines
    summary = re.sub(r"\n+", " ", summary)
    summary = re.sub(r"\s+", " ", summary)

    # Remove leading/trailing whitespace and periods
    summary = summary.strip()

    # If summary becomes too short or empty, try to preserve the first sentence
    if len(summary.strip()) < 20 and minimized.get("summary", ""):
        original = minimized["summary"]
        # Extract first meaningful sentence
        first_sentence = re.split(r"[.!?]", original)[0]
        if len(first_sentence) > 20:
            summary = first_sentence.strip()
    if preserved_description:
        summary = summary + " " + preserved_description
    if preserved_taxonomy:
        summary = summary + " " + preserved_taxonomy
    minimized["summary"] = summary
    return minimized


if __name__ == "__main__":

    target = Path(__file__).parent.parent / "genus_data.csv"
    processed = Path(__file__).parent.parent / "genus_data_processed.csv"
    outputs = []
    if target.exists():
        existing_data: set[str] = set()
        with open(target, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                minimized = minimize_species_row(row)
                outputs.append(minimized)
    if outputs:
        with open(processed, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["genus", "image_url", "summary"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(outputs)
