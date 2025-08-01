import wikipedia
import requests
from bs4 import BeautifulSoup
import duckdb
import csv
from pathlib import Path

def get_genus_list():
    db = duckdb.connect(database=':memory:')
    db.execute('''INSTALL httpfs;
LOAD httpfs;
CREATE TABLE data_all_112224 AS
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/crmcclain/MOBS_OPEN/refs/heads/main/data_all_112224.csv',
sample_size=-1);
''')
   
    genus = db.execute('''
SELECT DISTINCT Genus FROM data_all_112224
WHERE Genus IS NOT NULL AND Genus != '';
''').fetchall()
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

if __name__ == "__main__":
    genus_list = get_genus_list()
    target = Path(__file__).parent.parent / 'genus_data.csv'
    with open(target, 'r', encoding='utf-8') as f:
        # get existing values
        existing_data = set(line.strip().split(',')[0] for line in f if line.strip())
    print(f"Existing genera in CSV: {len(existing_data)}")
    # Filter out genera that already exist in the CSV
    final_genus_list = [genus for genus in genus_list if genus not in existing_data]    
    print(f"Total genera to process: {len(final_genus_list)}")
    # Open CSV file for writing
    with open(target, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['genus', 'image_url', 'summary']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Process each genus and write to CSV
        for genus in final_genus_list:
            data = get_wikipedia_summary_and_image(genus)
            
            # Write row with only the required fields
            writer.writerow({
                'genus': data['genus'],
                'image_url': data['image_url'],  # Will be None if not found
                'summary': data['summary']       # Will be None if not found
            })
            
            # Optional: Print progress
            if 'error' in data:
                print(f"Error processing {genus}: {data['error']}")
            else:
                print(f"Processed: {data['genus']}")
    
    print(f"\nData written to genus_data.csv for {len(genus_list)} genera")