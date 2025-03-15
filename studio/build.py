import os
import json
import glob
from pathlib import Path

def generate_json_files():
    # Base directory where the script is running
    base_dir = Path(__file__).parent.parent
    
    # Path to trilogy_public_models directory
    public_models_dir = os.path.join(base_dir, 'trilogy_public_models')
    
    # Path to examples directory
    examples_dir = os.path.join(base_dir, 'examples')
    
    # Path to studio directory (where JSON files will be saved)
    studio_dir = os.path.join(base_dir, 'studio')
    os.makedirs(studio_dir, exist_ok=True)
    
    # Get all engine directories (like bigquery, duckdb)
    for engine_dir in os.listdir(public_models_dir):
        if engine_dir.endswith('__pycache__'):
            continue
        engine_path = os.path.join(public_models_dir, engine_dir)
        
        if os.path.isdir(engine_path):
            # Get all dataset directories under each engine
            for dataset_dir in os.listdir(engine_path):
                if dataset_dir.endswith('__pycache__'):
                    continue
                dataset_path = os.path.join(engine_path, dataset_dir)
                
                if os.path.isdir(dataset_path):
                    # Create the JSON structure
                    json_data = {
                        "name": dataset_dir,
                        "engine": engine_dir,
                        "description": f"{dataset_dir} dataset for {engine_dir}",
                        "link": "",
                        "tags": [
                            engine_dir
                        ],
                        "components": []
                    }
                    
                    # Add source components from trilogy_public_models
                    preql_files = glob.glob(os.path.join(dataset_path, "*.preql"))
                    for preql_file in preql_files:
                        file_name = os.path.basename(preql_file).replace(".preql", "")
                        if file_name == 'entrypoint':
                            continue
                        github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/trilogy_public_models/{engine_dir}/{dataset_dir}/{file_name}.preql"
                        
                        component = {
                            "url": github_path,
                            "name": file_name,
                            "alias": file_name,
                            "purpose": "source"
                        }
                        json_data["components"].append(component)
                    
                    # Add example components from examples directory if they exist
                    examples_dataset_path = os.path.join(examples_dir, engine_dir, dataset_dir)
                    if os.path.exists(examples_dataset_path):
                        example_files = glob.glob(os.path.join(examples_dataset_path, "*.preql"))
                        for example_file in example_files:
                            file_name = os.path.basename(example_file).replace(".preql", "")
                            github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/examples/{engine_dir}/{dataset_dir}/{file_name}.preql"
                            
                            component = {
                                "name": file_name,
                                "url": github_path,
                                "purpose": "example"
                            }
                            json_data["components"].append(component)
                    
                    # Write the JSON file
                    json_file_name = f"{dataset_dir}.json"
                    json_file_path = os.path.join(studio_dir, json_file_name)
                    if os.path.exists(json_file_path):
                        with open(json_file_path, 'r') as f:
                            current = json.load(f)
                            json_data["description"] = current["description"]
                            json_data["tags"] = current["tags"]
                            json_data["link"] = current["link"]
                            
                    with open(json_file_path, 'w') as f:
                        json.dump(json_data, f, indent=2)
                    
                    print(f"Created {json_file_path}")

if __name__ == "__main__":
    generate_json_files()