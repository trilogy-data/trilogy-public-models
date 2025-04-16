import os
import json
import glob
import click
import tempfile
import filecmp
from pathlib import Path


@click.command()
@click.option("--check", is_flag=True, help="Check if any files would be changed")
def generate_json_files(check):
    """Generate JSON files for all datasets and optionally check if any changes would be made."""
    # Base directory where the script is running
    base_dir = Path(__file__).parent.parent

    # Path to trilogy_public_models directory
    public_models_dir = os.path.join(base_dir, "trilogy_public_models")

    # Path to examples directory
    examples_dir = os.path.join(base_dir, "examples")

    # Path to studio directory (where JSON files will be saved)
    studio_dir = os.path.join(base_dir, "studio")
    os.makedirs(studio_dir, exist_ok=True)

    # Track if any files would be modified
    files_changed = False

    # Get all engine directories (like bigquery, duckdb)
    for engine_dir in os.listdir(public_models_dir):
        if engine_dir.endswith("__pycache__"):
            continue
        engine_path = os.path.join(public_models_dir, engine_dir)

        if os.path.isdir(engine_path):
            # Get all dataset directories under each engine
            for dataset_dir in os.listdir(engine_path):
                if dataset_dir.endswith("__pycache__"):
                    continue
                dataset_path = os.path.join(engine_path, dataset_dir)

                if os.path.isdir(dataset_path):
                    # Create the JSON structure
                    json_data = {
                        "name": dataset_dir,
                        "engine": engine_dir,
                        "description": f"{dataset_dir} dataset for {engine_dir}",
                        "link": "",
                        "tags": [engine_dir],
                        "components": [],
                    }

                    # Add source components from trilogy_public_models
                    preql_files = glob.glob(os.path.join(dataset_path, "*.preql"))
                    for preql_file in preql_files:
                        file_name = os.path.basename(preql_file).replace(".preql", "")
                        if file_name == "entrypoint":
                            continue
                        github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/trilogy_public_models/{engine_dir}/{dataset_dir}/{file_name}.preql"

                        component = {
                            "url": github_path,
                            "name": file_name,
                            "alias": file_name,
                            "purpose": "source",
                        }
                        json_data["components"].append(component)
                    sql_files = glob.glob(os.path.join(dataset_path, "*.sql"))
                    for sql_file in sql_files:
                        file_name = os.path.basename(sql_file).replace(".sql", "")
                        if file_name == "entrypoint":
                            continue
                        github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/trilogy_public_models/{engine_dir}/{dataset_dir}/{file_name}.sql"

                        component = {
                            "url": github_path,
                            "name": file_name,
                            "alias": file_name,
                            "purpose": "setup",
                            "type": "sql",
                        }
                        json_data["components"].append(component)
                    # Add example components from examples directory if they exist
                    examples_dataset_path = os.path.join(
                        examples_dir, engine_dir, dataset_dir
                    )
                    if os.path.exists(examples_dataset_path):
                        example_files = glob.glob(
                            os.path.join(examples_dataset_path, "*.preql")
                        )
                        for example_file in example_files:
                            file_name = os.path.basename(example_file).replace(
                                ".preql", ""
                            )
                            github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/examples/{engine_dir}/{dataset_dir}/{file_name}.preql"

                            component = {
                                "name": file_name,
                                "url": github_path,
                                "purpose": "example",
                            }
                            json_data["components"].append(component)
                        dashboard_files = glob.glob(
                            os.path.join(examples_dataset_path, "*.json")
                        )
                        for dashboard_file in dashboard_files:
                            file_name = os.path.basename(dashboard_file).replace(
                                ".json", ""
                            )
                            github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/examples/{engine_dir}/{dataset_dir}/{file_name}.json"

                            component = {
                                "name": file_name,
                                "url": github_path,
                                "purpose": "dashboard",
                            }
                            json_data["components"].append(component)
                    # Get existing file data if it exists
                    json_file_name = f"{dataset_dir}.json"
                    json_file_path = os.path.join(studio_dir, json_file_name)
                    if os.path.exists(json_file_path):
                        with open(json_file_path, "r") as f:
                            current = json.load(f)
                            json_data["description"] = current["description"]
                            json_data["tags"] = current["tags"]
                            json_data["link"] = current["link"]

                    # Check if this would create changes
                    if check:
                        # Create a temporary file with the new content
                        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
                            json.dump(json_data, temp_file, indent=2)
                            temp_path = temp_file.name
                        
                        # Check if the file exists and is different
                        if not os.path.exists(json_file_path) or not filecmp.cmp(temp_path, json_file_path):
                            files_changed = True
                            print(f"File would change: {json_file_path}")
                        
                        # Clean up temp file
                        os.unlink(temp_path)
                    else:
                        # Write the JSON file
                        with open(json_file_path, "w") as f:
                            json.dump(json_data, f, indent=2)
                        print(f"Created {json_file_path}")

    # If running in check mode and files would change, exit with error
    if check and files_changed:
        raise click.ClickException(
            "Files would be changed. Please run the script without --check and commit the changes."
        )


if __name__ == "__main__":
    generate_json_files()