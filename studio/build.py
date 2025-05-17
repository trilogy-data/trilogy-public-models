import os
import json
import glob
import click
from pathlib import Path


@click.command()
@click.option("--check", is_flag=True, help="Check if any files would be changed")
def generate_json_files(check:bool):
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
                    for preql_file in sorted(preql_files):
                        file_name = os.path.basename(preql_file).replace(".preql", "")
                        if file_name == "entrypoint":
                            continue
                        github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/trilogy_public_models/{engine_dir}/{dataset_dir}/{file_name}.preql"

                        component = {
                            "url": github_path,
                            "name": file_name,
                            "alias": file_name,
                            "purpose": "source",
                            "type": "trilogy",
                        }
                        json_data["components"].append(component)
                    sql_files = glob.glob(os.path.join(dataset_path, "*.sql"))
                    for sql_file in sorted(sql_files):
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
                        for example_file in sorted(example_files):
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
                        for dashboard_file in sorted(dashboard_files):
                            file_name = os.path.basename(dashboard_file).replace(
                                ".json", ""
                            )
                            github_path = f"https://raw.githubusercontent.com/trilogy-data/trilogy-public-models/refs/heads/main/examples/{engine_dir}/{dataset_dir}/{file_name}.json"

                            component = {
                                "name": file_name,
                                "url": github_path,
                                "purpose": "example",
                                "type": "dashboard",
                            }
                            json_data["components"].append(component)
                    # Get existing file data if it exists
                    json_file_name = f"{dataset_dir}.json"
                    json_file_path = os.path.join(studio_dir, json_file_name)
                    if os.path.exists(json_file_path):
                        try:
                            with open(json_file_path, "r", newline="") as f:
                                current = json.load(f)
                                json_data["description"] = current["description"]
                                json_data["tags"] = current["tags"]
                                json_data["link"] = current["link"]
                                json_data["components"] = sorted(
                                    json_data["components"], key=lambda x: x["name"]
                                )
                        except json.JSONDecodeError:
                            print(
                                f"Warning: Could not parse existing file {json_file_path}"
                            )

                    # Ensure deterministic JSON output - sort keys and use consistent separators
                    json_output = json.dumps(
                        json_data, indent=2, sort_keys=True, separators=(",", ": ")
                    )

                    # Check if this would create changes
                    if check:
                        current_content = ""
                        if os.path.exists(json_file_path):
                            try:
                                with open(json_file_path, "r", newline="") as f:
                                    base = json.load(f)
                                    base["components"] = sorted(
                                        base["components"], key=lambda x: x["name"]
                                    )
                                    current_content = json.dumps(
                                        base,
                                        indent=2,
                                        sort_keys=True,
                                        separators=(",", ": "),
                                    )
                            except json.JSONDecodeError:
                                # If we can't parse the current file, we'll count it as a change
                                files_changed = True
                                print(
                                    f"File would change (current file not parsable): {json_file_path}"
                                )
                                continue

                        # Compare the normalized JSON strings instead of files
                        if not os.path.exists(json_file_path):
                            files_changed = True
                            print(f"File would be created: {json_file_path}")
                        elif current_content != json_output:
                            files_changed = True
                            print(f"File would change: {json_file_path}")

                            # Debug: Show the differences
                            print("--- Differences detected ---")
                            if len(current_content) != len(json_output):
                                print(
                                    f"Length difference: current={len(current_content)} vs new={len(json_output)}"
                                )

                            # Find and print the first few differences
                            diff_count = 0
                            for i, (c1, c2) in enumerate(
                                zip(current_content, json_output)
                            ):
                                if c1 != c2:
                                    diff_count += 1
                                    print(f"Diff at position {i}: '{c1}' vs '{c2}'")

                                    # Print context around difference
                                    start = max(0, i - 10)
                                    end = min(len(current_content), i + 10)
                                    print(
                                        f"Current context: '{current_content[start:end]}'"
                                    )
                                    print(f"New context: '{json_output[start:end]}'")

                                    if diff_count >= 3:  # Show only first 3 differences
                                        break

                            # Check for trailing content if lengths differ
                            if len(current_content) != len(json_output):
                                if len(current_content) > len(json_output):
                                    print(
                                        f"Current file has {len(current_content) - len(json_output)} extra characters"
                                    )
                                    print(
                                        f"Extra trailing content: '{current_content[len(json_output):len(json_output)+20]}...'"
                                    )
                                else:
                                    print(
                                        f"New output has {len(json_output) - len(current_content)} extra characters"
                                    )
                                    print(
                                        f"Extra trailing content: '{json_output[len(current_content):len(current_content)+20]}...'"
                                    )
                            print("------------------------")
                        else:
                            print(f"No changes detected for {json_file_path}")
                    else:
                        # Write the JSON file with LF line endings
                        with open(json_file_path, "w", newline="\n") as f:
                            f.write(json_output)
                        print(f"Created {json_file_path}")

    # copy tpc_h.json output as demo-model.json with the name updated
    demo_model_path = os.path.join(studio_dir, "demo-model.json")
    tpc_h_path = os.path.join(studio_dir, "tpc_h.json")
    if os.path.exists(tpc_h_path):
        with open(tpc_h_path, "r", newline="") as f:
            tpc_h_data = json.load(f)
            tpc_h_data["name"] = "demo-model"
            tpc_h_data["description"] = "The demo model used in Studio tutorials"
            tpc_h_data["tags"] = [*tpc_h_data["tags"], "demo"]
            demo_model_output = json.dumps(
                tpc_h_data, indent=2, sort_keys=True, separators=(",", ": ")
            )
        with open(demo_model_path, "w", newline="\n") as f:
            f.write(demo_model_output)
        print(f"Created {demo_model_path}")

    # If running in check mode and files would change, exit with error
    if check and files_changed:
        raise click.ClickException(
            "Files would be changed. Please run the script without --check and commit the changes."
        )


if __name__ == "__main__":
    generate_json_files()
