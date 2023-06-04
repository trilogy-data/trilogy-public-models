"""Adhoc script to upload AOE2 match data to the community dataset"""

from pathlib import Path
from tenacity import retry, wait_exponential, stop_after_attempt

# @retry(wait=wait_exponential(multiplier=1, min=4, max=600), stop=stop_after_attempt(7))
def upload(file_path: str):
    root = Path(file_path).stem
    from google.cloud import bigquery
    from google.cloud import storage

    # Construct a BigQuery client object.
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    gcs_path = f"uploads/{root}.parquet"
    gcs_uri = f"gs://preql_demo/{gcs_path}"

    bucket = storage_client.get_bucket("preql_demo")

    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(file_path, timeout=600)
    print(f"File {file_path} uploaded to {gcs_uri}.")

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"ttl-test-355422.aoe2.{root}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    # empty table
    # destination_table = bq_client.get_table(table_id)
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows to {table_id}.")


def upload_action_file(file_path, index, storage_client, bq_client):
    from google.cloud import bigquery

    gcs_path = f"uploads/{index}/match_actions.parquet"
    gcs_uri = f"gs://preql_demo/{gcs_path}"

    bucket = storage_client.get_bucket("preql_demo")

    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(file_path, timeout=600)
    print(f"File uploaded to {gcs_uri}.")
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "ttl-test-355422.aoe2.match_player_actions"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    # empty table
    # destination_table = bq_client.get_table(table_id)
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,

    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows to {table_id}.")


def load_match_actions():
    from google.cloud import bigquery
    from google.cloud import storage
    from pathlib import Path

    # Construct a BigQuery client object.
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    #have done from 17
    folders = (Path(__file__).parent.parent / "match_actions").iterdir()
    for folder in folders:
        idx = Path(folder).stem.split('=')[1]

        files = folder.iterdir()
        for file in files:
            path = Path(__file__) / "match_player_actions" / folder / file
            print(f'uploading {path}')
            upload_action_file(path, idx, storage_client, bq_client)


def main(load_dimensions=False, load_game_data=True):
    if load_dimensions:
        for file in [
            # r"C:\Users\ethan\coding_projects\trilogy-public-models\match_player_actions.parquet",
            # r"C:\Users\ethan\coding_projects\trilogy-public-models\match_players.parquet",
            # r"C:\Users\ethan\coding_projects\trilogy-public-models\matches.parquet",
            # r"C:\Users\ethan\coding_projects\trilogy-public-models\players.parquet",
            r"C:\Users\ethan\coding_projects\trilogy-public-models\unit_ids.parquet"
        ]:
            upload(file)
    if load_game_data:
        load_match_actions()

if __name__ == "__main__":
    main(load_game_data=False, load_dimensions=True)
