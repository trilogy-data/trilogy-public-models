# requires openai
# and langchain

from os.path import dirname
from pathlib import Path


def parse_public_bigquery_project(dataset: str, write: bool):
    from google import auth
    from google.cloud import bigquery
    root = dirname(dirname(__file__))
    Path(root) / "bigquery" / dataset
    cred, project = auth.default()
    client = bigquery.Client(credentials=cred, project="bigquery-public-data")

    datasets = client.list_datasets()
    for dataset in datasets:
        tables= client.list_tables(dataset=dataset)
        for table in tables:
            table = client.get_table(table)
            modified = str(table.modified)
            if modified.startswith('2023'):
                print(table.full_table_id)
                print(table.modified)
if __name__ == "__main__":
    parse_public_bigquery_project("fcc_political_ads", write=True)
