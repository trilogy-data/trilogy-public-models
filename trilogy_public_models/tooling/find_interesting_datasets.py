# requires openai
# and langchain

from typing import TYPE_CHECKING
from preql.core.models import (
    Concept,
    Metadata
)
from preql.core.enums import DataType, Purpose
import re
from os.path import dirname
from pathlib import Path
import json


def camel_to_snake(name: str) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


if TYPE_CHECKING:
    from google.cloud import bigquery


def write_ds_file():
    pass



def get_table_keys(table:"bigquery.Table"):
    from langchain.llms import OpenAI

    llm = OpenAI(temperature=0.99)
    columns = '\n'.join([f'{c.name}:{c.description}' for c in table.schema])
    text = f"""Given a list of the following pairs of columns and descriptions for a SQL table, which column
or set of columns are the primary keys for the table?

output the answer as a lJSON ist of column names with quotes around them.
Example response:

- ["user_id", "order_id"]
- ["ssn"]
- ["customer_id"]
- ["date", "search_term"]

Columns are:
{columns}
answer:
    """
    results = llm(text)
    print(results)
    return json.loads(results)

def process_description(input):
    if not input:
        return None
    return ' '.join([x.strip() for x in input.split('\n')])



def parse_column(
    c: "bigquery.SchemaField", keys:list[str], parents: list | None = None
) -> list[Concept]:
    parents = []
    type_map = {
        "STRING": DataType.STRING,
        "INTEGER": DataType.INTEGER,
        "BOOLEAN": DataType.BOOL,
        "TIMESTAMP": DataType.TIMESTAMP,
    }
    if c.field_type == "RECORD":
        output = []
        for x in c.fields:
            output.extend(parse_column(x, keys=keys, parents= parents + [c.name]))
        return output
    purpose = Purpose.KEY
    if c.name in keys:
        purpose = Purpose.KEY
    else:
        purpose = Purpose.PROPERTY

    return [
        Concept(
            name=camel_to_snake(c.name),
            metadata=Metadata(description=process_description(c.description)),
            datatype=type_map[c.field_type],
            purpose=purpose,
        )
    ]


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
