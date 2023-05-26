# requires openai
# and langchain

from typing import TYPE_CHECKING
from preql.core.models import (
    Datasource,
    ColumnAssignment,
    Environment,
    Concept,
    Metadata,
    Grain
)
from preql.core.enums import DataType, Purpose
from preql.parsing.render import render_environment
import re
from os.path import dirname
import os
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
        "FLOAT": DataType.FLOAT
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


def process_table(table, client: "bigquery.Client", keys:list[str]) -> Environment:
    environment = Environment()


    columns = []
    grain = []
    for c in table.schema:
        concepts = parse_column(c, keys=keys)
        if c.name in keys:
            grain.extend(concepts)
        for concept in concepts:
            environment.add_concept(concept, add_derived=False)
            assignment = ColumnAssignment(alias=c.name, concept=concept)
            columns.append(assignment)
    for concept in environment.concepts.values():
        if concept.purpose == Purpose.PROPERTY:
            concept.keys = grain
    datasource = Datasource(
        columns=columns, identifier=table.table_id, address=table.full_table_id.replace(':','.'),
        grain = Grain(components = grain)
    )

    environment.datasources[table.table_id] = datasource
    return environment


def parse_public_bigquery_project(dataset: str, write: bool):
    from google import auth
    from google.cloud import bigquery
    root = dirname(dirname(__file__))
    target = Path(root) / "bigquery" / dataset
    cred, project = auth.default()
    client = bigquery.Client(credentials=cred, project="bigquery-public-data")

    dataset_instance = client.get_dataset(
        dataset,
    )
    entrypoints = []
    for table_ref in client.list_tables(dataset=dataset_instance):
        table = client.get_table(table_ref)
        keys = get_table_keys(table) or []
        ds = process_table(table, client=client, keys=keys)
        snake = camel_to_snake(table.table_id)
        entrypoints.append(snake)
        if write:

            os.makedirs(target, exist_ok=True)
            path = target / (snake + ".preql")
            with open(path, "w") as f:
                f.write(render_environment(ds))
    if write:
        os.makedirs(target, exist_ok=True)
        init = '''from trilogy_public_models.inventory import parse_initial_models

model = parse_initial_models(__file__)
'''
        path = target / '__init__.py'
        with open(path, 'w') as f:
            f.write(init)
        entrypoint = target / 'entrypoint.preql'
        with open(entrypoint, 'w') as f:
            entrypoints = '\n'.join([f'import {z} as {z};' for z in entrypoints])
            f.write(entrypoints)

if __name__ == "__main__":
    parse_public_bigquery_project("thelook_ecommerce", write=True)
