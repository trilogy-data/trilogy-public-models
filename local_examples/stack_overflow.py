from sys import path
from os.path import dirname

nb_path = __file__
root_path = dirname(dirname(nb_path))
from preql import Dialects  # noqa: E402
from trilogy_public_models import models  # noqa: E402

env = models["bigquery.usa_names"]

print(env.concepts["name_count"])

executor = Dialects.BIGQUERY.default_executor(environment=env)

results = executor.execute_text(
    """

key vermont_names <- filter name where state = 'VT';


SELECT

vermont_names,
name_count.sum,
year
where
year = 1950
order by name_count.sum desc

LIMIT 100;"""
)

for row in results[0].fetchall():
    print(row)
