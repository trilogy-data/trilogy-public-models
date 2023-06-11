from preql import Dialects
from trilogy_public_models import models

env = models["bigquery.usa_names"]


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
