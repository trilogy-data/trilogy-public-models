from preql import Dialects
from trilogy_public_models import models

env = models["bigquery.chicago_crime"]


executor = Dialects.BIGQUERY.default_executor(environment=env)

results = executor.execute_text(
    """

property armed_crime <- filter crime.unique_key where like(crime.description,'ARMED') is True;
auto armed_crime_count <- count(armed_crime);


SELECT
crime.year,
crime.primary_type,
armed_crime_count
order by
armed_crime_count desc

LIMIT 100;"""  # noqa: E501
)  # noqa: E501

for row in results[0].fetchall():
    print(row)
