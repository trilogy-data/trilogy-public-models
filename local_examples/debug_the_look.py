from trilogy_public_models import models
from preql import Dialects
from preql.hooks.query_debugger import DebuggingHook


environment = models["bigquery.usa_names"]
executor = Dialects.BIGQUERY.default_executor(
    environment=environment, hooks=[DebuggingHook()]
)


text = """
key vermont_names <- filter name where state = 'VT';


SELECT
name_count.sum,
year
where
year = 1950

LIMIT 100;"""
results = executor.execute_text(text)
for row in results:
    answers = row.fetchall()
    for x in answers:
        print(x)
