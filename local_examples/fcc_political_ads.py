from preql import Dialects
from trilogy_public_models import models

env = models["bigquery.fcc_political_ads"]


executor = Dialects.BIGQUERY.default_executor(environment=env)

results = executor.execute_text(
    """

auto total_spend <- sum(content_info.gross_spend);


SELECT
content_info.candidate,
content_info.advertiser,
content_info.period_start,
total_spend
order by
total_spend desc

LIMIT 100;"""
)

for row in results[0].fetchall():
    print(row)
