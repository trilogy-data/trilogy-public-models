from datetime import datetime, timezone
from os.path import dirname
from sys import path

nb_path = __file__
root_path = dirname(dirname(nb_path))
path.insert(0, r"C:\Users\ethan\coding_projects\trilogy-public-models")

start = datetime.now(timezone.utc)
from trilogy_public_models import models
from trilogy_public_models.bigquery import stack_overflow

print(type(models["bigquery.stack_overflow"]))


print(type(stack_overflow))
print(len(models["bigquery.stack_overflow"].concepts))
assert len(stack_overflow.concepts) == len(
    models["bigquery.stack_overflow"].concepts
), f"Expected {len(models['bigquery.stack_overflow'].concepts)} concepts, got {len(stack_overflow.concepts)} concepts."

print(datetime.now(timezone.utc) - start)
