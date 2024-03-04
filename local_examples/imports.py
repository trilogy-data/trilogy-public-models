from datetime import datetime


from sys import path
from os.path import dirname

nb_path = __file__
root_path = dirname(dirname(nb_path))
path.insert(0, r"C:\Users\ethan\coding_projects\trilogy-public-models")
path.insert(0, r"C:\Users\ethan\coding_projects\pypreql")
start = datetime.now()
from trilogy_public_models import models  # noqa: E402
from trilogy_public_models.bigquery import stack_overflow  # noqa: E402

print(type(models["bigquery.stack_overflow"]))

print(type(stack_overflow))

print(datetime.now() - start)
