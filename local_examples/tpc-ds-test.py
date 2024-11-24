from os.path import dirname
from pathlib import Path
nb_path = __file__
root_path = dirname(dirname(nb_path))
from sys import path
from os.path import dirname
from json import dump

path.insert(0, root_path)
print(root_path)
from trilogy_public_models import get_executor

executor = get_executor("duckdb.tpc_ds", run_setup=True)

QA_1 = """
select 
    store_sales.date.year, 
    count(store_sales.customer.id)->customer_count
order by 
    store_sales.date.year desc ;
"""  # noqa: E501

results = executor.execute_text(QA_1)
concepts = []
for x in executor.environment.concepts:
    if any(y.startswith('_') for y in x.split('.')):
        continue
    concepts.append(x)
with open(Path(__file__).parent / 'concepts.txt', 'w') as f:
    dump(concepts, f, indent=4)
