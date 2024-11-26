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
from datetime import datetime
start = datetime.now()
executor = get_executor("duckdb.tpc_ds", run_setup=True)


QA_1 = """
select 
    customer.full_name,
    count(store_sales.ticket_number) as orders,
order by
    customer.full_name desc;
"""  # noqa: E501
print(datetime.now()-start)
results = executor.execute_text(QA_1)

for row in results[0].fetchall():
    print(row)