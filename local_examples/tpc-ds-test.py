from os.path import dirname

nb_path = __file__
root_path = dirname(dirname(nb_path))
from sys import path
from os.path import dirname

path.insert(0, root_path)
print(root_path)
from trilogy_public_models import models
from trilogy_public_models import get_executor

env = models["duckdb.tpc-ds"]

executor = get_executor("duckdb.tpc_ds")


QA_1 ="""
select store_sales.date.year, count(store_sales.customer.id)->customer_count
order by store_sales.date.year desc 
"""  # noqa: E501


results = executor.execute_text(QA_1)

for row in results[0].fetchall():
    print(row)
