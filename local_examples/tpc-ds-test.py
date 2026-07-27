from datetime import datetime, timezone
from os.path import dirname
from sys import path

nb_path = __file__
root_path = dirname(dirname(nb_path))

path.insert(0, root_path)
print(root_path)
from trilogy_public_models import get_executor

start = datetime.now(timezone.utc)
executor = get_executor("duckdb.tpc_ds", run_setup=True)


QA_1 = """
MERGE store_sales.customer.* into customer.*;
select 
    customer.id,
    customer.full_name,
    customer.birth_date,
    count(store_sales.ticket_number) as orders,
order by
    customer.full_name desc;
"""
print(datetime.now(timezone.utc) - start)
results = executor.execute_text(QA_1)

for row in results[0].fetchall():
    print(row)
