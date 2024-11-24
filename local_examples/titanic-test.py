from os.path import dirname

nb_path = __file__
root_path = dirname(dirname(nb_path))
from sys import path
from os.path import dirname

path.insert(0, root_path)
print(root_path)
from trilogy_public_models import get_executor

executor = get_executor("duckdb.titanic")

QA_1 = """
select 
    passenger.last_name,
     count(passenger.id)->passenger_count ;
"""  # noqa: E501

results = executor.execute_text(QA_1)

for row in results[0].fetchall():
    print(row)
