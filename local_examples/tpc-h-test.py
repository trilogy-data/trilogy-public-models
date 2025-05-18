from os.path import dirname

nb_path = __file__
root_path = dirname(dirname(nb_path))
from sys import path
from os.path import dirname

path.insert(0, root_path)
print(root_path)
from trilogy_public_models import get_executor
from datetime import datetime

start = datetime.now()
executor = get_executor("duckdb.tpc_h")


QA_1 = """
# trilogy models run on imports to reuse logic
import lineitem as line_item;

# you can define new concepts in line 
auto discounted_price <- line_item.extended_price * (1-line_item.discount); #the discounted price is off the extended privce
auto charge_price <- discounted_price * (1+line_item.tax); #charged price includes taxes

# use functions to define repeatable templatized logic
def part_percent_of_nation(x) -> sum(x) by line_item.part.name, line_item.supplier.nation.id / sum(x) by line_item.supplier.nation.id *100;


WHERE line_item.ship_date <= '1998-12-01'::date 
SELECT
    line_item.part.name,
    line_item.supplier.nation.name,
    sum(charge_price)-> total_charge_price,
    sum(discounted_price) -> total_discounted_price,
    # call functions with @
    @part_percent_of_nation(charge_price) as charge_price_percent_of_nation,
    @part_percent_of_nation(discounted_price) as discount_price_percent_of_nation
ORDER BY   
    total_charge_price desc limit 100
;


"""  # noqa: E501
print(datetime.now() - start)
results = executor.execute_text(QA_1)

for row in results[0].fetchall():
    print(row)
