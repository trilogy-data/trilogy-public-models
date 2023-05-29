from preql import Dialects
from trilogy_public_models import models

env = models['bigquery.thelook_ecommerce']

'''SELECT oi.product_id as product_id, p.name as product_name, p.category as product_category, count(*) as num_of_orders
FROM `bigquery-public-data.thelook_ecommerce.products` as p 
JOIN `bigquery-public-data.thelook_ecommerce.order_items` as oi
ON p.id = oi.product_id
GROUP BY 1,2,3
ORDER BY num_of_orders DESC'''  # noqa: E501

executor = Dialects.BIGQUERY.default_executor(environment=env)
QA_1 = '''

auto num_of_orders <- count(orders.id);


SELECT
products.id,
products.name,
products.category,
num_of_orders
order by num_of_orders desc


LIMIT 100;'''
QA_2 = '''

auto order_price <- sum(order_items.sale_price) by orders.id;


SELECT
    users.id,
    users.first_name,
    users.last_name,
    avg(order_price) -> average_user_order_value
order by 
    average_user_order_value  desc


LIMIT 100;'''

QA_3 =  '''

key cancelled_orders <- filter orders.id where orders.status = 'Cancelled';
auto orders.id.cancelled_count <- count(cancelled_orders);

SELECT
    users.city,
    orders.id.cancelled_count / orders.id.count -> cancellation_rate,
    orders.id.cancelled_count,
    orders.id.count,
    orders.created_at.year,
WHERE
    (orders.created_at.year = 2020)
    and orders.id.count>10
ORDER BY
    cancellation_rate desc;
'''

results = executor.execute_text(QA_3)

for row in results[0].fetchall():
    print(row)