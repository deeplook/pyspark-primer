"""
SQL interface — querying DataFrames with plain SQL via temporary views.

Any DataFrame can be registered as a temp view and then queried with
spark.sql(). Useful when SQL is more readable than the DataFrame API.
Demonstrates aggregations, filtering, and ordering on a small orders dataset.
"""
from pyspark.sql import SparkSession

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("sql").master("local[*]")).getOrCreate()

orders = spark.createDataFrame([
    (1, "Alice", "Widget",  3, 9.99),
    (2, "Bob",   "Gadget",  1, 24.99),
    (3, "Alice", "Widget",  2, 9.99),
    (4, "Carol", "Doohickey", 5, 4.49),
    (5, "Bob",   "Widget",  1, 9.99),
], ["order_id", "customer", "product", "qty", "unit_price"])

orders.createOrReplaceTempView("orders")

print("=== total spend per customer ===")
spark.sql("""
    SELECT customer, ROUND(SUM(qty * unit_price), 2) AS total_spend
    FROM orders
    GROUP BY customer
    ORDER BY total_spend DESC
""").show()

print("=== customers who bought Widget ===")
spark.sql("""
    SELECT DISTINCT customer
    FROM orders
    WHERE product = 'Widget'
""").show()

print("=== most popular product by qty ===")
spark.sql("""
    SELECT product, SUM(qty) AS total_qty
    FROM orders
    GROUP BY product
    ORDER BY total_qty DESC
    LIMIT 1
""").show()
