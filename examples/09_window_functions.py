"""
Window functions — computing values relative to a group of rows without collapsing them.

Unlike groupBy (which reduces rows), window functions add a new column while
keeping every row. Demonstrates rank(), lag() (previous row's value), and a
running sum — all partitioned per person and ordered by month.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, lag, sum as spark_sum
from pyspark.sql.window import Window

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("windows").master("local[*]")).getOrCreate()

sales = spark.createDataFrame([
    ("Alice", "Jan", 200),
    ("Alice", "Feb", 150),
    ("Alice", "Mar", 300),
    ("Bob",   "Jan", 100),
    ("Bob",   "Feb", 250),
    ("Bob",   "Mar", 175),
], ["name", "month", "amount"])

by_person = Window.partitionBy("name").orderBy("month")

print("=== rank within each person ===")
sales.withColumn("rank", rank().over(by_person)).show()

print("=== lag: previous month's amount ===")
sales.withColumn("prev_amount", lag("amount", 1).over(by_person)).show()

print("=== running total per person ===")
sales.withColumn("running_total", spark_sum("amount").over(by_person)).show()
