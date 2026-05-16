"""
Aggregations — summarising data with groupBy, agg, and pivot.

groupBy().agg() lets you apply multiple aggregation functions in a single
pass. countDistinct() counts unique values. pivot() rotates row values into
columns — useful for cross-tabulation. These are all executed on the JVM
with no Python serialisation overhead.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct,
    avg, sum as spark_sum, min as spark_min, max as spark_max,
    round as spark_round, stddev,
)

spark = SparkSession.builder.appName("aggregations").master("local[*]").getOrCreate()

sales = spark.createDataFrame([
    ("Alice", "eng",  "Widget",   3,  9.99),
    ("Bob",   "mkt",  "Gadget",   1, 24.99),
    ("Alice", "eng",  "Widget",   2,  9.99),
    ("Carol", "hr",   "Doohickey",5,  4.49),
    ("Bob",   "mkt",  "Widget",   1,  9.99),
    ("Carol", "hr",   "Gadget",   2, 24.99),
    ("Alice", "eng",  "Gadget",   1, 24.99),
    ("Dave",  "mkt",  "Doohickey",3,  4.49),
], ["rep", "dept", "product", "qty", "unit_price"])

sales = sales.withColumn("revenue", col("qty") * col("unit_price"))

print("=== multi-function agg per department ===")
sales.groupBy("dept").agg(
    count("*").alias("num_sales"),
    countDistinct("rep").alias("unique_reps"),
    spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
    spark_round(avg("revenue"), 2).alias("avg_revenue"),
    spark_round(stddev("revenue"), 2).alias("stddev_revenue"),
    spark_min("revenue").alias("min_sale"),
    spark_max("revenue").alias("max_sale"),
).orderBy("dept").show()

print("=== agg per rep ===")
sales.groupBy("rep").agg(
    spark_sum("qty").alias("total_units"),
    spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
).orderBy(col("total_revenue").desc()).show()

print("=== pivot: revenue per dept per product ===")
sales.groupBy("dept") \
     .pivot("product", ["Doohickey", "Gadget", "Widget"]) \
     .agg(spark_round(spark_sum("revenue"), 2)) \
     .orderBy("dept") \
     .show()

print("=== countDistinct across the whole dataset ===")
sales.select(
    count("*").alias("total_rows"),
    countDistinct("rep").alias("unique_reps"),
    countDistinct("product").alias("unique_products"),
).show()
