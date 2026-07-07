"""
Handling nulls — detecting, dropping, and filling missing values.

Nulls are common in real data and cause silent errors if ignored. Spark provides
na.drop() to remove rows, na.fill() to substitute defaults, and when/otherwise
for conditional replacement. Demonstrates all three approaches on a small
dataset with intentional gaps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from _spark_config import configure_spark

spark = configure_spark(
    SparkSession.builder.appName("nulls").master("local[*]")
).getOrCreate()

df = spark.createDataFrame(
    [
        (1, "Alice", 30, "eng"),
        (2, "Bob", None, "mkt"),
        (3, None, 25, None),
        (4, "Dave", None, "hr"),
        (5, "Eve", 28, "eng"),
    ],
    ["id", "name", "age", "dept"],
)

print("=== original (with nulls) ===")
df.show()

print("=== drop rows with ANY null ===")
df.na.drop().show()

print("=== drop rows where 'age' is null ===")
df.na.drop(subset=["age"]).show()

print("=== fill nulls with defaults ===")
df.na.fill({"age": 0, "name": "Unknown", "dept": "none"}).show()

print("=== replace with when/otherwise ===")
df.withColumn(
    "dept", when(col("dept").isNull(), "unassigned").otherwise(col("dept"))
).show()
