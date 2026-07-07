"""
Set operations — combining and comparing DataFrames like SQL set algebra.

union() stacks rows from two DataFrames (duplicates kept); add .distinct()
for a true set union. unionByName() matches columns by name rather than
position — safer when schemas may differ in column order.
intersect() returns rows common to both; subtract() returns rows in the
left side that do not appear on the right.
"""

from pyspark.sql import SparkSession

from _spark_config import configure_spark

spark = configure_spark(
    SparkSession.builder.appName("set-ops").master("local[*]")
).getOrCreate()

q1 = spark.createDataFrame(
    [
        ("Alice", "eng"),
        ("Bob", "mkt"),
        ("Carol", "eng"),
    ],
    ["name", "dept"],
)

q2 = spark.createDataFrame(
    [
        ("Bob", "mkt"),
        ("Dave", "hr"),
        ("Alice", "eng"),
    ],
    ["name", "dept"],
)

print("=== union (keeps duplicates) ===")
q1.union(q2).show()

print("=== union + distinct (true set union) ===")
q1.union(q2).distinct().show()

print("=== intersect: rows in both ===")
q1.intersect(q2).show()

print("=== subtract: in Q1 but not Q2 ===")
q1.subtract(q2).show()

print("=== unionByName: safe when column order differs ===")
q2_reordered = spark.createDataFrame(
    [
        ("mkt", "Bob"),
        ("hr", "Dave"),
    ],
    ["dept", "name"],
)  # swapped column order
q1.unionByName(q2_reordered).distinct().show()
