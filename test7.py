"""
Joins — combining two DataFrames on a shared key.

Covers inner, left, and full outer joins using a small employees/departments
dataset. Inner keeps only matched rows; left keeps all left-side rows (unmatched
right side becomes null); outer keeps everything from both sides.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("joins").master("local[*]").getOrCreate()

employees = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob",   20),
    (3, "Carol", 10),
    (4, "Dave",  99),  # no matching dept
], ["id", "name", "dept_id"])

departments = spark.createDataFrame([
    (10, "Engineering"),
    (20, "Marketing"),
    (30, "HR"),           # no matching employee
], ["dept_id", "dept_name"])

print("=== inner join ===")
employees.join(departments, on="dept_id", how="inner").show()

print("=== left join (keeps Dave, no dept) ===")
employees.join(departments, on="dept_id", how="left").show()

print("=== full outer join ===")
employees.join(departments, on="dept_id", how="outer").show()
