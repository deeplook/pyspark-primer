"""
Lazy evaluation — transformations are recipes, actions execute them.

Spark has two kinds of operations:
  - Transformations (filter, select, withColumn, join, …) are *lazy*: they
    build up a logical plan but do nothing until an action is called.
  - Actions (show, count, collect, first, take, write) trigger execution.

This design lets Spark optimise the entire pipeline before running a single
task. Use explain() to inspect the plan; use count() / show() to execute it.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("lazy").master("local[*]")).getOrCreate()

df = spark.range(1_000_000)  # transformation — no work done yet

# Chaining more transformations — still no work done
big = (
    df.withColumn("doubled", col("id") * 2)
      .filter(col("doubled") > 100)
      .filter(col("doubled") < 900)
)

print("No computation has happened yet — we only have a plan.")
big.explain()   # prints the logical/physical plan

# --- Actions trigger execution ---
print(f"\ncount (action): {big.count()}")   # now Spark runs the job
print(f"first (action): {big.first()}")

print("\n=== common actions ===")
print("take(3)   →", big.take(3))
print("collect() → first 5 of", len(big.collect()), "rows (use sparingly on large data)")

print("\n=== transformation vs action cheat-sheet ===")
cheat = spark.createDataFrame([
    ("filter / where",      "transformation"),
    ("select",              "transformation"),
    ("withColumn",          "transformation"),
    ("groupBy … agg",       "transformation"),
    ("join",                "transformation"),
    ("show()",              "action"),
    ("count()",             "action"),
    ("collect()",           "action"),
    ("first() / take(n)",   "action"),
    ("write.parquet(…)",    "action"),
], ["operation", "kind"])
cheat.show(truncate=False)
