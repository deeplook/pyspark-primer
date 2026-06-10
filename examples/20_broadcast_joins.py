"""
Broadcast joins — eliminating shuffle when one side is small.

A regular join shuffles both DataFrames so matching keys land on the same
executor (expensive for large tables). If one side is small (a lookup or
dimension table), broadcast() ships a full copy to every executor instead —
no shuffle, significantly faster. explain() shows BroadcastHashJoin vs
SortMergeJoin so you can confirm the plan is what you expect.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("broadcast").master("local[*]")).getOrCreate()

orders = spark.range(500_000).withColumn("dept_id", (col("id") % 5).cast("int"))

departments = spark.createDataFrame([
    (0, "Engineering"), (1, "Marketing"), (2, "HR"),
    (3, "Finance"),     (4, "Legal"),
], ["dept_id", "dept_name"])

print("=== regular join ===")
t0 = time.time()
orders.join(departments, on="dept_id").count()
print(f"time: {time.time() - t0:.2f}s")

print("\n=== broadcast join ===")
t0 = time.time()
orders.join(broadcast(departments), on="dept_id").count()
print(f"time: {time.time() - t0:.2f}s")

print("\n=== explain: look for BroadcastHashJoin ===")
orders.join(broadcast(departments), on="dept_id").explain()
