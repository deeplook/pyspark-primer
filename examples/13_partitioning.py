"""
Partitioning and query plans — understanding how Spark distributes data.

Spark splits data into partitions processed in parallel. repartition() reshuffles
data (full network shuffle), coalesce() merges partitions cheaply (no shuffle).
Partitioning by a column co-locates related rows, speeding up joins and
aggregations on that column. explain() prints the physical query plan so you
can see what Spark will actually do before it runs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id

from _spark_config import configure_spark

spark = configure_spark(
    SparkSession.builder.appName("partitioning").master("local[*]")
).getOrCreate()

df = spark.range(100).withColumn("value", col("id") * 3)

print(f"default partitions: {df.rdd.getNumPartitions()}")

df4 = df.repartition(4)
print(f"after repartition(4): {df4.rdd.getNumPartitions()}")

print("=== rows per partition ===")
df4.groupBy(spark_partition_id().alias("partition")).count().orderBy("partition").show()

print("=== coalesce to 2 (no shuffle) ===")
df2 = df4.coalesce(2)
print(f"after coalesce(2): {df2.rdd.getNumPartitions()}")

print("=== repartition by column (each unique value -> own partition bucket) ===")
df_col = spark.createDataFrame(
    [
        ("eng", "Alice"),
        ("mkt", "Bob"),
        ("eng", "Carol"),
        ("hr", "Dave"),
        ("mkt", "Eve"),
        ("hr", "Frank"),
    ],
    ["dept", "name"],
)

df_col_part = df_col.repartition(3, "dept")
df_col_part.withColumn("partition", spark_partition_id()).orderBy("dept", "name").show()

print("\n=== explain() — physical plan for a filter ===")
df.filter(col("value") > 50).explain()
