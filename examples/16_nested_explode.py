"""
Nested data and explode — flattening arrays and maps into rows.

explode() converts one row with an N-element array into N rows (one per
element) — essential for flattening nested JSON. posexplode() also yields
the array index. explode() on a map column produces one row per key-value
pair; map_keys() / map_values() extract them as arrays instead.
"""

from _spark_config import configure_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, map_keys, map_values, posexplode

spark = configure_spark(
    SparkSession.builder.appName("nested").master("local[*]")
).getOrCreate()

df = spark.createDataFrame(
    [
        (1, "Alice", [85, 92, 78], {"math": "A", "english": "B"}),
        (2, "Bob", [70, 88], {"math": "C", "english": "A"}),
        (3, "Carol", [95, 99, 91], {"math": "A", "english": "A"}),
    ],
    ["id", "name", "scores", "grades"],
)

print("=== original (array + map columns) ===")
df.printSchema()
df.show(truncate=False)

print("=== explode array: one row per score ===")
df.select("name", explode("scores").alias("score")).show()

print("=== posexplode: include array index ===")
df.select("name", posexplode("scores").alias("pos", "score")).show()

print("=== explode map: one row per key-value pair ===")
df.select("name", explode("grades").alias("subject", "grade")).show()

print("=== map_keys / map_values: keep as arrays ===")
df.select(
    col("name"),
    map_keys("grades").alias("subjects"),
    map_values("grades").alias("grade_vals"),
).show(truncate=False)
