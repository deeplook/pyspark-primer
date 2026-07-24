"""
Reading JSON — loading semi-structured data with automatic schema inference.

spark.read.json() expects newline-delimited JSON by default (one object per
line). It infers a merged schema across all records in one pass: nested
objects become StructType columns, arrays become ArrayType columns.
Nested fields are accessed with dot notation: col("address.city").
"""

import json
from pathlib import Path

from _spark_config import configure_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = configure_spark(
    SparkSession.builder.appName("json").master("local[*]")
).getOrCreate()

records = [
    {
        "id": 1,
        "name": "Alice",
        "scores": [85, 92, 78],
        "address": {"city": "NYC", "zip": "10001"},
    },
    {
        "id": 2,
        "name": "Bob",
        "scores": [70, 88],
        "address": {"city": "LA", "zip": "90001"},
    },
    {
        "id": 3,
        "name": "Carol",
        "scores": [95, 99, 91],
        "address": {"city": "NYC", "zip": "10002"},
    },
]

root_dir = Path(__file__).resolve().parent.parent
json_path = root_dir / "out" / "people.jsonl"
json_path.parent.mkdir(exist_ok=True)
with open(json_path, "w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

df = spark.read.json(str(json_path))

print("=== inferred schema (nested struct + array) ===")
df.printSchema()
df.show(truncate=False)

print("=== accessing nested struct fields with dot notation ===")
df.select(
    col("name"),
    col("address.city").alias("city"),
    col("address.zip").alias("zip"),
).show()
