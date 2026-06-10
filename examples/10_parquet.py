"""
Writing and reading data — persisting DataFrames to disk as Parquet files.

Parquet is the standard columnar format for Spark. Writing with partitionBy()
splits output into subdirectories by column value, which lets Spark skip
entire partitions when reading (partition pruning). Also shows how to read
back the full dataset or a single partition directory.
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("write-read").master("local[*]").getOrCreate()

df = spark.createDataFrame([
    ("Alice", "eng",  95000),
    ("Bob",   "mkt",  72000),
    ("Carol", "eng",  105000),
    ("Dave",  "mkt",  68000),
    ("Eve",   "hr",   61000),
], ["name", "dept", "salary"])

root_dir = Path(__file__).resolve().parent.parent
out_path = root_dir / "out" / "employees"

print("=== writing parquet partitioned by dept ===")
df.write.partitionBy("dept").mode("overwrite").parquet(str(out_path))

print("=== files written ===")
for root, dirs, files in os.walk(out_path):
    for f in files:
        print(os.path.join(root, f))

print("\n=== reading back ===")
spark.read.parquet(str(out_path)).orderBy("name").show()

print("=== reading only eng partition ===")
spark.read.parquet(str(out_path / "dept=eng")).show()
