"""
Caching and persistence — avoiding repeated recomputation.

Spark recomputes a DataFrame from scratch for every action (lazy eval).
cache() stores it in memory after the first action so subsequent actions
skip recomputation. persist() gives finer control over storage level.
Always unpersist() when done to free executor memory.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("caching").master("local[*]").getOrCreate()

df = spark.range(2_000_000).withColumn("value", col("id") % 1000)
filtered = df.filter(col("value") > 500)

print("=== without cache: recomputed on every action ===")
t0 = time.time()
filtered.count()
filtered.count()
print(f"two counts, no cache:    {time.time() - t0:.2f}s")

cached = filtered.cache()

print("\n=== with cache: materialised on first action ===")
t0 = time.time()
cached.count()   # materialises
cached.count()   # served from memory
print(f"two counts, with cache:  {time.time() - t0:.2f}s")
cached.unpersist()

print("\n=== persist() with explicit storage level ===")
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()
print("storage level:", df.storageLevel)
df.unpersist()
print("after unpersist:", df.storageLevel)
