"""
Train/test split and sampling — preparing data for machine learning.

randomSplit() divides a DataFrame into non-overlapping subsets by fraction —
the standard first step before training a model. sample() draws a random
fraction for quick exploration or bootstrapping. Both accept a seed so
results are reproducible across runs.
"""

from _spark_config import configure_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = configure_spark(
    SparkSession.builder.appName("sampling").master("local[*]")
).getOrCreate()

df = spark.range(1000).withColumn("value", col("id") * 3)

print("=== randomSplit: 80/20 train-test ===")
train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f"total: {df.count()}  train: {train.count()}  test: {test.count()}")

print("\n=== three-way split: 70/15/15 train/val/test ===")
train, val, test = df.randomSplit([0.7, 0.15, 0.15], seed=42)
print(f"train: {train.count()}  val: {val.count()}  test: {test.count()}")

print("\n=== sample: random 10% without replacement ===")
s = df.sample(fraction=0.1, seed=42)
print(f"sampled rows: {s.count()}")
s.show(5)

print("\n=== sample with replacement (bootstrapping) ===")
boot = df.sample(withReplacement=True, fraction=1.0, seed=42)
print(f"bootstrap size: {boot.count()}  (expect ~1000, with duplicates)")
