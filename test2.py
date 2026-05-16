"""
In-memory DataFrames — creating and transforming data without any files.

The quickest way to experiment with Spark is to build a DataFrame directly
from a Python list. Covers createDataFrame(), show(), printSchema(), filter()
to select rows by condition, and groupBy() with count() to aggregate.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()
df.printSchema()

from pyspark.sql.functions import col

df.filter(col("age") > 28).show()
df.groupBy("age").count().show()
