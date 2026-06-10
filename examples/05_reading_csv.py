"""
Reading CSV files — loading structured data from disk into a DataFrame.

spark.read.csv() infers column types automatically when inferSchema=True.
Demonstrates show(), printSchema(), and two groupBy() aggregations on the
classic tips dataset (~240 rows). Note: Spark cannot read directly from HTTP
URLs, so the dataset is included under data/.
"""
from pathlib import Path

from pyspark.sql import SparkSession

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("test").master("local[*]")).getOrCreate()

root_dir = Path(__file__).resolve().parent.parent
csv_path = root_dir / "data" / "tips.csv"

tips = spark.read.csv(str(csv_path), header=True, inferSchema=True)

tips.show(5)           # display first 5 rows
tips.printSchema()     # show column types
tips.groupBy("day").count().show()
tips.groupBy("smoker").agg({"tip": "mean"}).show()
