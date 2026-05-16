"""
Reading CSV files — loading structured data from disk into a DataFrame.

spark.read.csv() infers column types automatically when inferSchema=True.
Demonstrates show(), printSchema(), and two groupBy() aggregations on the
classic tips dataset (~240 rows). Note: Spark cannot read directly from HTTP
URLs, so the file is downloaded locally first.
"""
import urllib.request
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# Download tips dataset locally (PySpark can't read HTTP URLs directly)
csv_path = "tips.csv"
urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv",
    csv_path,
)

tips = spark.read.csv(csv_path, header=True, inferSchema=True)

tips.show(5)           # display first 5 rows
tips.printSchema()     # show column types
tips.groupBy("day").count().show()
tips.groupBy("smoker").agg({"tip": "mean"}).show()
