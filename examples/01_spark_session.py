"""
SparkSession basics — the entry point to every Spark application.

SparkSession is the object you create first in any Spark program. It manages
the connection to the cluster (or local engine) and gives you access to all
DataFrame and SQL functionality. This script verifies the setup is working
by generating a range of 1000 numbers and counting them.
"""
from pyspark.sql import SparkSession

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("test").master("local[*]")).getOrCreate()

count = spark.range(1000).count()

print(count)
