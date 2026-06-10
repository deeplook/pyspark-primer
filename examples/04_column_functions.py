"""
Column expressions and built-in functions — transforming data without UDFs.

pyspark.sql.functions contains hundreds of native functions that run on the
JVM and are far faster than Python UDFs. This script walks through the most
common categories: column selection, string ops, math, dates, casting, and
conditional logic.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit,
    upper, lower, length, substring, concat, trim,
    round as spark_round, abs as spark_abs, sqrt, pow as spark_pow,
    current_date, date_format, datediff, to_date,
    when, coalesce,
)
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("functions").master("local[*]").getOrCreate()

people = spark.createDataFrame([
    (1, "  Alice  ", "1990-06-15", 82500.456),
    (2, "Bob",       "1985-11-30", 67000.0),
    (3, "carol",     "2000-03-08", 91250.789),
    (4, "Dave",      "1978-09-22", None),
], ["id", "name", "birthdate", "salary"])

print("=== string functions ===")
people.select(
    col("name"),
    trim(col("name")).alias("trimmed"),
    upper(trim(col("name"))).alias("upper"),
    lower(trim(col("name"))).alias("lower"),
    length(trim(col("name"))).alias("name_len"),
    concat(lit("Hello, "), trim(col("name"))).alias("greeting"),
    substring(trim(col("name")), 1, 3).alias("first3"),
).show()

print("=== math functions ===")
people.select(
    col("salary"),
    spark_round(col("salary"), 0).alias("rounded"),
    spark_abs(col("salary") - lit(80000)).alias("delta_from_80k"),
    sqrt(col("salary")).alias("sqrt_salary"),
).show()

print("=== date functions ===")
people.select(
    to_date(col("birthdate")).alias("birthdate"),
    current_date().alias("today"),
    datediff(current_date(), to_date(col("birthdate"))).alias("days_alive"),
    date_format(to_date(col("birthdate")), "MMM yyyy").alias("formatted"),
).show()

print("=== casting and withColumn ===")
people.withColumn("salary_int", col("salary").cast("int")) \
      .withColumn("id_str",     col("id").cast("string")) \
      .select("id", "id_str", "salary", "salary_int") \
      .show()

print("=== conditional logic: when / otherwise / coalesce ===")
people.withColumn(
    "band",
    when(col("salary") >= 90000, "high")
    .when(col("salary") >= 70000, "mid")
    .when(col("salary").isNotNull(), "low")
    .otherwise("unknown")
).withColumn(
    "salary_safe",
    coalesce(col("salary"), lit(0.0))
).show()
