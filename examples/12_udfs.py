"""
User-Defined Functions (UDFs) — applying custom Python logic to DataFrame columns.

UDFs let you use arbitrary Python when built-in Spark functions aren't enough.
They are slower than native functions (data serialises between JVM and Python),
so prefer built-ins where possible. Demonstrates extracting an email domain
and converting a numeric score to a letter grade.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("udfs").master("local[*]")).getOrCreate()

df = spark.createDataFrame([
    ("alice@example.com",  82),
    ("bob@work.org",       91),
    ("carol@example.com",  55),
    ("dave@school.edu",    74),
], ["email", "score"])

@udf(StringType())
def email_domain(email: str) -> str:
    return email.split("@")[-1] if email else None

@udf(StringType())
def grade(score: int) -> str:
    if score is None:
        return None
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    return "F"

print("=== extract domain + assign grade ===")
df.withColumn("domain", email_domain(col("email"))) \
  .withColumn("grade",  grade(col("score"))) \
  .show()
