"""
Explicit schemas — defining column types upfront with StructType.

inferSchema=True is convenient but expensive: Spark must scan the data twice.
Defining a schema by hand is faster, type-safe, and documents intent. It also
lets you mark columns nullable=False, which the optimiser can exploit.
A DDL string ("id INT NOT NULL, name STRING") is a terser alternative.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, BooleanType,
)

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("schemas").master("local[*]")).getOrCreate()

schema = StructType([
    StructField("id",     IntegerType(), nullable=False),
    StructField("name",   StringType(),  nullable=True),
    StructField("salary", DoubleType(),  nullable=True),
    StructField("active", BooleanType(), nullable=False),
])

data = [
    (1, "Alice",  95000.0, True),
    (2, "Bob",    72000.0, False),
    (3, "Carol", 105000.0, True),
    (4, "Dave",   None,    True),
]

df = spark.createDataFrame(data, schema)

print("=== schema (defined, not inferred) ===")
df.printSchema()
df.show()

print("=== DDL string: terser alternative ===")
ddl = "id INT NOT NULL, name STRING, salary DOUBLE, active BOOLEAN NOT NULL"
df2 = spark.createDataFrame(data, ddl)
df2.printSchema()
