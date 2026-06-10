"""
Deduplication — removing duplicate rows with distinct and dropDuplicates.

distinct() removes rows that are identical across every column.
dropDuplicates(subset=[...]) deduplicates on a specific key while keeping
the first occurrence and preserving all other columns — the more useful
form in ETL pipelines where a surrogate id or timestamp differs per copy.
"""
from pyspark.sql import SparkSession

from _spark_config import configure_spark

spark = configure_spark(SparkSession.builder.appName("dedup").master("local[*]")).getOrCreate()

df = spark.createDataFrame([
    (1, "Alice", "eng",  95000),
    (2, "Bob",   "mkt",  72000),
    (1, "Alice", "eng",  95000),  # exact duplicate of row 1
    (3, "Alice", "eng",  98000),  # same name+dept, different id & salary
    (4, "Bob",   "mkt",  72000),  # same name+dept+salary, different id
], ["id", "name", "dept", "salary"])

print("=== original (5 rows, 2 exact duplicates) ===")
df.show()

print("=== distinct(): removes fully identical rows ===")
df.distinct().show()

print("=== dropDuplicates on name+dept: first occurrence per person/dept ===")
df.dropDuplicates(["name", "dept"]).show()

print("=== counts ===")
spark.createDataFrame([(
    df.count(),
    df.distinct().count(),
    df.dropDuplicates(["name", "dept"]).count(),
)], ["original", "after_distinct", "after_dedup_by_key"]).show()
