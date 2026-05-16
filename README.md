# Sparky — PySpark learning scripts

A collection of small, self-contained PySpark scripts for learning the Spark
DataFrame API on a local machine.

## Prerequisites

### 1. Java (Temurin 21)

Spark runs on the JVM, so Java must be installed first. The easiest way on
macOS is via Homebrew using the Temurin distribution (Eclipse's open-source
OpenJDK build):

```bash
brew install --cask temurin@21
```

Verify:

```bash
java -version
# openjdk version "21.x.x" ... Temurin-21
```

> **Why Temurin?** It is a stable, LTS-backed OpenJDK build with a Homebrew
> cask, which makes installation and upgrades straightforward. Spark 4.x
> supports Java 17 and 21.

### 2. uv (Python package manager)

[uv](https://github.com/astral-sh/uv) manages the Python version and
virtualenv. Install it via Homebrew:

```bash
brew install uv
```

### 3. Python 3.12

PySpark 4.x does not yet support Python 3.13+, so this project pins to 3.12
via a `.python-version` file. uv picks this up automatically:

```bash
uv python install 3.12
```

## Installation

Clone the repo and let uv create the virtualenv and install dependencies
(including PySpark itself):

```bash
uv sync
```

That's it — no separate `pip install pyspark` or `SPARK_HOME` setup needed.
PySpark 4.x bundles its own Spark distribution and unpacks it at runtime.

## Modules

Run any script with `uv run <file>`. They are designed to be read in order —
each one builds on concepts introduced by the previous.

| File | Topic |
|------|-------|
| `01_spark_session.py`    | SparkSession — the entry point to every Spark application |
| `02_dataframes.py`       | In-memory DataFrames — `createDataFrame`, `show`, `filter`, `groupBy` |
| `03_lazy_evaluation.py`  | Lazy evaluation — transformations vs actions, `explain()` |
| `04_column_functions.py` | Column expressions — string, math, date functions, `when`/`coalesce` |
| `05_reading_csv.py`      | Reading CSV — `inferSchema`, `printSchema`, basic aggregations |
| `06_aggregations.py`     | Aggregations — multi-function `agg`, `countDistinct`, `pivot` |
| `07_joins.py`            | Joins — inner, left, and full outer joins |
| `08_sql_interface.py`    | SQL interface — `createOrReplaceTempView`, `spark.sql()` |
| `09_window_functions.py` | Window functions — `rank`, `lag`, running totals |
| `10_parquet.py`          | Writing & reading Parquet — `partitionBy`, partition pruning |
| `11_null_handling.py`    | Handling nulls — `na.drop`, `na.fill`, `when`/`otherwise` |
| `12_udfs.py`             | User-Defined Functions (UDFs) — custom Python logic on columns |
| `13_partitioning.py`     | Partitioning & query plans — `repartition`, `coalesce`, `explain()` |
| `14_schemas.py`          | Explicit schemas — `StructType`, `StructField`, DDL strings |
| `15_reading_json.py`     | Reading JSON — nested structs, `ArrayType`, dot-notation access |
| `16_nested_explode.py`   | Nested data — `explode`, `posexplode`, `map_keys`/`map_values` |
| `17_deduplication.py`    | Deduplication — `distinct`, `dropDuplicates` by key |
| `18_set_operations.py`   | Set operations — `union`, `unionByName`, `intersect`, `subtract` |
| `19_caching.py`          | Caching — `cache`, `persist`, `unpersist`, storage levels |
| `20_broadcast_joins.py`  | Broadcast joins — eliminating shuffle for small lookup tables |
| `21_sampling.py`         | Train/test split — `randomSplit`, `sample`, bootstrapping |

## Running all tests

A pytest suite checks that every module runs without errors:

```bash
uv run pytest test_all.py -v
```

## Notes

- The `WARN NativeCodeLoader` message on startup is harmless — it just means
  the native Hadoop library isn't available, which is expected on macOS.
- `05_reading_csv.py` downloads `tips.csv` from GitHub on first run and saves
  it locally. Subsequent runs use the local copy.
- `10_parquet.py` writes Parquet output to `out/employees/`.
