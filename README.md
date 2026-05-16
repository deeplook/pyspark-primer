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
| `test1.py`  | SparkSession — the entry point to every Spark application |
| `test2.py`  | In-memory DataFrames — `createDataFrame`, `show`, `filter`, `groupBy` |
| `test3.py`  | Lazy evaluation — transformations vs actions, `explain()` |
| `test4.py`  | Column expressions — string, math, date functions, `when`/`coalesce` |
| `test5.py`  | Reading CSV — `inferSchema`, `printSchema`, basic aggregations |
| `test6.py`  | Aggregations — multi-function `agg`, `countDistinct`, `pivot` |
| `test7.py`  | Joins — inner, left, and full outer joins |
| `test8.py`  | SQL interface — `createOrReplaceTempView`, `spark.sql()` |
| `test9.py`  | Window functions — `rank`, `lag`, running totals |
| `test10.py` | Writing & reading Parquet — `partitionBy`, partition pruning |
| `test11.py` | Handling nulls — `na.drop`, `na.fill`, `when`/`otherwise` |
| `test12.py` | User-Defined Functions (UDFs) — custom Python logic on columns |
| `test13.py` | Partitioning & query plans — `repartition`, `coalesce`, `explain()` |

## Running all tests

A pytest suite checks that every module runs without errors:

```bash
uv run pytest test_all.py -v
```

## Notes

- The `WARN NativeCodeLoader` message on startup is harmless — it just means
  the native Hadoop library isn't available, which is expected on macOS.
- `test5.py` downloads `tips.csv` from GitHub on first run and saves it
  locally. Subsequent runs use the local copy.
- `test10.py` writes Parquet output to `out/employees/`.
