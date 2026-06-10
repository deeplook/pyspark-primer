# PySpark-Primer

[![CI](https://github.com/deeplook/pyspark-primer/actions/workflows/ci.yml/badge.svg)](https://github.com/deeplook/pyspark-primer/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/deeplook/pyspark-primer)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/downloads/release/python-3120/)

A collection of small, self-contained PySpark scripts for learning the Spark
DataFrame API on a local machine.

## Prerequisites

> **Platform support:** This project is developed on macOS and tested on Linux
> by GitHub Actions. The Windows instructions below follow the official Java
> and uv installation guidance, but Windows support has not been verified.

### 1. Java (Temurin 21)

Spark runs on the JVM, so Java must be installed first. This project uses
[Eclipse Temurin](https://adoptium.net/installation/), an open-source OpenJDK
distribution. Spark 4.x supports Java 17 and 21; the CI suite runs on Java 21.

#### macOS

```bash
brew install --cask temurin@21
```

#### Linux

Install Temurin 21 using the
[Adoptium packages for your distribution](https://adoptium.net/installation/linux/).
Packages are available for Debian/Ubuntu, Fedora/RHEL, Alpine, and other
common distributions.

#### Windows

Download and run the
[Temurin 21 Windows MSI installer](https://adoptium.net/installation/windows/).
Enable the installer options that add Java to `PATH` and set `JAVA_HOME`.

Verify the installation on any platform:

```bash
java -version
# openjdk version "21.x.x" ... Temurin-21
```

### 2. uv (Python package manager)

[uv](https://github.com/astral-sh/uv) manages the Python version and
virtualenv.

#### macOS

```bash
brew install uv
```

#### Linux

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Restart your shell after installation so that `uv` is available on `PATH`.

#### Windows

Install with WinGet from PowerShell:

```powershell
winget install --id=astral-sh.uv -e
```

See the [official uv installation guide](https://docs.astral.sh/uv/getting-started/installation/)
for alternative package managers and standalone installers.

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
The same `uv sync`, `uv run examples/<file>`, and test commands below work in
macOS/Linux shells and Windows PowerShell.

## Modules

Run any script with `uv run examples/<file>`. They are designed to be read in
order — each one builds on concepts introduced by the previous.

| File | Topic |
|------|-------|
| `examples/01_spark_session.py`    | SparkSession — the entry point to every Spark application |
| `examples/02_dataframes.py`       | In-memory DataFrames — `createDataFrame`, `show`, `filter`, `groupBy` |
| `examples/03_lazy_evaluation.py`  | Lazy evaluation — transformations vs actions, `explain()` |
| `examples/04_column_functions.py` | Column expressions — string, math, date functions, `when`/`coalesce` |
| `examples/05_reading_csv.py`      | Reading CSV — `inferSchema`, `printSchema`, basic aggregations |
| `examples/06_aggregations.py`     | Aggregations — multi-function `agg`, `countDistinct`, `pivot` |
| `examples/07_joins.py`            | Joins — inner, left, and full outer joins |
| `examples/08_sql_interface.py`    | SQL interface — `createOrReplaceTempView`, `spark.sql()` |
| `examples/09_window_functions.py` | Window functions — `rank`, `lag`, running totals |
| `examples/10_parquet.py`          | Writing & reading Parquet — `partitionBy`, partition pruning |
| `examples/11_null_handling.py`    | Handling nulls — `na.drop`, `na.fill`, `when`/`otherwise` |
| `examples/12_udfs.py`             | User-Defined Functions (UDFs) — custom Python logic on columns |
| `examples/13_partitioning.py`     | Partitioning & query plans — `repartition`, `coalesce`, `explain()` |
| `examples/14_schemas.py`          | Explicit schemas — `StructType`, `StructField`, DDL strings |
| `examples/15_reading_json.py`     | Reading JSON — nested structs, `ArrayType`, dot-notation access |
| `examples/16_nested_explode.py`   | Nested data — `explode`, `posexplode`, `map_keys`/`map_values` |
| `examples/17_deduplication.py`    | Deduplication — `distinct`, `dropDuplicates` by key |
| `examples/18_set_operations.py`   | Set operations — `union`, `unionByName`, `intersect`, `subtract` |
| `examples/19_caching.py`          | Caching — `cache`, `persist`, `unpersist`, storage levels |
| `examples/20_broadcast_joins.py`  | Broadcast joins — eliminating shuffle for small lookup tables |
| `examples/21_sampling.py`         | Train/test split — `randomSplit`, `sample`, bootstrapping |

## Running all tests

A pytest suite checks that every module runs without errors:

```bash
uv run python -m pytest -v
```

## Notes

- The `WARN NativeCodeLoader` message on startup is harmless for these local
  examples. It means Spark could not load the platform-specific native Hadoop
  library and will use its built-in Java implementations instead.
- `examples/05_reading_csv.py` reads the included `data/tips.csv` dataset.
- `examples/10_parquet.py` writes Parquet output to `out/employees/`.
