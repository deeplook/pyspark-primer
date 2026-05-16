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

## Running the scripts

```bash
uv run test1.py   # SparkSession basics
uv run test2.py   # In-memory DataFrames
uv run test3.py   # Reading CSV files
uv run test4.py   # Joins
uv run test5.py   # Window functions
uv run test6.py   # SQL interface
uv run test7.py   # Writing & reading Parquet
uv run test8.py   # User-Defined Functions (UDFs)
uv run test9.py   # Handling nulls
uv run test10.py  # Partitioning and query plans
```

## Notes

- The `WARN NativeCodeLoader` message on startup is harmless — it just means
  the native Hadoop library isn't available, which is expected on macOS.
- `test3.py` downloads `tips.csv` from GitHub on first run and saves it
  locally. Subsequent runs use the local copy.
- `test7.py` writes Parquet output to `out/employees/`.
