# PySpark-Primer

[![CI](https://github.com/deeplook/pyspark-primer/actions/workflows/ci.yml/badge.svg)](https://github.com/deeplook/pyspark-primer/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/deeplook/pyspark-primer)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/downloads/release/python-3120/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-ffdd00?style=flat&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/deeplook)

A collection of small, self-contained PySpark scripts for learning the Spark
DataFrame API on a local machine.

## Sample output

A recorded demo session — click the image to play it on [Asciinema]:

[![asciicast](https://asciinema.org/a/DLdle1UqtDPjDmIP.png)](https://asciinema.org/a/DLdle1UqtDPjDmIP)

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

## Docker

If you would rather not install Java, uv, and Python on the host, a container
ships all three. Build the image once:

```bash
docker build -t pyspark-primer .
```

The entrypoint is `uv run`, so pass any script path to run an example:

```bash
docker run --rm pyspark-primer examples/02_dataframes.py
```

Run the test suite the same way:

```bash
docker run --rm pyspark-primer python -m pytest -v
```

To iterate on the examples without rebuilding, mount them over the copy baked
into the image:

```bash
docker run --rm -v "$PWD/examples:/app/examples" pyspark-primer examples/02_dataframes.py
```

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

## Sail vs. PySpark benchmark

[Sail](https://github.com/lakehq/sail) is a Rust-native, single-process engine
that speaks the Spark Connect protocol, so the *same* DataFrame code can run on
either classic PySpark (a local JVM engine) or Sail (an in-process `sc://`
server). `examples/sail_vs_pyspark.py` runs one shared workload on both,
verifies they produce identical results, and reports the wall-clock time of
each.

Sail lives in an optional dependency group (it pulls in the Spark Connect
client extras), so install it explicitly:

```bash
uv sync --group sail
uv run examples/sail_vs_pyspark.py --rows 5000000 --runs 3
```

`--rows` sets how many rows to generate and `--runs` how many timed runs to do
per engine (the first is a warm-up, so start-up cost is excluded from the
reported timings).

### Compatibility matrix

Because Sail speaks Spark Connect, most of the tutorial examples run against it
unmodified. `examples/sail_compat.py` runs every numbered example against Sail
and prints a three-way matrix:

```bash
uv run examples/sail_compat.py
```

A recorded demo session — click the image to play it on [Asciinema]:

[![asciicast](https://asciinema.org/a/HHy2gnewI8mQJh13.png)](https://asciinema.org/a/HHy2gnewI8mQJh13)

- `succ` (green badge) — ran cleanly, exactly as on classic PySpark.
- `warn` (yellow badge) — ran and produced correct results, but Sail logged
  that an operation isn't supported yet and was silently ignored.
- `fail` (red badge) — raised an error Sail (or Spark Connect) couldn't handle.

At the time of writing: **18 clean, 2 no-ops, 1 incompatible.**

- `13_partitioning.py` (fail) calls `df.rdd.getNumPartitions()`. The low-level
  RDD API is not part of the Spark Connect protocol, so it fails on any Connect
  backend, not just Sail (it runs fine on classic PySpark).
- `19_caching.py` (no-op) — `persist`/`unpersist` are no-ops on Sail.
- `20_broadcast_joins.py` (no-op) — the broadcast `hint` is a no-op; Sail's
  planner chooses its own join strategy.

The examples run untouched: each is executed in a subprocess that starts a
private Sail server and redirects the hardcoded `local[*]` session to it via
`SPARK_REMOTE`. It's a report, not a gate, and exits 0 even with failures; pass
`--fail-on-error` for CI-style behavior.

## Running all tests

A pytest suite checks that every module runs without errors:

```bash
uv run python -m pytest -v
```

## Notes

- The included `conf/log4j2.properties` profile suppresses routine Spark and
  Hadoop startup logging. Java may still print `Using incubator modules:
  jdk.incubator.vector`; Spark enables that Java module automatically and the
  notice is harmless.
- `examples/05_reading_csv.py` reads the included `data/tips.csv` dataset.
- `examples/10_parquet.py` writes Parquet output to `out/employees/`.

[Asciinema]: https://asciinema.org
