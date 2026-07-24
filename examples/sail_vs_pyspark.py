"""
Sail vs. PySpark — running the same workload on two engines and timing it.

`Sail <https://github.com/lakehq/sail>`_ is a Rust-native, single-process
implementation of the Spark Connect protocol. Because it speaks the same wire
protocol, the *identical* DataFrame code can run against either:

* classic PySpark, which launches a local JVM Spark engine (``local[*]``), or
* Sail, which starts an in-process Rust server and connects over ``sc://``.

This script runs one shared workload (``run_workload``) on both engines, checks
that they produce the same result, and reports the wall-clock time of each so
you can compare them on your own machine.

The two engines cannot be active in the same Python process at once, so the
classic session is fully stopped before the Sail server is started.

Requires the optional ``sail`` dependency group::

    uv sync --group sail
    uv run examples/sail_vs_pyspark.py --rows 5000000 --runs 3
"""

from __future__ import annotations

import argparse
import time
from statistics import median

from _spark_config import configure_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.functions import count as spark_count
from pyspark.sql.functions import sum as spark_sum


def run_workload(spark: SparkSession, n_rows: int) -> tuple[int, int]:
    """A deterministic, engine-agnostic DataFrame workload.

    Generates ``n_rows`` rows, derives a few integer columns, aggregates by a
    composite key, self-joins the aggregate, and collapses the result to a
    single row. Only integer arithmetic is used so both engines return exactly
    the same numbers, letting us assert they agree.
    """
    df = spark.range(0, n_rows).select(
        (col("id") % 1000).alias("key"),
        (col("id") % 7).alias("bucket"),
        (col("id") * col("id") % 1_000_003).alias("val"),
    )

    agg = df.groupBy("key", "bucket").agg(
        spark_sum("val").alias("sum_val"),
        spark_count("*").alias("cnt"),
        avg("val").alias("avg_val"),
    )

    # Self-join on the key to add some shuffle/join work to the plan.
    joined = agg.join(
        agg.groupBy("key").agg(spark_sum("sum_val").alias("key_total")),
        on="key",
    )

    row = joined.agg(
        spark_sum("sum_val").alias("total_sum"),
        spark_sum("cnt").alias("total_cnt"),
    ).collect()[0]

    return int(row["total_sum"]), int(row["total_cnt"])


def time_runs(
    spark: SparkSession, n_rows: int, runs: int
) -> tuple[tuple[int, int], list[float]]:
    """Run the workload ``runs`` times, returning its result and each timing.

    The first run is a warm-up (it pays engine/plan-compilation start-up cost)
    and is excluded from the returned timings when more than one run is asked
    for.
    """
    result: tuple[int, int] | None = None
    timings: list[float] = []
    for i in range(runs):
        start = time.perf_counter()
        result = run_workload(spark, n_rows)
        elapsed = time.perf_counter() - start
        if runs == 1 or i > 0:
            timings.append(elapsed)
    assert result is not None
    return result, timings


def build_pyspark_session() -> SparkSession:
    """Classic PySpark: a local JVM engine using the repo's quiet log profile."""
    return configure_spark(
        SparkSession.builder.appName("sail-vs-pyspark").master("local[*]")
    ).getOrCreate()


def build_sail_session() -> tuple[SparkSession, object]:
    """Start an in-process Sail server and connect a Spark Connect session."""
    from pysail.spark import SparkConnectServer

    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    host, port = server.listening_address
    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    return spark, server


def summarise(name: str, timings: list[float]) -> None:
    best = min(timings)
    print(
        f"{name:<10} runs={len(timings)}  "
        f"best={best:6.3f}s  median={median(timings):6.3f}s  "
        f"worst={max(timings):6.3f}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5_000_000,
        help="number of rows to generate (default: 5,000,000)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="timed runs per engine; the first is a warm-up (default: 3)",
    )
    args = parser.parse_args()

    print(f"Workload: {args.rows:,} rows, {args.runs} run(s) per engine\n")

    # --- Classic PySpark (local JVM) -------------------------------------
    spark = build_pyspark_session()
    pyspark_result, pyspark_timings = time_runs(spark, args.rows, args.runs)
    spark.stop()

    # --- Sail (Rust, Spark Connect) --------------------------------------
    # Must run after the classic session is stopped: the two cannot be active
    # in the same process at the same time.
    spark, server = build_sail_session()
    sail_result, sail_timings = time_runs(spark, args.rows, args.runs)
    spark.stop()
    server.stop()  # type: ignore[attr-defined]

    # --- Report ----------------------------------------------------------
    print("\n=== results ===")
    print(f"PySpark result: {pyspark_result}")
    print(f"Sail result:    {sail_result}")
    if pyspark_result != sail_result:
        raise SystemExit("Engines disagreed on the result!")
    print("Both engines agree.\n")

    print("=== timings (start-up excluded via warm-up) ===")
    summarise("PySpark", pyspark_timings)
    summarise("Sail", sail_timings)

    def compare(pyspark_time: float, sail_time: float) -> str:
        if sail_time < pyspark_time:
            return f"Sail {pyspark_time / sail_time:.2f}x faster"
        return f"PySpark {sail_time / pyspark_time:.2f}x faster"

    best = compare(min(pyspark_timings), min(sail_timings))
    worst = compare(max(pyspark_timings), max(sail_timings))
    print(f"\nSpeedup range: best run — {best}; worst run — {worst}.")


if __name__ == "__main__":
    main()
