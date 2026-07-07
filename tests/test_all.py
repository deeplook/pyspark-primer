"""Smoke test: every example module must run cleanly end to end.

Each example is executed in-process via runpy so that SparkSession.getOrCreate()
returns the already-running session from the spark fixture rather than cold-starting
a new JVM. The session is shared across all 21 tests; individual examples cannot
stop it because none of them call spark.stop().
"""

import runpy
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parent.parent
MODULES = sorted((ROOT_DIR / "examples").glob("[0-9][0-9]_*.py"))


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_module_runs(module: Path, spark: SparkSession) -> None:
    runpy.run_path(str(module), run_name="__main__")
