"""Session-scoped SparkSession shared across all example tests."""

import sys
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# runpy.run_path does not add the file's directory to sys.path the way
# running a script directly does, so examples that do `from _spark_config
# import ...` would fail. Add the examples directory once for the session.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from _spark_config import configure_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = configure_spark(
        SparkSession.builder.appName("primer-tests").master("local[*]")
    ).getOrCreate()
    yield session
    session.stop()
