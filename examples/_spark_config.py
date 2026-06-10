"""Shared Spark configuration for the tutorial scripts."""

from pathlib import Path

from pyspark.sql import SparkSession

LOG4J_CONFIG = Path(__file__).resolve().parent.parent / "conf" / "log4j2.properties"


def configure_spark(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Use the repository's quieter Log4j profile for the driver JVM."""
    return builder.config(
        "spark.driver.extraJavaOptions",
        f"-Dlog4j.configurationFile={LOG4J_CONFIG.as_uri()}",
    )
