"""One local SparkSession for the pipeline and tests (2 cores here)."""
from pyspark.sql import SparkSession


def get_spark(app="ingest"):
    return (SparkSession.builder.master("local[2]").appName(app)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())
