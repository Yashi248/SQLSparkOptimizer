"""
Runtime helpers — spin up a local Spark and register a directory of Parquet
tables. Shared by the CLI and the demos so table wiring lives in one place.

In corporate use the caller brings their OWN SparkSession and table registrations
(their warehouse) and calls `optimize()` directly; these helpers are the
convenience path for local files / demos.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def make_local_spark(app_name: str = "sqlspark-optimizer",
                     expose_broadcast: bool = True) -> SparkSession:
    """Local single-node Spark. `expose_broadcast=True` disables auto-broadcast
    and AQE so broadcast-join opportunities are visible + plans are readable."""
    builder = (SparkSession.builder.appName(app_name).master("local[*]")
               .config("spark.sql.shuffle.partitions", "8"))
    if expose_broadcast:
        builder = (builder
                   .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                   .config("spark.sql.adaptive.enabled", "false"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_parquet_dir(spark: SparkSession, data_dir: str | Path) -> list[str]:
    """Register every <name>.parquet in a directory as a temp view named <name>.
    Returns the view names."""
    data_dir = Path(data_dir)
    names = []
    for pq in sorted(data_dir.glob("*.parquet")):
        spark.read.parquet(pq.as_posix()).createOrReplaceTempView(pq.stem)
        names.append(pq.stem)
    return names
