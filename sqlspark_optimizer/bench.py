"""
Shared benchmarking + plan-inspection helpers for the optimize->measure loops.
Kept here so each phase runner reuses the same timing method (apples-to-apples).
"""
from __future__ import annotations

import re
import time

from pyspark.sql import SparkSession

# Includes Photon (Databricks) variants so plan diffs work on a Photon cluster.
JOIN_OPS = ("SortMergeJoin", "ShuffledHashJoin", "BroadcastHashJoin",
            "PhotonSortMergeJoin", "PhotonShuffledHashJoin", "PhotonBroadcastHashJoin")


def time_query(spark: SparkSession, sql: str, runs: int = 3) -> float:
    """Min wall-clock (seconds) over N runs, forcing full execution via the noop
    sink (no collection overhead). Min reduces JIT/cache noise."""
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - t)
    return min(times)


def executed_plan(spark: SparkSession, sql: str) -> str:
    """The physical plan text. Uses the SQL EXPLAIN command so it works on BOTH
    classic Spark and Spark Connect / serverless (where the `_jdf` JVM attribute
    is blocked). The output still contains the operators + PushedFilters our
    parsers look for."""
    plan_sql = "EXPLAIN " + sql.strip().rstrip(";")
    return spark.sql(plan_sql).collect()[0][0]


def join_ops(plan_text: str) -> list[str]:
    return [op for line in plan_text.splitlines() for op in JOIN_OPS if op in line]


def pushed_filters(plan_text: str) -> list[str]:
    """Extract the `PushedFilters: [...]` lists from FileScan nodes this is the
    direct evidence that a predicate was (or wasn't) pushed into the scan."""
    found = []
    for m in re.finditer(r"PushedFilters:\s*\[([^\]]*)\]", plan_text):
        inner = m.group(1).strip()
        found.append(inner if inner else "(none)")
    return found
