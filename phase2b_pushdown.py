"""
Phase 2b — predicate pushdown via a sargable-predicate rewrite.

The anti-pattern neither Catalyst nor AQE fixes: a filter that wraps a column in
a function.

    WHERE YEAR(l_shipdate) = 1994

YEAR(col) is opaque to the Parquet reader, so the filter can't be pushed into the
scan — Spark reads ALL 6M rows, then filters. The Optimizer rewrites it into an
equivalent *sargable* range on the bare column:

    WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'

Now the predicate appears in the scan's `PushedFilters` and far fewer rows flow
upward. This is a LOGICAL rewrite (it changes the plan's meaning if done wrong),
so the Validator must prove the result is unchanged — this is where the safety
layer earns its keep.

Run (with `mlflow ui` up):  python phase2b_pushdown.py
"""
from __future__ import annotations

from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

from sqlspark_optimizer.agents.optimizer import Optimizer
from sqlspark_optimizer.agents.validator import frames_match
from sqlspark_optimizer.bench import executed_plan, pushed_filters, time_query
from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"

DEMO_QUERY = """
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
WHERE YEAR(l_shipdate) = 1994
""".strip()


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("phase2b-pushdown")
        .config("spark.sql.adaptive.enabled", "false")   # static, readable plan
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


def main() -> None:
    init_mlflow()
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.parquet((PARQUET_DIR / "lineitem.parquet").as_posix()) \
        .createOrReplaceTempView("lineitem")

    optimizer = Optimizer()

    with pipeline_run(query_id="phase2b-pushdown", phase="2b-pushdown"):
        # --- optimize (runs the rule registry; sargable rules fire here) ---
        opt = optimizer.optimize(DEMO_QUERY)
        print("Rules applied:", opt.patterns or ["(none)"], "| columns:", opt.rewritten_columns)
        print("\nOptimized SQL:\n", opt.optimized_sql)

        if not opt.did_optimize:
            print("\nNo non-sargable predicate found — nothing to rewrite.")
            spark.stop()
            return

        # --- evidence: did the filter get pushed into the scan? ---
        before_plan = executed_plan(spark, DEMO_QUERY)
        after_plan = executed_plan(spark, opt.optimized_sql)
        print("\nPushedFilters BEFORE:", pushed_filters(before_plan))
        print("PushedFilters AFTER: ", pushed_filters(after_plan))

        # --- validate: the rewrite must NOT change the answer ---
        original_df = spark.sql(DEMO_QUERY).toPandas()
        optimized_df = spark.sql(opt.optimized_sql).toPandas()
        passed, reason = frames_match(original_df, optimized_df)
        print(f"\nValidation: {'PASS' if passed else 'FAIL'} — {reason}")

        # --- measure ---
        before = time_query(spark, DEMO_QUERY)
        after = time_query(spark, opt.optimized_sql)
        speedup = before / after if after else float("nan")
        print(f"\nRuntime before: {before*1000:7.1f} ms")
        print(f"Runtime after:  {after*1000:7.1f} ms")
        print(f"Speedup:        {speedup:.2f}x")

        # --- log ---
        mlflow.log_param("pattern", opt.pattern)
        mlflow.log_param("rewritten_columns", ", ".join(opt.rewritten_columns))
        mlflow.log_metric("validation_passed", int(passed))
        mlflow.log_metric("runtime_before_ms", round(before * 1000, 1))
        mlflow.log_metric("runtime_after_ms", round(after * 1000, 1))
        mlflow.log_metric("speedup", round(speedup, 3))
        mlflow.log_text(before_plan, "plan_before.txt")
        mlflow.log_text(after_plan, "plan_after.txt")
        mlflow.log_text(opt.optimized_sql, "optimized.sql")

    spark.stop()
    print("\nPhase 2b complete. See the run + plan artifacts at http://localhost:5000")


if __name__ == "__main__":
    main()
