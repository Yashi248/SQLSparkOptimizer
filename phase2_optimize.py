"""
Phase 2 — the optimize -> validate -> measure loop (broadcast join).

End to end:
  1. Run a query that joins a huge table (lineitem) with tiny ones (supplier,
     nation). We force Spark into the anti-pattern (sort-merge join) by setting
     autoBroadcastJoinThreshold=-1 and disabling AQE -> deterministic, readable.
  2. Plan-Analyzer reads the physical plan, sees SortMergeJoins over small
     tables, and flags them as broadcast candidates.
  3. Optimizer injects /*+ BROADCAST(...) */ -> Spark now does a broadcast join.
  4. Validator proves the optimized result is identical to the original.
  5. We time both and log before/after + speedup to MLflow.

Run (with `mlflow ui` up):  python phase2_optimize.py
For a bigger speedup, regenerate data at scale: python data/tpch_setup.py --sf 1
"""
from __future__ import annotations

import time
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

from agents.optimizer import Optimizer
from agents.plan_analyzer import PlanAnalyzer, SHUFFLE_JOIN_OPS
from agents.validator import frames_match
from observability.tracing import init_mlflow, pipeline_run

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"

# A query with an obvious broadcast win: lineitem (huge) joined to supplier and
# nation (tiny). Revenue by nation.
DEMO_QUERY = """
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation   ON s_nationkey = n_nationkey
GROUP BY n_name
ORDER BY revenue DESC
""".strip()


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("phase2-optimize")
        # Force the anti-pattern so the optimization is demonstrable:
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # no auto-broadcast
        .config("spark.sql.adaptive.enabled", "false")         # static, readable plan
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


def time_query(spark: SparkSession, sql: str, runs: int = 3) -> float:
    """Min wall-clock over N runs, forcing full execution via the noop sink
    (no collection overhead). Min reduces JIT/cache noise."""
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - t)
    return min(times)


def join_ops(plan_text: str) -> list[str]:
    found = []
    for line in plan_text.splitlines():
        for op in (*SHUFFLE_JOIN_OPS, "BroadcastHashJoin"):
            if op in line:
                found.append(op)
    return found


def main() -> None:
    init_mlflow()
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Register the tables this query needs.
    for tbl in ("lineitem", "supplier", "nation"):
        spark.read.parquet((PARQUET_DIR / f"{tbl}.parquet").as_posix()) \
            .createOrReplaceTempView(tbl)

    analyzer = PlanAnalyzer(spark, PARQUET_DIR)
    optimizer = Optimizer()

    with pipeline_run(query_id="phase2-demo", phase="2-optimize"):
        # --- analyze the original ---
        analysis = analyzer.analyze(DEMO_QUERY)
        print("Original join operators:", join_ops(analysis.plan_text) or ["(none)"])
        print("Scanned tables (bytes):", analysis.scanned_tables)
        print("Broadcast candidates:", analysis.broadcast_candidates)
        print("Anti-pattern present:", analysis.has_anti_pattern)

        if not analysis.has_anti_pattern:
            print("\nNo broadcast anti-pattern found — nothing to optimize.")
            spark.stop()
            return

        # --- optimize ---
        opt = optimizer.optimize(DEMO_QUERY, analysis.broadcast_candidates)
        opt_plan = spark.sql(opt.optimized_sql)._jdf.queryExecution() \
            .executedPlan().toString()
        print("\nOptimized join operators:", join_ops(opt_plan) or ["(none)"])

        # --- validate: optimized result must equal the original result ---
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

        # --- log everything ---
        mlflow.log_param("pattern", opt.pattern)
        mlflow.log_param("broadcast_tables", ", ".join(opt.broadcast_tables))
        mlflow.log_metric("validation_passed", int(passed))
        mlflow.log_metric("runtime_before_ms", round(before * 1000, 1))
        mlflow.log_metric("runtime_after_ms", round(after * 1000, 1))
        mlflow.log_metric("speedup", round(speedup, 3))
        mlflow.log_text(analysis.plan_text, "plan_before.txt")
        mlflow.log_text(opt_plan, "plan_after.txt")
        mlflow.log_text(opt.optimized_sql, "optimized.sql")

    spark.stop()
    print("\nPhase 2 loop complete. See the run + plan artifacts at http://localhost:5000")


if __name__ == "__main__":
    main()
