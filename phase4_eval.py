"""
Phase 4 — the eval/observability layer. Runs the curated eval set and publishes
the numbers that make the project's claims measured, not asserted:
  correctness %, routing accuracy, avg speedup, convergence, cost.

Prereqs: Spark + data/tpch generated. pgvector/mlflow optional (best-effort).
Run:  python phase4_eval.py
"""
from __future__ import annotations

from pathlib import Path

import mlflow

from sqlspark_optimizer.observability.eval import DEFAULT_EVAL_SET, evaluate
from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"


def main() -> None:
    init_mlflow()
    spark = make_local_spark(app_name="phase4-eval")   # broadcast exposed
    register_parquet_dir(spark, PARQUET_DIR)

    report = evaluate(spark, DEFAULT_EVAL_SET, PARQUET_DIR)
    spark.stop()

    print("\n=== per-case results ===")
    print(f"  {'case':<14}{'routing':<9}{'speedup':>8}  {'iters':>5}  applied / expected")
    for c in report.cases:
        mark = "OK" if c.routing_correct else "X"
        exp = ",".join(c.expected_rules) or "-"
        app = ",".join(c.applied_rules) or "-"
        note = f" [{c.error}]" if c.error else ""
        print(f"  {c.qid:<14}{mark:<9}{c.speedup:>7.2f}x  {c.iterations:>5}  "
              f"{app} / {exp}{note}")

    print("\n=== eval metrics ===")
    metrics = report.metrics()
    for k, v in metrics.items():
        print(f"  {k:<18} {v}")

    with pipeline_run(query_id="phase4-eval", phase="4-eval"):
        for k, v in metrics.items():
            mlflow.log_metric(k, v)
    print("\nLogged eval metrics to MLflow. See http://localhost:5000")


if __name__ == "__main__":
    main()
