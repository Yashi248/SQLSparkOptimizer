"""
Whole-workload optimization — run the optimizer across a batch of queries and
aggregate impact. One query is a demo; a workload is a business case.

Resilient by design: a query that errors is captured as a result row, never fatal.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from sqlspark_optimizer.api import optimize


def run_one(spark: SparkSession, qid: str, sql: str, parquet_dir: str | Path) -> dict:
    try:
        r = optimize(sql, spark, parquet_dir, source_dialect="duckdb",
                     timing_runs=1, use_llm_explain=False)
        return {"qid": qid, "optimized": r.optimized, "rules": r.applied_rules,
                "speedup": r.speedup if r.optimized else 1.0, "status": r.status,
                "cost": r.cost_summary.get("total_cost_usd", 0.0), "error": None}
    except Exception as exc:  # noqa: BLE001 - one bad query must not stop the batch
        return {"qid": qid, "optimized": False, "rules": [], "speedup": 1.0,
                "status": "ERROR", "cost": 0.0, "error": str(exc)[:80]}


def run_workload(spark: SparkSession, queries: dict[str, str],
                 parquet_dir: str | Path) -> tuple[list[dict], dict]:
    """Optimize every query; return (per-query results, aggregate summary)."""
    results = [run_one(spark, qid, sql, parquet_dir) for qid, sql in queries.items()]
    results.sort(key=lambda r: r["speedup"], reverse=True)

    optimized = [r for r in results if r["optimized"]]
    errored = [r for r in results if r["error"]]
    speedups = [r["speedup"] for r in optimized]
    summary = {
        "queries_run": len(results),
        "queries_optimized": len(optimized),
        "queries_errored": len(errored),
        "avg_speedup": round(sum(speedups) / len(speedups), 3) if speedups else 0.0,
        "best_speedup": round(max(speedups), 3) if speedups else 1.0,
        "total_cost_usd": round(sum(r["cost"] for r in results), 6),
    }
    return results, summary
