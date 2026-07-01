"""
Whole-workload mode — run the optimizer across a BATCH of queries and report
aggregate impact. This is where the project earns its keep: one query is a demo,
a workload is a business case.

For each TPC-H query it runs the full orchestrator (translate -> analyze ->
retrieve -> optimize -> validate -> explain), then aggregates:
  - how many queries had an optimization found + validated,
  - average / best speedup,
  - total optimizer cost (the cost-routing story: cheap to run over many queries),
  - per-query ranking by impact ("fix query N first").

Robust by design: a query that errors or whose fix fails validation is reported,
not fatal — exactly how a real workload tool behaves.

Run:  python whole_workload.py --limit 8        (quick)
      python whole_workload.py --all             (all 22, slower)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run
from sqlspark_optimizer.orchestrator.graph import OptimizerGraph

DATA_DIR = Path(__file__).resolve().parent / "data"
PARQUET_DIR = DATA_DIR / "tpch"
QUERIES_PATH = DATA_DIR / "tpch_queries.json"
TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("whole-workload")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # expose broadcast wins
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


def run_one(spark, qid: str, sql: str) -> dict:
    """Optimize a single query; never raise — capture errors as a result row."""
    try:
        og = OptimizerGraph(spark, PARQUET_DIR, source_dialect="duckdb",
                            timing_runs=1, use_llm_explain=False)
        final = og.build().invoke({"source_sql": sql})
        rules = final.get("applied_rules", [])
        return {
            "qid": qid,
            "optimized": bool(rules),
            "rules": rules,
            "speedup": final.get("speedup", 1.0) if rules else 1.0,
            "status": final.get("status", "?"),
            "cost": final.get("cost_summary", {}).get("total_cost_usd", 0.0),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - one bad query must not stop the batch
        return {"qid": qid, "optimized": False, "rules": [], "speedup": 1.0,
                "status": "ERROR", "cost": 0.0, "error": str(exc)[:80]}


def main(query_ids: list[str]) -> None:
    queries = json.loads(QUERIES_PATH.read_text(encoding="utf-8"))
    init_mlflow()
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")
    for tbl in TPCH_TABLES:
        spark.read.parquet((PARQUET_DIR / f"{tbl}.parquet").as_posix()) \
            .createOrReplaceTempView(tbl)

    results = []
    for qid in query_ids:
        sql = queries.get(qid)
        if not sql:
            continue
        print(f"  optimizing Q{qid} ...", flush=True)
        results.append(run_one(spark, qid, sql))
    spark.stop()

    # --- per-query table, ranked by speedup ---
    results.sort(key=lambda r: r["speedup"], reverse=True)
    print("\n=== per-query results (ranked by speedup) ===")
    print(f"  {'query':<7}{'speedup':>9}  {'status':<10} rules")
    for r in results:
        rules = ",".join(r["rules"]) or ("ERROR: " + (r["error"] or "") if r["error"] else "-")
        print(f"  Q{r['qid']:<6}{r['speedup']:>8.2f}x  {r['status']:<10} {rules}")

    # --- aggregate ---
    optimized = [r for r in results if r["optimized"]]
    errored = [r for r in results if r["error"]]
    speedups = [r["speedup"] for r in optimized]
    avg_speedup = sum(speedups) / len(speedups) if speedups else 0.0
    best = max(results, key=lambda r: r["speedup"]) if results else None
    total_cost = sum(r["cost"] for r in results)

    print("\n=== workload summary ===")
    print(f"  queries run:        {len(results)}")
    print(f"  optimized+validated:{len(optimized)}")
    print(f"  errored:            {len(errored)}")
    print(f"  avg speedup (opt):  {avg_speedup:.2f}x")
    if best:
        print(f"  best:               Q{best['qid']} at {best['speedup']:.2f}x "
              f"({','.join(best['rules']) or '-'})")
    print(f"  total optimizer cost: ${total_cost:.6f}")

    with pipeline_run(query_id="whole-workload", phase="workload"):
        mlflow.log_metric("queries_run", len(results))
        mlflow.log_metric("queries_optimized", len(optimized))
        mlflow.log_metric("queries_errored", len(errored))
        mlflow.log_metric("avg_speedup", round(avg_speedup, 3))
        mlflow.log_metric("total_optimizer_cost_usd", round(total_cost, 6))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="run all 22 queries")
    ap.add_argument("--limit", type=int, default=8, help="run first N queries")
    ap.add_argument("--queries", type=int, nargs="+", help="specific query numbers")
    args = ap.parse_args()
    if args.queries:
        ids = [str(q) for q in args.queries]
    else:
        ids = [str(i) for i in range(1, 23)]
        if not args.all:
            ids = ids[:args.limit]
    main(ids)
