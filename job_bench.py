"""
JOB (Join Order Benchmark) run — the "real-skew" tier of the data taxonomy.

Same scaling thesis as stress_test.py, on real IMDB data: ANALYZE all 113 JOB
queries with the plan-only triage (no execution -> cannot OOM), report coverage
and which patterns fire, then run the full optimize->validate->measure on only
the top-K by predicted opportunity.

Why not just `sqlspark workload --all`? Full validation executes every query's
original AND rewrite; JOB's biggest tables (cast_info 36M, movie_info 15M rows)
overflow local Spark's 1GB off-heap direct-buffer limit under local[*] and take
down the driver. Analyze-all is execution-free, and the validate step here uses a
hardened, lower-parallelism session with a raised direct-memory limit so the
top-K survive. On a real cluster this is a non-issue; this is a laptop guardrail.

Run:
  python job_bench.py --queries C:/Users/you/Job_data/job_queries --validate-top 10
"""
from __future__ import annotations

import argparse
import os
import time
from collections import Counter
from pathlib import Path

# Deterministic + quiet, set before pyspark/graph import so the router sees them.
os.environ.setdefault("SQLSPARK_DISABLE_TELEMETRY", "1")
os.environ.setdefault("SQLSPARK_DISABLE_LLM", "1")
# Raise the JVM's off-heap ceiling (the 1GB default is what OOM'd) and give the
# driver real heap. Must be set before the Spark JVM launches (getOrCreate), and
# PYSPARK_SUBMIT_ARGS is the only launch-time hook from inside Python.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--driver-memory 4g "
    "--conf spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=3g "
    "pyspark-shell",
)

from pyspark.sql import SparkSession  # noqa: E402  (after env setup, by design)

from sqlspark_optimizer.agents.plan_analyzer import PlanAnalyzer  # noqa: E402
from sqlspark_optimizer.agents.translator import Translator  # noqa: E402
from sqlspark_optimizer.analyze import analyze_only  # noqa: E402
from sqlspark_optimizer.loaders import load_queries  # noqa: E402
from sqlspark_optimizer.runtime import register_parquet_dir  # noqa: E402
from sqlspark_optimizer.workload import run_one  # noqa: E402

DATA_DIR = Path(__file__).resolve().parent / "data" / "job"


def _hardened_spark() -> SparkSession:
    """Local Spark tuned to survive JOB's big scans: fewer concurrent tasks (less
    simultaneous direct-buffer pressure) and smaller scan partitions. Keeps the
    broadcast-visible config (no auto-broadcast, no AQE) so the analyzer still
    sees the shuffle-join opportunities."""
    spark = (
        SparkSession.builder.appName("job-bench").master("local[4]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(queries_path: str, validate_top: int) -> None:
    queries = load_queries(queries_path, dialect="postgres")
    print(f"Loaded {len(queries)} JOB queries from {queries_path}")

    spark = _hardened_spark()
    register_parquet_dir(spark, DATA_DIR)
    analyzer = PlanAnalyzer(spark, DATA_DIR)
    translator = Translator(source_dialect="postgres")

    # --- analyze EVERYTHING (plan-only, no execution -> cannot OOM) ---
    t0 = time.perf_counter()
    results = [analyze_only(qid, sql, analyzer, translator)
               for qid, sql in queries.items()]
    dt = time.perf_counter() - t0

    with_opp = sorted((r for r in results if r.has_opportunity),
                      key=lambda r: r.opportunity_bytes, reverse=True)
    errored = [r for r in results if r.error]
    by_rule = Counter(rule for r in results for rule in r.rules)

    print("\n=== analyze-only triage (all 113) ===")
    print(f"  analyzed {len(results)} queries in {dt:.1f}s "
          f"({dt / len(results) * 1000:.0f} ms/query)")
    print(f"  opportunities found in {len(with_opp)} queries "
          f"({100 * len(with_opp) / len(results):.0f}%)")
    print(f"  by pattern: {dict(by_rule)}")
    if errored:
        print(f"  analyze errors: {len(errored)} "
              f"(e.g. {errored[0].qid}: {errored[0].error})")
    print("\n  top opportunities (predicted shuffle avoided):")
    for r in with_opp[:15]:
        print(f"    {r.qid:<8} {r.rules}  ~{r.opportunity_bytes / 1e6:.0f} MB")

    # --- full validate + measure on only the top-K (expensive, but few) ---
    if validate_top:
        topk = with_opp[:validate_top]
        print(f"\n=== full validate + measure: top {len(topk)} ===")
        rows, ok, speedups = [], 0, []
        for r in topk:
            res = run_one(spark, r.qid, queries[r.qid], DATA_DIR, "postgres")
            rows.append(res)
            if res["error"] is None:
                ok += 1
            if res["optimized"]:
                speedups.append(res["speedup"])
            tag = res["error"] or (",".join(res["rules"]) or "-")
            print(f"    {r.qid:<8} {res['speedup']:>6.2f}x  {res['status']:<10} {tag}")
        print("\n=== JOB summary ===")
        print(f"  validated (ran without error): {ok}/{len(topk)}")
        print(f"  rewrites applied + proven identical: {len(speedups)}/{len(topk)}")
        if speedups:
            print(f"  avg speedup (optimized): "
                  f"{sum(speedups) / len(speedups):.2f}x  |  "
                  f"best: {max(speedups):.2f}x")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run the optimizer over JOB/IMDB.")
    ap.add_argument("--queries", required=True,
                    help="Folder of the 113 JOB *.sql query files.")
    ap.add_argument("--validate-top", type=int, default=10,
                    help="How many top-opportunity queries to fully validate.")
    args = ap.parse_args()
    main(args.queries, args.validate_top)
