"""
Stress test at scale (roadmap item C) — "analyze all cheaply, validate the few".

Generates a large synthetic workload, ANALYZES every query with the plan-only
triage (seconds), ranks by predicted opportunity, then runs the full
optimize->validate->measure only on the top-K. This is the scaling thesis made
concrete: you audit thousands of queries without executing them all.

Run:  python stress_test.py --n 1000 --validate-top 10
"""
from __future__ import annotations

import argparse
import time
from collections import Counter
from pathlib import Path

from sqlspark_optimizer.agents.plan_analyzer import PlanAnalyzer
from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.analyze import analyze_only
from sqlspark_optimizer.generator import generate_workload
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir
from sqlspark_optimizer.workload import run_one

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"


def main(n: int, validate_top: int) -> None:
    spark = make_local_spark(app_name="stress-test")
    register_parquet_dir(spark, PARQUET_DIR)
    queries = generate_workload(n)
    analyzer = PlanAnalyzer(spark, PARQUET_DIR)
    translator = Translator(source_dialect="spark")

    # --- analyze EVERYTHING (cheap, no execution) ---
    t0 = time.perf_counter()
    results = [analyze_only(qid, sql, analyzer, translator)
               for qid, sql in queries.items()]
    dt = time.perf_counter() - t0

    with_opp = sorted((r for r in results if r.has_opportunity),
                      key=lambda r: r.opportunity_bytes, reverse=True)
    by_rule = Counter(rule for r in results for rule in r.rules)

    print(f"\n=== analyze-only triage ===")
    print(f"  analyzed {len(results)} queries in {dt:.1f}s "
          f"({dt / len(results) * 1000:.0f} ms/query)")
    print(f"  found opportunities in {len(with_opp)} queries "
          f"({100 * len(with_opp) / len(results):.0f}%)")
    print(f"  by pattern: {dict(by_rule)}")
    print(f"\n  top opportunities (predicted shuffle avoided):")
    for r in with_opp[:10]:
        print(f"    {r.qid:<14} {r.rules}  ~{r.opportunity_bytes / 1e6:.0f} MB")

    # --- validate + measure only the top-K (expensive, but few) ---
    if validate_top:
        print(f"\n=== full validate + measure: top {validate_top} ===")
        tv = time.perf_counter()
        for r in with_opp[:validate_top]:
            res = run_one(spark, r.qid, queries[r.qid], PARQUET_DIR, "spark")
            mark = "PASS" if res["status"] != "reverted" else "reverted"
            print(f"  {r.qid:<14} {res['rules']}  {res['speedup']:.2f}x  [{mark}]")
        print(f"  (validated {validate_top} in {time.perf_counter() - tv:.1f}s)")

    spark.stop()
    print("\nTakeaway: analyzed the whole workload in seconds; paid the expensive "
          "validation only on the high-impact few.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1000, help="workload size")
    ap.add_argument("--validate-top", type=int, default=10,
                    help="full-validate the top-K opportunities (0 to skip)")
    main(ap.parse_args().n, ap.parse_args().validate_top)
