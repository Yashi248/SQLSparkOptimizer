"""
Command-line interface — installed as `sqlspark` (see pyproject console_scripts).

    sqlspark draw                              # print the orchestrator diagram
    sqlspark optimize query.sql --data data/tpch [--dialect duckdb]
    sqlspark workload --data data/tpch --queries <dir|.sql|.json|.csv> --limit 8
"""
from __future__ import annotations

import argparse
from pathlib import Path

from sqlspark_optimizer.api import optimize
from sqlspark_optimizer.loaders import load_queries
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir
from sqlspark_optimizer.workload import run_workload


def _cmd_draw(_args) -> None:
    from sqlspark_optimizer.orchestrator.graph import OptimizerGraph
    graph = OptimizerGraph(spark=None, parquet_dir=".").build()
    print(graph.get_graph().draw_mermaid())


def _cmd_optimize(args) -> None:
    sql = Path(args.file).read_text(encoding="utf-8")
    spark = make_local_spark()
    register_parquet_dir(spark, args.data)
    r = optimize(sql, spark, args.data, source_dialect=args.dialect,
                 timing_runs=args.runs)
    print(f"\nApplied rules: {r.applied_rules or ['(none)']}")
    print(f"Status: {r.status}  |  speedup: {r.speedup:.2f}x")
    print(f"\nExplanation:\n  {r.explanation}")
    print(f"\nOptimized SQL:\n{r.optimized_sql}")
    spark.stop()


def _cmd_workload(args) -> None:
    queries = load_queries(args.queries, dialect=args.dialect)  # dir/.sql/.json/.csv
    ids = list(queries)
    if not args.all:
        ids = ids[:args.limit]
    queries = {q: queries[q] for q in ids}
    print(f"Loaded {len(queries)} queries from {args.queries}")

    spark = make_local_spark()
    register_parquet_dir(spark, args.data)
    results, summary = run_workload(spark, queries, args.data,
                                    source_dialect=args.dialect)
    spark.stop()

    print("\n=== per-query (ranked by speedup) ===")
    for r in results:
        rules = ",".join(r["rules"]) or (f"ERROR: {r['error']}" if r["error"] else "-")
        print(f"  Q{r['qid']:<4}{r['speedup']:>7.2f}x  {r['status']:<10} {rules}")
    print("\n=== summary ===")
    for k, v in summary.items():
        print(f"  {k:<20} {v}")


def main() -> None:
    ap = argparse.ArgumentParser(prog="sqlspark",
                                 description="Multi-agent SQL->PySpark optimizer.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("draw", help="print the orchestrator Mermaid diagram") \
        .set_defaults(func=_cmd_draw)

    p_opt = sub.add_parser("optimize", help="optimize one .sql file")
    p_opt.add_argument("file")
    p_opt.add_argument("--data", required=True, help="dir of <table>.parquet files")
    p_opt.add_argument("--dialect", default="spark")
    p_opt.add_argument("--runs", type=int, default=3)
    p_opt.set_defaults(func=_cmd_optimize)

    p_wl = sub.add_parser("workload", help="optimize a batch of queries")
    p_wl.add_argument("--data", required=True, help="dir of <table>.parquet files")
    p_wl.add_argument("--queries", required=True,
                      help="query source: a dir of .sql, or a .sql/.json/.csv file")
    p_wl.add_argument("--dialect", default="spark")
    p_wl.add_argument("--all", action="store_true")
    p_wl.add_argument("--limit", type=int, default=8)
    p_wl.set_defaults(func=_cmd_workload)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
