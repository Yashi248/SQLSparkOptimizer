"""
Phase 3 - run the full multi-agent pipeline through the LangGraph orchestrator.

The demo query has TWO anti-patterns at once:
  - a sort-merge join against small tables (supplier, nation)  -> broadcast_join
  - YEAR(l_shipdate) = 1994 blocking pushdown                  -> sargable_year

The orchestrator detects both, retrieves the matching fixes from pgvector, applies
them, proves the result is unchanged, measures the speedup, then loops once more,
finds nothing left, and converges.

Prereqs: Spark + data; pgvector up (optional - falls back if down); mlflow ui
(optional - best-effort). Run:  python phase3_orchestrate.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run
from sqlspark_optimizer.orchestrator.graph import OptimizerGraph

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"

DEMO_QUERY = """
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation   ON s_nationkey = n_nationkey
WHERE YEAR(l_shipdate) = 1994
GROUP BY n_name
ORDER BY revenue DESC
""".strip()


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("phase3-orchestrate")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # force the join anti-pattern
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


def draw() -> None:
    """Print the orchestrator as a Mermaid diagram (no Spark needed). Paste the
    output into any Mermaid viewer (e.g. mermaid.live) to see the pipeline."""
    graph = OptimizerGraph(spark=None, parquet_dir=PARQUET_DIR).build()
    print(graph.get_graph().draw_mermaid())


def main() -> None:
    init_mlflow()
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    for tbl in ("lineitem", "supplier", "nation"):
        spark.read.parquet((PARQUET_DIR / f"{tbl}.parquet").as_posix()) \
            .createOrReplaceTempView(tbl)

    og = OptimizerGraph(spark, PARQUET_DIR)
    graph = og.build()

    with pipeline_run(query_id="phase3-orchestrate", phase="3"):
        final = graph.invoke({"source_sql": DEMO_QUERY})
        summary = final.get("cost_summary", {})
        mlflow.log_param("applied_rules", ", ".join(final.get("applied_rules", [])))
        mlflow.log_metric("final_speedup", round(final.get("speedup", 0.0), 3))
        mlflow.log_metric("total_cost_usd", summary.get("total_cost_usd", 0.0))
        mlflow.log_metric("total_tokens", summary.get("total_tokens", 0))
        mlflow.log_text(final.get("current_sql", ""), "optimized.sql")

    print("\n=== orchestration log ===")
    for line in final.get("log", []):
        print(" ", line)
    print("\nApplied rules:", final.get("applied_rules") or ["(none)"])
    print(f"Final status: {final.get('status')}  |  speedup: "
          f"{final.get('speedup', float('nan')):.2f}x")

    print("\n=== cost routing (per stage) ===")
    print(f"  {'stage':<10} {'tier':<6} {'model':<22} {'tokens':>7} {'cost($)':>9}")
    for s in og.router.ledger:
        toks = s.prompt_tokens + s.completion_tokens
        print(f"  {s.stage:<10} {s.tier.value:<6} {s.model:<22} {toks:>7} {s.est_cost_usd:>9.6f}")
    summ = final.get("cost_summary", {})
    print(f"  total: {summ.get('total_tokens', 0)} tokens, "
          f"${summ.get('total_cost_usd', 0.0):.6f}")

    print("\n=== explanation (smart tier) ===")
    print(" ", final.get("explanation"))
    print("\n=== final optimized SQL ===")
    print(final.get("current_sql"))

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--draw", action="store_true",
                    help="Print the orchestrator's Mermaid diagram and exit.")
    if ap.parse_args().draw:
        draw()
    else:
        main()
