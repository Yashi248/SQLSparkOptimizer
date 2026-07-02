"""
Phase 5 — LLM escalation path.

The demo query has an inefficiency NO deterministic rule covers: an arithmetic
expression wraps the column (`l_discount * 100 = 5`), which blocks predicate
pushdown. Our rule registry can't fix it -> the orchestrator escalates to the
SMART LLM, which proposes a novel rewrite (e.g. `l_discount = 0.05`). That
proposal is UNTRUSTED, so it goes straight through the Validator and is accepted
only if proven output-identical. LLM reach + Validator safety net.

Set a key to enable the smart tier (falls back gracefully if unset):
    $env:NVIDIA_API_KEY = "nvapi-..."     # free at build.nvidia.com
    $env:LLM_MODEL = "meta/llama-3.3-70b-instruct"

Run:  python phase5_escalate.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run
from sqlspark_optimizer.orchestrator.graph import OptimizerGraph
from sqlspark_optimizer.routing import ModelRouter
from sqlspark_optimizer.runtime import make_local_spark

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"

DEMO_QUERY = """
SELECT l_returnflag, SUM(l_extendedprice) AS revenue
FROM lineitem
WHERE l_discount * 100 = 5
GROUP BY l_returnflag
""".strip()


def main(fast: bool = False) -> None:
    if not ModelRouter().llm_available:
        print("No LLM configured — escalation will no-op. Set NVIDIA_API_KEY "
              "(free at build.nvidia.com) to enable the smart tier.\n")

    init_mlflow()
    spark = make_local_spark(app_name="phase5-escalate")
    spark.read.parquet((PARQUET_DIR / "lineitem.parquet").as_posix()) \
        .createOrReplaceTempView("lineitem")

    # `explain` is on by default (the corporate-value narration). --fast swaps it
    # for the deterministic template so iterating on escalation is a single LLM call.
    graph = OptimizerGraph(spark, PARQUET_DIR, timing_runs=1,
                           use_llm_explain=not fast).build()
    with pipeline_run(query_id="phase5-escalate", phase="5-escalate"):
        final = graph.invoke({"source_sql": DEMO_QUERY})
    spark.stop()

    print("=== orchestration log ===")
    for line in final.get("log", []):
        print(" ", line)
    print("\nApplied:", final.get("applied_rules") or ["(none)"])
    print(f"Status: {final.get('status')}  |  speedup: {final.get('speedup', 1.0):.2f}x")
    if "llm_escalation" in final.get("applied_rules", []):
        print("\n✅ LLM escalation produced a VALIDATED novel rewrite:")
    else:
        print("\n(no validated escalation — LLM unavailable, or its rewrite "
              "didn't validate/help; the original was kept — safe either way)")
    print("\n=== explanation ===")
    print(" ", final.get("explanation"))
    print("\n=== final SQL ===")
    print(final.get("current_sql"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fast", action="store_true",
                    help="template explanation (1 LLM call, for quick iteration)")
    main(ap.parse_args().fast)
