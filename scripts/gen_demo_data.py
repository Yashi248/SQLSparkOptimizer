"""
Generate docs/demo_data.js for the read-only GitHub Pages demo.

Runs the REAL pipeline on a few example queries and bakes the results (including
the before/after physical-plan graphs) into a JS file the static demo loads. No
backend is needed to view the demo — the data is authentic, captured here once.

Run:  python scripts/gen_demo_data.py
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlspark_optimizer.agents.plan_analyzer import parse_plan_tree
from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.api import optimize
from sqlspark_optimizer.bench import executed_plan
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "data" / "tpch"
OUT = ROOT / "docs" / "demo_data.js"

EXAMPLES = {
    "Broadcast join":
        "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue\n"
        "FROM lineitem\n"
        "JOIN supplier ON l_suppkey = s_suppkey\n"
        "JOIN nation ON s_nationkey = n_nationkey\n"
        "GROUP BY n_name ORDER BY revenue DESC",
    "Predicate pushdown":
        "SELECT SUM(l_extendedprice) AS revenue\n"
        "FROM lineitem\n"
        "WHERE YEAR(l_shipdate) = 1994",
}


def _graph_json(g) -> dict:
    return {"nodes": [{"id": n.id, "label": n.op} for n in g.nodes],
            "edges": [{"from": p, "to": c} for p, c in g.edges]}


def main() -> None:
    spark = make_local_spark(app_name="gen-demo")
    register_parquet_dir(spark, PARQUET_DIR)
    demo = []
    for name, q in EXAMPLES.items():
        spark_sql = Translator(source_dialect="spark").translate(q).spark_sql
        before = _graph_json(parse_plan_tree(executed_plan(spark, spark_sql)))
        r = optimize(q, spark, PARQUET_DIR, source_dialect="spark",
                     timing_runs=1, use_llm_explain=False)
        after = _graph_json(parse_plan_tree(executed_plan(spark, r.optimized_sql)))
        demo.append({
            "name": name, "query": q, "optimized_sql": r.optimized_sql,
            "applied_rules": r.applied_rules, "speedup": round(r.speedup, 2),
            "status": r.status, "explanation": r.explanation,
            "plan_before": before, "plan_after": after,
        })
        print(f"  captured '{name}': {r.applied_rules} @ {r.speedup:.2f}x")
    spark.stop()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("window.DEMO = " + json.dumps(demo, indent=2) + ";\n",
                   encoding="utf-8")
    print(f"\nWrote {OUT.relative_to(ROOT)} ({len(demo)} examples).")


if __name__ == "__main__":
    main()
