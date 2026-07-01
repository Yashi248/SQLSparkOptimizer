"""
Phase 2c - persist the before/after physical plans into Neo4j.

Runs the broadcast-join demo, captures the plan Spark uses BEFORE optimization
(SortMergeJoin) and AFTER (BroadcastHashJoin), parses each into a DAG, and writes
both to Neo4j tagged "before"/"after". You can then see and later visualize
the exact operator that changed.

Prereqs:
  - Neo4j running:  docker run -d --name neo4j-spark -p 7474:7474 -p 7687:7687 \
                      -e NEO4J_AUTH=neo4j/sparkplan123 neo4j:5
  - mlflow ui up (optional; the optimizer spans still log).
Run:  python phase2c_graph.py
"""
from __future__ import annotations

from sqlspark_optimizer.agents.optimizer import Optimizer
from sqlspark_optimizer.agents.plan_analyzer import PlanAnalyzer, parse_plan_tree
from sqlspark_optimizer.agents.rules import RuleContext
from sqlspark_optimizer.bench import executed_plan, join_ops
from phase2_optimize import DEMO_QUERY, PARQUET_DIR, make_spark
from sqlspark_optimizer.plan_graph_store import PlanGraphStore


def main() -> None:
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    for tbl in ("lineitem", "supplier", "nation"):
        spark.read.parquet((PARQUET_DIR / f"{tbl}.parquet").as_posix()) \
            .createOrReplaceTempView(tbl)

    analyzer = PlanAnalyzer(spark, PARQUET_DIR)
    optimizer = Optimizer()

    analysis = analyzer.analyze(DEMO_QUERY)
    opt = optimizer.optimize(
        DEMO_QUERY, RuleContext(broadcast_candidates=analysis.broadcast_candidates)
    )
    before_plan = analysis.plan_text
    after_plan = executed_plan(spark, opt.optimized_sql)
    spark.stop()

    print("Before joins:", join_ops(before_plan))
    print("After joins: ", join_ops(after_plan))

    g_before = parse_plan_tree(before_plan)
    g_after = parse_plan_tree(after_plan)
    print(f"Graph sizes — before: {len(g_before.nodes)} nodes/{len(g_before.edges)} edges, "
          f"after: {len(g_after.nodes)} nodes/{len(g_after.edges)} edges")

    try:
        store = PlanGraphStore()
        store.write_plan("before", g_before)
        store.write_plan("after", g_after)
        store.close()
    except Exception as exc:  # noqa: BLE001 - friendly hint if Neo4j is down
        raise SystemExit(
            f"\nCould not write to Neo4j ({exc}).\n"
            "Is the container up?  docker ps  |  http://localhost:7474\n"
        )

    print("\nWrote both plans to Neo4j. Explore at http://localhost:7474 with:")
    print("  // the whole optimized plan as a tree")
    print("  MATCH p=(n:Op {plan:'after'})-[:CHILD*0..]->(m) RETURN p")
    print("  // compare the join operator before vs after")
    print("  MATCH (n:Op) WHERE n.op ENDS WITH 'Join' RETURN n.plan, n.op, n.detail")


if __name__ == "__main__":
    main()
