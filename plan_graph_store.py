"""
Persist a parsed Spark physical plan into Neo4j as a DAG.

Why Neo4j: a query plan *is* a graph (operators = nodes, data flow = edges), so a
graph DB is the natural store and it's the backing store for the later plan
visualizer (seeing a SortMergeJoin node become a BroadcastHashJoin). The analyzer
already produces the node/edge structure (agents/plan_analyzer.parse_plan_tree);
this just writes it.

Each plan is tagged with a `plan` label (e.g. "before"/"after") so we can store
both the pre- and post-optimization plans side by side and diff them visually.
"""
from __future__ import annotations

from neo4j import GraphDatabase

from agents.plan_analyzer import PlanGraph


class PlanGraphStore:
    def __init__(self, uri: str = "bolt://localhost:7687",
                 user: str = "neo4j", password: str = "sparkplan123"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def write_plan(self, label: str, graph: PlanGraph) -> None:
        """Replace any existing nodes for `label` and write this plan's DAG."""
        with self.driver.session() as session:
            session.execute_write(self._write_plan, label, graph)

    @staticmethod
    def _write_plan(tx, label: str, graph: PlanGraph) -> None:
        tx.run("MATCH (n:Op {plan: $label}) DETACH DELETE n", label=label)
        tx.run(
            "UNWIND $nodes AS n "
            "CREATE (:Op {plan: $label, nid: n.id, op: n.op, detail: n.detail})",
            label=label,
            nodes=[{"id": n.id, "op": n.op, "detail": n.detail} for n in graph.nodes],
        )
        tx.run(
            "UNWIND $edges AS e "
            "MATCH (a:Op {plan: $label, nid: e.p}), (b:Op {plan: $label, nid: e.c}) "
            "CREATE (a)-[:CHILD]->(b)",
            label=label,
            edges=[{"p": p, "c": c} for p, c in graph.edges],
        )
