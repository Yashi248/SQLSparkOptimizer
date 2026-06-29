"""
Plan-Analyzer agent — read Spark's physical plan, find the broadcast-join win.

This is where the project stops being a SQL converter and starts being an
optimizer. We ask Spark for the *physical* plan it would actually execute,
look at the join operators, and flag the classic anti-pattern:

    a SortMergeJoin (shuffle both sides) where one input is small enough to
    BROADCAST instead — turning an expensive shuffle into a cheap map-side join.

How we decide "small enough": every base table is a Parquet file written by
tpch_setup.py, so its on-disk size is a sound, honest proxy for broadcastability.
If the plan shuffle-joins a table whose Parquet is well under the broadcast
threshold, that's a missed broadcast.

The analyzer keeps its findings as a plain Python structure (a small graph of
plan nodes). Persisting that to Neo4j later (for the visualizer) is a ~20-line
add, not a redesign — which is exactly why we deferred Neo4j.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

from observability.tracing import traced

# Spark's default autoBroadcastJoinThreshold is 10MB. A table under this is a
# legitimate broadcast candidate.
BROADCAST_THRESHOLD_BYTES = 10 * 1024 * 1024

# Physical join operators that shuffle (i.e. NOT already broadcast).
SHUFFLE_JOIN_OPS = ("SortMergeJoin", "ShuffledHashJoin")


@dataclass
class PlanNode:
    op: str          # operator type, e.g. "SortMergeJoin", "FileScan"
    detail: str      # the raw plan line (for the visualizer / debugging)


@dataclass
class GraphNode:
    id: int          # stable id within one plan
    op: str          # operator type
    detail: str      # raw line


@dataclass
class PlanGraph:
    nodes: list[GraphNode]
    edges: list[tuple[int, int]]   # (parent_id, child_id)


def parse_plan_tree(plan_text: str) -> PlanGraph:
    """Turn Spark's indented physical-plan text into a parent->child DAG.

    Spark draws the operator tree with leading art (`+-`, `:-`, `:`, spaces). The
    indentation depth of each line encodes the tree: a deeper line is a child of
    the nearest shallower line above it. We track that with an indent stack, so
    joins (two children at the same deeper indent) naturally get two edges.
    """
    nodes: list[GraphNode] = []
    edges: list[tuple[int, int]] = []
    stack: list[tuple[int, int]] = []   # (indent, node_id), shallow -> deep
    nid = 0
    for raw in plan_text.splitlines():
        if not raw.strip():
            continue
        # Indent = count of leading tree-art chars before the operator label.
        indent = len(raw) - len(raw.lstrip(" :+-"))
        core = raw[indent:]
        m = re.match(r"(?:\*\(\d+\)\s*)?([A-Za-z]+)", core)
        if not m:
            continue
        nid += 1
        nodes.append(GraphNode(id=nid, op=m.group(1), detail=raw.strip()))
        # Pop siblings/deeper entries; the remaining top is this node's parent.
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if stack:
            edges.append((stack[-1][1], nid))
        stack.append((indent, nid))
    return PlanGraph(nodes=nodes, edges=edges)


@dataclass
class AnalysisResult:
    plan_text: str                       # full physical plan, as Spark prints it
    nodes: list[PlanNode]                # flattened operator list
    scanned_tables: dict[str, int]       # table name -> size in bytes
    shuffle_joins: list[str]             # raw lines of shuffle-join operators
    broadcast_candidates: list[str]      # tables that SHOULD be broadcast
    has_anti_pattern: bool = field(init=False)

    def __post_init__(self) -> None:
        self.has_anti_pattern = bool(self.shuffle_joins and self.broadcast_candidates)


class PlanAnalyzer:
    def __init__(self, spark: SparkSession, parquet_dir: Path,
                 threshold_bytes: int = BROADCAST_THRESHOLD_BYTES):
        self.spark = spark
        self.parquet_dir = Path(parquet_dir)
        self.threshold_bytes = threshold_bytes

    @traced("plan_analyzer")
    def analyze(self, spark_sql: str) -> AnalysisResult:
        df = self.spark.sql(spark_sql)
        # The executed plan = what Spark will actually run (post optimizer/AQE planning).
        plan_text = df._jdf.queryExecution().executedPlan().toString()

        nodes = self._parse_nodes(plan_text)
        shuffle_joins = [n.detail for n in nodes if n.op in SHUFFLE_JOIN_OPS]
        scanned = self._scanned_table_sizes(plan_text)

        # A table is a broadcast candidate if it's scanned, small, AND the plan
        # is currently shuffle-joining (otherwise there's nothing to fix).
        candidates = []
        if shuffle_joins:
            candidates = [
                t for t, size in scanned.items()
                if size < self.threshold_bytes
            ]

        return AnalysisResult(
            plan_text=plan_text,
            nodes=nodes,
            scanned_tables=scanned,
            shuffle_joins=shuffle_joins,
            broadcast_candidates=candidates,
        )

    def _parse_nodes(self, plan_text: str) -> list[PlanNode]:
        """Flatten the plan into operators. Each non-blank line starts with an
        operator name (possibly after tree-drawing chars like +- :- *(1))."""
        nodes: list[PlanNode] = []
        for raw in plan_text.splitlines():
            line = raw.strip()
            if not line:
                continue
            # Strip leading tree art / whole-stage-codegen markers: +- :- * (1)
            cleaned = re.sub(r"^[\s:+\-*()0-9]+", "", line)
            m = re.match(r"([A-Za-z]+)", cleaned)
            if m:
                nodes.append(PlanNode(op=m.group(1), detail=line))
        return nodes

    def _scanned_table_sizes(self, plan_text: str) -> dict[str, int]:
        """Find which TPC-H tables the plan scans (by the Parquet paths in the
        FileScan lines) and look up each file's on-disk size."""
        sizes: dict[str, int] = {}
        # FileScan lines reference the parquet location, e.g. .../tpch/nation.parquet
        for tbl in re.findall(r"([a-z]+)\.parquet", plan_text):
            path = self.parquet_dir / f"{tbl}.parquet"
            if path.exists() and tbl not in sizes:
                sizes[tbl] = os.path.getsize(path)
        return sizes
