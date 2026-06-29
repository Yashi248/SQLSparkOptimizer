"""
Optimizer agent - apply the fix the Plan-Analyzer found.

v1 implements ONE pattern directly: the broadcast-join fix. Given the small
tables the analyzer flagged, it injects a Spark broadcast hint:

    SELECT /*+ BROADCAST(nation, supplier) */ ...

That hint forces Spark to broadcast those tables, overriding whatever made it
pick a SortMergeJoin (missing stats, a conservative threshold, AQE off). The
output is unchanged, only the execution strategy changes which is why the
Validator must confirm identical results afterwards.

Why hardcoded (for now): with a SINGLE pattern, "retrieve the right fix" is
retrieving from a list of one pgvector RAG would add infrastructure and zero
information. When we add more patterns, the retrieval step swaps in here without
touching the optimize->validate->measure loop around it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from observability.tracing import traced


@dataclass
class OptimizationResult:
    original_sql: str
    optimized_sql: str
    applied: bool
    pattern: str
    broadcast_tables: list[str]


class Optimizer:
    @traced("optimizer")
    def optimize(self, spark_sql: str, broadcast_tables: list[str]) -> OptimizationResult:
        if not broadcast_tables:
            return OptimizationResult(spark_sql, spark_sql, False, "none", [])

        hint = f"/*+ BROADCAST({', '.join(broadcast_tables)}) */"
        # Insert the hint right after the first SELECT keyword (case-insensitive).
        optimized, n = re.subn(
            r"(?i)\bSELECT\b", f"SELECT {hint}", spark_sql, count=1
        )
        applied = n == 1
        return OptimizationResult(
            original_sql=spark_sql,
            optimized_sql=optimized if applied else spark_sql,
            applied=applied,
            pattern="broadcast_join" if applied else "none",
            broadcast_tables=broadcast_tables,
        )
