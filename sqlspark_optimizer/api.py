"""
Public API — the one function most users call.

    from sqlspark_optimizer import optimize
    result = optimize(sql, spark, parquet_dir="/path/to/tables")
    print(result.optimized_sql, result.speedup, result.explanation)

`optimize` runs the full multi-agent pipeline (translate -> analyze -> retrieve ->
optimize -> validate -> explain) over the caller's SparkSession and returns a
clean result. The caller owns Spark + table registration, so this runs inside
their environment against their data (code goes to the data, not the reverse).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

from sqlspark_optimizer.orchestrator.graph import OptimizerGraph


@dataclass
class OptimizeResult:
    original_sql: str
    optimized_sql: str
    applied_rules: list[str]
    speedup: float
    status: str
    explanation: str
    iterations: int = 0
    cost_summary: dict = field(default_factory=dict)
    log: list[str] = field(default_factory=list)

    @property
    def optimized(self) -> bool:
        return bool(self.applied_rules)

    @property
    def reverted(self) -> bool:
        return self.status == "reverted"


def optimize(sql: str, spark: SparkSession, parquet_dir: str | Path, *,
             source_dialect: str = "spark", timing_runs: int = 3,
             use_llm_explain: bool = True) -> OptimizeResult:
    """Optimize one query. `parquet_dir` is where the tables' Parquet lives (used
    for the broadcast size heuristic). Tables must already be registered on
    `spark` as temp views matching the query's table names."""
    graph = OptimizerGraph(spark, Path(parquet_dir), source_dialect=source_dialect,
                           timing_runs=timing_runs, use_llm_explain=use_llm_explain)
    final = graph.build().invoke({"source_sql": sql})
    return OptimizeResult(
        original_sql=sql,
        optimized_sql=final.get("current_sql", sql),
        applied_rules=final.get("applied_rules", []),
        speedup=final.get("speedup", 1.0),
        status=final.get("status", "unknown"),
        explanation=final.get("explanation", ""),
        iterations=final.get("iteration", 0),
        cost_summary=final.get("cost_summary", {}),
        log=final.get("log", []),
    )
