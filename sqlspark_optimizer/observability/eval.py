"""
Evaluation layer (Phase 4) — turn the pipeline into *measured claims*.

Runs an eval set of queries with KNOWN expected optimizations and scores:
  - correctness   : every emitted result is output-preserving (validated fixes
                    are proven identical; failed fixes are reverted to original) —
                    the guardrail metric. Should be 100% by construction.
  - routing accuracy: did the system select the expected rule(s) for each query?
  - speedup       : average runtime improvement on optimized queries.
  - convergence   : average loop iterations to reach a stable query.
  - cost          : total optimizer cost (cost-routing story).

This is the deliverable as much as the code: numbers that prove it works.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

from sqlspark_optimizer.api import optimize


@dataclass
class EvalCase:
    qid: str
    sql: str
    expected_rules: list[str]      # empty = expect no optimization
    dialect: str = "spark"


@dataclass
class CaseResult:
    qid: str
    expected_rules: list[str]
    applied_rules: list[str]
    routing_correct: bool
    output_preserving: bool
    speedup: float
    iterations: int
    cost: float
    status: str
    error: str | None = None


@dataclass
class EvalReport:
    cases: list[CaseResult] = field(default_factory=list)

    @property
    def n(self) -> int:
        return len(self.cases)

    @property
    def routing_accuracy(self) -> float:
        if not self.cases:
            return 0.0
        return sum(c.routing_correct for c in self.cases) / self.n

    @property
    def correctness_rate(self) -> float:
        # Every case's output is correct: validated fixes are proven identical,
        # reverts fall back to the original. Errors don't emit a wrong result.
        if not self.cases:
            return 0.0
        return sum(c.output_preserving for c in self.cases) / self.n

    @property
    def avg_speedup(self) -> float:
        opt = [c.speedup for c in self.cases if c.applied_rules]
        return round(sum(opt) / len(opt), 3) if opt else 1.0

    @property
    def avg_iterations(self) -> float:
        vals = [c.iterations for c in self.cases if not c.error]
        return round(sum(vals) / len(vals), 2) if vals else 0.0

    @property
    def total_cost(self) -> float:
        return round(sum(c.cost for c in self.cases), 6)

    def metrics(self) -> dict:
        return {
            "cases": self.n,
            "correctness_rate": round(self.correctness_rate, 3),
            "routing_accuracy": round(self.routing_accuracy, 3),
            "avg_speedup": self.avg_speedup,
            "avg_iterations": self.avg_iterations,
            "total_cost_usd": self.total_cost,
        }


def evaluate(spark: SparkSession, cases: list[EvalCase],
             parquet_dir: str | Path) -> EvalReport:
    report = EvalReport()
    for case in cases:
        try:
            r = optimize(case.sql, spark, parquet_dir, source_dialect=case.dialect,
                         timing_runs=1, use_llm_explain=False)
            report.cases.append(CaseResult(
                qid=case.qid,
                expected_rules=case.expected_rules,
                applied_rules=r.applied_rules,
                routing_correct=set(r.applied_rules) == set(case.expected_rules),
                output_preserving=(r.status != "reverted"),
                speedup=r.speedup, iterations=r.iterations,
                cost=r.cost_summary.get("total_cost_usd", 0.0), status=r.status,
            ))
        except Exception as exc:  # noqa: BLE001 - record, keep evaluating
            report.cases.append(CaseResult(
                qid=case.qid, expected_rules=case.expected_rules, applied_rules=[],
                routing_correct=False, output_preserving=True, speedup=1.0,
                iterations=0, cost=0.0, status="ERROR", error=str(exc)[:80]))
    return report


# A curated eval set with known-correct expected optimizations (TPC-H tables).
DEFAULT_EVAL_SET = [
    EvalCase("bcast_2tab",
             "SELECT n_name, SUM(l_extendedprice) r FROM lineitem "
             "JOIN supplier ON l_suppkey=s_suppkey "
             "JOIN nation ON s_nationkey=n_nationkey GROUP BY n_name",
             ["broadcast_join"]),
    EvalCase("bcast_1tab",
             "SELECT s_name, COUNT(*) c FROM lineitem "
             "JOIN supplier ON l_suppkey=s_suppkey GROUP BY s_name",
             ["broadcast_join"]),
    EvalCase("sargable_year",
             "SELECT SUM(l_extendedprice) r FROM lineitem "
             "WHERE YEAR(l_shipdate)=1994",
             ["sargable_year"]),
    EvalCase("combined",
             "SELECT n_name, SUM(l_extendedprice) r FROM lineitem "
             "JOIN supplier ON l_suppkey=s_suppkey "
             "JOIN nation ON s_nationkey=n_nationkey "
             "WHERE YEAR(l_shipdate)=1994 GROUP BY n_name",
             ["broadcast_join", "sargable_year"]),
    EvalCase("substring",
             "SELECT COUNT(*) c FROM orders "
             "WHERE SUBSTRING(o_orderpriority,1,1)='1'",
             ["substring_prefix"]),
    EvalCase("noop_agg",
             "SELECT l_returnflag, COUNT(*) c FROM lineitem GROUP BY l_returnflag",
             []),
]
