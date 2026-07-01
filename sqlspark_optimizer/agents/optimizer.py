"""
Optimizer agent runs a registry of optimization rules over a query.

The optimizer itself is now tiny: it iterates the rule registry, letting each
rule transform the SQL in turn (rules chain a later rule sees the earlier
rule's output). All the pattern logic lives in agents/rules.py, so adding a
pattern never touches this loop.

Each applied rule is reported so the runner can validate + measure + log it.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from sqlspark_optimizer.agents.rules import DEFAULT_RULES, RuleContext, RuleResult, Rule
from sqlspark_optimizer.observability.tracing import traced


@dataclass
class OptimizationReport:
    original_sql: str
    optimized_sql: str
    applied: list[RuleResult] = field(default_factory=list)

    @property
    def did_optimize(self) -> bool:
        return bool(self.applied)

    @property
    def patterns(self) -> list[str]:
        return [r.rule for r in self.applied]

    @property
    def pattern(self) -> str:
        return ", ".join(self.patterns) if self.applied else "none"

    @property
    def broadcast_tables(self) -> list[str]:
        return [t for r in self.applied
                for t in r.detail.get("broadcast_tables", [])]

    @property
    def rewritten_columns(self) -> list[str]:
        return [c for r in self.applied for c in r.detail.get("columns", [])]


class Optimizer:
    def __init__(self, rules: list[Rule] | None = None):
        self.rules = rules if rules is not None else DEFAULT_RULES

    @traced("optimizer")
    def optimize(self, spark_sql: str, ctx: RuleContext | None = None,
                 only: list[str] | None = None) -> OptimizationReport:
        """Run the rule registry over the query. If `only` is given, apply just
        those rules by name (this is how retrieval drives optimization — the
        analyzer detects a symptom, pgvector picks the fix, we apply that rule)."""
        ctx = ctx or RuleContext()
        current = spark_sql
        applied: list[RuleResult] = []
        for rule in self.rules:
            if only is not None and rule.name not in only:
                continue
            result = rule.apply(current, ctx)
            if result is not None:
                current = result.optimized_sql
                applied.append(result)
        return OptimizationReport(original_sql=spark_sql, optimized_sql=current, applied=applied)
