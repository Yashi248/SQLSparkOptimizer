"""
Analyze-only triage — the cheap half of "analyze all, validate the few".

For each query it translates, captures the physical plan (`explain`, no execution),
and detects deterministic anti-patterns — NO toPandas, NO timing, NO LLM. So it
scans thousands of queries in seconds and ranks them by *predicted* opportunity
(the bytes of a large table being needlessly shuffled). You then run the expensive
validate+measure only on the top-K.

This is how a real tool scales — and how you'd audit a warehouse without executing
every production query.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlspark_optimizer.agents.plan_analyzer import PlanAnalyzer
from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.orchestrator.graph import detect_symptoms


@dataclass
class AnalyzeResult:
    qid: str
    rules: list[str]          # rules that WOULD apply (deterministic)
    opportunity_bytes: int    # predicted shuffle bytes avoidable (rank key)
    error: str | None = None

    @property
    def has_opportunity(self) -> bool:
        return bool(self.rules)


def analyze_only(qid: str, sql: str, analyzer: PlanAnalyzer,
                 translator: Translator) -> AnalyzeResult:
    """Detect deterministic anti-patterns from the plan alone (no execution)."""
    try:
        spark_sql = translator.translate(sql).spark_sql
        analysis = analyzer.analyze(spark_sql)
        rules = [s["fallback"] for s in detect_symptoms(spark_sql, analysis, set())]
        # Opportunity = bytes of the large tables involved in a shuffle join that a
        # broadcast would spare. Predicate-only wins get a nominal weight so they
        # still rank above no-op queries.
        opp = 0
        if analysis.has_anti_pattern:
            opp = sum(sz for sz in analysis.scanned_tables.values()
                      if sz >= analyzer.threshold_bytes)
        elif rules:
            opp = 1
        return AnalyzeResult(qid, rules, opp)
    except Exception as exc:  # noqa: BLE001 - one bad query must not stop the sweep
        return AnalyzeResult(qid, [], 0, str(exc)[:80])
