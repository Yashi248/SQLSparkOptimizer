"""
Optimization rules - a pluggable registry.

Each rule is a self-contained (detect + rewrite) unit with the SAME interface:
`apply(spark_sql, context) -> RuleResult | None`. The Optimizer just runs the
registry; adding a pattern = adding a Rule, no changes to the loop around it.

Two categories so far:
  - join_strategy     : changes HOW Spark executes (physical hint). Safe by
                        construction; Validator is belt-and-suspenders.
  - predicate_pushdown: LOGICAL rewrites that make a filter sargable so it pushes
                        into the scan. Could change results if wrong → Validator
                        is load-bearing.

Why a registry (vs one method per pattern): once there are many rules, "which
rule applies to this query?" becomes a *retrieval* problem exactly what the
pgvector knowledge base will answer later. With one rule that was pointless; with
a registry it's the natural design.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, Protocol

import sqlglot
from sqlglot import exp


@dataclass
class RuleContext:
    """Extra info a rule may need beyond the SQL itself (e.g. plan analysis)."""
    broadcast_candidates: list[str] = field(default_factory=list)


@dataclass
class RuleResult:
    rule: str
    category: str
    optimized_sql: str
    detail: dict


class Rule(Protocol):
    name: str
    category: str
    def apply(self, spark_sql: str, ctx: RuleContext) -> Optional[RuleResult]: ...


# Join-strategy rules
class BroadcastJoinRule:
    name = "broadcast_join"
    category = "join_strategy"

    def apply(self, spark_sql: str, ctx: RuleContext) -> Optional[RuleResult]:
        tables = ctx.broadcast_candidates
        if not tables:
            return None
        hint = f"/*+ BROADCAST({', '.join(tables)}) */"
        new, n = re.subn(r"(?i)\bSELECT\b", f"SELECT {hint}", spark_sql, count=1)
        if n != 1:
            return None
        return RuleResult(self.name, self.category, new, {"broadcast_tables": tables})


# Predicate-pushdown rules (AST rewrites)
class _AstRewriteRule:
    """Base for rules that walk the AST and rewrite EQ comparisons. Subclasses
    implement `_rewrite_eq(node) -> (new_node, detail_key, detail_value) | None`."""
    name = "abstract"
    category = "predicate_pushdown"

    def apply(self, spark_sql: str, ctx: RuleContext) -> Optional[RuleResult]:
        tree = sqlglot.parse_one(spark_sql, read="spark")
        hits: list[str] = []

        def transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.EQ):
                out = self._rewrite_eq(node)
                if out is not None:
                    new_node, hit = out
                    hits.append(hit)
                    return new_node
            return node

        new_sql = tree.transform(transform).sql(dialect="spark", pretty=True)
        if not hits:
            return None
        return RuleResult(self.name, self.category, new_sql, {"columns": hits})

    def _rewrite_eq(self, node: exp.EQ):  # -> (exp.Expression, str) | None
        raise NotImplementedError


class SargableYearRule(_AstRewriteRule):
    """YEAR(col) = N  ->  col >= DATE 'N-01-01' AND col < DATE 'N+1-01-01'."""
    name = "sargable_year"

    def _rewrite_eq(self, node: exp.EQ):
        year_expr, lit = _match(node, exp.Year, lambda y: y.is_number and "." not in y.name)
        if year_expr is None:
            return None
        # SQLGlot models YEAR(x) as Year(TO_DATE(x)); reach the bare Column.
        col = year_expr.find(exp.Column)
        if col is None:
            return None
        y = int(lit.name)
        cs = col.sql(dialect="spark")
        rng = sqlglot.condition(
            f"{cs} >= CAST('{y}-01-01' AS DATE) AND {cs} < CAST('{y + 1}-01-01' AS DATE)",
            dialect="spark",
        )
        return exp.paren(rng), cs


class SubstringPrefixRule(_AstRewriteRule):
    """SUBSTRING(col, 1, k) = 'PREFIX'  ->  col LIKE 'PREFIX%'  (pushes down as
    StringStartsWith). Only fires for a genuine left-anchored prefix test."""
    name = "substring_prefix"

    def _rewrite_eq(self, node: exp.EQ):
        sub, lit = _match(node, exp.Substring, lambda l: l.is_string)
        if sub is None:
            return None
        col = sub.this
        if not isinstance(col, exp.Column):
            return None
        start, length = sub.args.get("start"), sub.args.get("length")
        prefix = lit.name
        # Must start at position 1, cover exactly the prefix, and be wildcard-free.
        if not (isinstance(start, exp.Literal) and start.name == "1"):
            return None
        if length is not None and not (isinstance(length, exp.Literal)
                                       and length.name == str(len(prefix))):
            return None
        if "%" in prefix or "_" in prefix:
            return None
        cs = col.sql(dialect="spark")
        return exp.paren(sqlglot.condition(f"{cs} LIKE '{prefix}%'", dialect="spark")), cs


def _match(node: exp.EQ, fn_type, lit_ok):
    """If one side of `node` is an instance of `fn_type` and the other a Literal
    satisfying `lit_ok`, return (fn_node, literal); else (None, None)."""
    for x, y in ((node.this, node.expression), (node.expression, node.this)):
        if isinstance(x, fn_type) and isinstance(y, exp.Literal) and lit_ok(y):
            return x, y
    return None, None


# Default registry. Order = application order (rules chain on each other's output).
DEFAULT_RULES: list[Rule] = [
    BroadcastJoinRule(),
    SargableYearRule(),
    SubstringPrefixRule(),
]
