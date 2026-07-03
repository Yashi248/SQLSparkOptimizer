"""
Synthetic workload generator — parameterize a handful of TPC-H templates into a
large, realistic mix of queries. A real workload isn't one query; it's thousands,
some optimizable, some not. This produces that mix so we can test at scale.

Templates cover: broadcast-join opportunities (big table + small dim), non-sargable
date/substring predicates, an arithmetic predicate (LLM-escalation territory), and
plain aggregations with no anti-pattern.
"""
from __future__ import annotations

import random

_YEARS = [1992, 1993, 1994, 1995, 1996, 1997]


def _broadcast_supplier(y: int) -> str:
    return (f"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue "
            f"FROM lineitem JOIN supplier ON l_suppkey = s_suppkey "
            f"JOIN nation ON s_nationkey = n_nationkey "
            f"WHERE l_shipdate >= DATE '{y}-01-01' AND l_shipdate < DATE '{y + 1}-01-01' "
            f"GROUP BY n_name")


def _broadcast_part(y: int) -> str:
    return (f"SELECT p_brand, SUM(l_quantity) AS qty "
            f"FROM lineitem JOIN part ON l_partkey = p_partkey "
            f"WHERE l_shipdate >= DATE '{y}-01-01' AND l_shipdate < DATE '{y + 1}-01-01' "
            f"GROUP BY p_brand")


def _year(y: int) -> str:
    return f"SELECT COUNT(*) AS c FROM lineitem WHERE YEAR(l_shipdate) = {y}"


def _substr(d: int) -> str:
    return f"SELECT COUNT(*) AS c FROM orders WHERE SUBSTRING(o_orderpriority, 1, 1) = '{d}'"


def _arith(n: int) -> str:
    return f"SELECT SUM(l_extendedprice) AS r FROM lineitem WHERE l_discount * 100 = {n}"


def _noop(q: int) -> str:
    return (f"SELECT l_returnflag, l_linestatus, COUNT(*) AS c FROM lineitem "
            f"WHERE l_quantity > {q} GROUP BY l_returnflag, l_linestatus")


# (name, template fn, parameter space). Round-robined for an even-ish mix.
_SPECS = [
    ("bcast_sup", _broadcast_supplier, _YEARS),
    ("bcast_part", _broadcast_part, _YEARS),
    ("year", _year, _YEARS),
    ("substr", _substr, [1, 2, 3, 4, 5]),
    ("arith", _arith, list(range(1, 10))),
    ("noop", _noop, [10, 20, 30, 40, 48]),
]


def generate_workload(n: int = 1000, seed: int = 0) -> dict[str, str]:
    """Generate `n` queries as {id: sql}, round-robining templates with random
    parameters (deterministic for a given seed)."""
    rnd = random.Random(seed)
    out: dict[str, str] = {}
    i = 0
    while len(out) < n:
        name, fn, params = _SPECS[i % len(_SPECS)]
        out[f"{name}_{i}"] = fn(rnd.choice(params))
        i += 1
    return out
