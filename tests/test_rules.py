from sqlspark_optimizer.agents.rules import (
    BroadcastJoinRule, RuleContext, SargableYearRule, SubstringPrefixRule,
)


def test_sargable_year_fires():
    r = SargableYearRule().apply("SELECT * FROM t WHERE YEAR(d) = 1994", RuleContext())
    assert r is not None
    flat = r.optimized_sql.replace("\n", " ")
    assert "d >= CAST('1994-01-01'" in flat and "d < CAST('1995-01-01'" in flat


def test_sargable_year_noop_on_plain_predicate():
    assert SargableYearRule().apply("SELECT * FROM t WHERE d > 5", RuleContext()) is None


def test_substring_prefix_fires():
    r = SubstringPrefixRule().apply(
        "SELECT * FROM t WHERE SUBSTRING(c, 1, 2) = 'US'", RuleContext())
    assert r is not None and "LIKE 'US%'" in r.optimized_sql


def test_substring_prefix_wildcard_guard():
    # A prefix containing LIKE wildcards must NOT be rewritten (would change meaning).
    assert SubstringPrefixRule().apply(
        "SELECT * FROM t WHERE SUBSTRING(c, 1, 2) = 'a%'", RuleContext()) is None


def test_broadcast_needs_candidates():
    assert BroadcastJoinRule().apply("SELECT * FROM a JOIN b", RuleContext()) is None


def test_broadcast_fires_with_candidates():
    r = BroadcastJoinRule().apply(
        "SELECT * FROM a JOIN b", RuleContext(broadcast_candidates=["b"]))
    assert r is not None and "BROADCAST(b)" in r.optimized_sql
