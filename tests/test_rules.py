from sqlspark_optimizer.agents.rules import (
    BroadcastJoinRule, RuleContext, SargableDateRule, SargableYearRule,
    SubstringPrefixRule,
)


def test_sargable_year_fires():
    r = SargableYearRule().apply("SELECT * FROM t WHERE YEAR(d) = 1994", RuleContext())
    assert r is not None
    flat = r.optimized_sql.replace("\n", " ")
    assert "d >= CAST('1994-01-01'" in flat and "d < CAST('1995-01-01'" in flat


def test_sargable_year_noop_on_plain_predicate():
    assert SargableYearRule().apply("SELECT * FROM t WHERE d > 5", RuleContext()) is None


def test_sargable_date_fires_on_cast():
    r = SargableDateRule().apply(
        "SELECT * FROM t WHERE CAST(d AS DATE) = '2024-01-01'", RuleContext())
    assert r is not None
    flat = r.optimized_sql.replace("\n", " ")
    # half-open [day, next day) range on the bare column
    assert "d >= CAST('2024-01-01'" in flat and "d < CAST('2024-01-02'" in flat


def test_sargable_date_fires_on_date_fn():
    # DATE(col) parses to the same CAST-to-DATE node, so it rewrites too.
    r = SargableDateRule().apply(
        "SELECT * FROM t WHERE DATE(ts) = '2024-02-28'", RuleContext())
    assert r is not None
    flat = r.optimized_sql.replace("\n", " ")
    assert "ts >= CAST('2024-02-28'" in flat and "ts < CAST('2024-02-29'" in flat


def test_sargable_date_leap_year_rollover():
    # 2024 is a leap year: the day after Feb 29 is Mar 1, not an invalid Feb 30.
    r = SargableDateRule().apply(
        "SELECT * FROM t WHERE CAST(d AS DATE) = '2024-02-29'", RuleContext())
    assert r is not None and "d < CAST('2024-03-01'" in r.optimized_sql.replace("\n", " ")


def test_sargable_date_noop_on_non_date_literal():
    # A CAST-to-DATE compared to a non-date string must not be rewritten.
    assert SargableDateRule().apply(
        "SELECT * FROM t WHERE CAST(d AS DATE) = 'not-a-date'", RuleContext()) is None


def test_sargable_date_noop_on_other_cast():
    # CAST to a non-DATE type is out of scope for this rule.
    assert SargableDateRule().apply(
        "SELECT * FROM t WHERE CAST(x AS INT) = '5'", RuleContext()) is None


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
