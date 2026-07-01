from sqlspark_optimizer.agents.optimizer import Optimizer


def test_only_filter_applies_selected_rule():
    rep = Optimizer().optimize(
        "SELECT * FROM t WHERE YEAR(d) = 1994", only=["sargable_year"])
    assert rep.patterns == ["sargable_year"] and rep.did_optimize


def test_only_filter_excludes_unselected():
    # sargable_year would fire, but it's not in `only` -> nothing applied.
    rep = Optimizer().optimize(
        "SELECT * FROM t WHERE YEAR(d) = 1994", only=["broadcast_join"])
    assert not rep.did_optimize


def test_noop_query():
    rep = Optimizer().optimize("SELECT * FROM t WHERE d > 5")
    assert not rep.did_optimize and rep.pattern == "none"
