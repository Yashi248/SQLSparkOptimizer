from sqlspark_optimizer.agents.plan_analyzer import AnalysisResult, parse_plan_tree
from sqlspark_optimizer.orchestrator.graph import OptimizerGraph, detect_symptoms
from sqlspark_optimizer.routing import ModelRouter, Tier


def test_router_template_fallback():
    r = ModelRouter()
    r._gemini = None       # force no smart backend
    r._ollama_up = False   # force no local model
    text, sc = r.reason("explain", "prompt", "FALLBACK")
    assert text == "FALLBACK" and sc.model == "template" and sc.est_cost_usd == 0.0


def test_router_record_local():
    r = ModelRouter()
    sc = r.record_local("translate")
    assert sc.tier == Tier.LOCAL and sc.est_cost_usd == 0.0
    assert r.summary()["stages"] == 1


def test_graph_compiles_with_all_nodes():
    g = OptimizerGraph(spark=None, parquet_dir=".").build()
    nodes = g.get_graph().nodes
    for n in ("translate", "analyze", "retrieve", "optimize", "validate", "explain"):
        assert n in nodes


def test_detect_symptoms_finds_both():
    fake = AnalysisResult(plan_text="", nodes=[], scanned_tables={},
                          shuffle_joins=["x"], broadcast_candidates=["nation"])
    syms = detect_symptoms("SELECT * FROM t WHERE YEAR(d) = 1994", fake, set())
    assert {s["fallback"] for s in syms} == {"broadcast_join", "sargable_year"}


def test_detect_symptoms_skips_applied():
    fake = AnalysisResult(plan_text="", nodes=[], scanned_tables={},
                          shuffle_joins=["x"], broadcast_candidates=["nation"])
    syms = detect_symptoms("SELECT * FROM t", fake, already={"broadcast_join"})
    assert syms == []  # broadcast already applied, no predicate present


def test_parse_plan_tree_edges():
    plan = "HashAggregate\n+- Project\n   +- FileScan"
    g = parse_plan_tree(plan)
    assert len(g.nodes) == 3 and len(g.edges) == 2


def test_extract_sql_strips_fences():
    from sqlspark_optimizer.orchestrator.graph import _extract_sql
    assert _extract_sql("```sql\nSELECT a FROM t;\n```") == "SELECT a FROM t"
    assert _extract_sql("SELECT b FROM u") == "SELECT b FROM u"
    assert _extract_sql("") == ""


def test_decide_escalates_when_no_rule_and_llm_available():
    og = OptimizerGraph(spark=None, parquet_dir=".")
    og.router._openai = object()   # pretend an LLM is configured
    assert og.decide({"status": "no_change", "has_shuffle": True,
                      "escalated": False}) == "escalate"
    # ...but not if there's no inefficiency signal, or escalation already ran
    assert og.decide({"status": "no_change", "has_shuffle": False,
                      "escalated": False}) == "explain"
    assert og.decide({"status": "no_change", "has_shuffle": True,
                      "escalated": True}) == "explain"


def test_decide_reverted_goes_to_explain():
    # a bad LLM rewrite that fails validation must not loop — it ends safely
    og = OptimizerGraph(spark=None, parquet_dir=".")
    assert og.decide({"status": "reverted"}) == "explain"


def test_bad_candidate_reverts_not_crashes():
    # A candidate that fails to run (e.g. malformed LLM SQL) must revert safely,
    # never raise out of validate_node.
    class _BadSpark:
        def sql(self, q):
            raise ValueError("[PARSE_SYNTAX_ERROR] Syntax error near 'SELECT'")
    og = OptimizerGraph(spark=_BadSpark(), parquet_dir=".")
    og._base = (__import__("pandas").DataFrame({"x": [1]}), 0.1)  # skip baseline run
    out = og.validate_node({"made_change": True, "candidate_sql": "GARBAGE SQL",
                            "spark_sql": "SELECT 1", "selected_rules": ["llm_escalation"],
                            "iteration": 2, "log": []})
    assert out["status"] == "reverted" and out["validation_passed"] is False


def test_escalation_rejected_if_not_faster(monkeypatch):
    # An LLM rewrite that is output-identical but NOT faster must be rejected,
    # keeping the prior (faster) optimization instead of replacing it.
    import pandas as pd
    import sqlspark_optimizer.orchestrator.graph as g

    class _DF:
        def toPandas(self):
            return pd.DataFrame({"x": [1]})

    class _Spark:
        def sql(self, q):
            return _DF()

    og = OptimizerGraph(spark=_Spark(), parquet_dir=".")
    og._base = (pd.DataFrame({"x": [1]}), 3.0)                 # baseline time
    monkeypatch.setattr(g, "frames_match", lambda a, b: (True, "ok"))
    monkeypatch.setattr(g, "time_query", lambda s, q, runs=1: 3.0)  # candidate == baseline -> ~1x
    out = og.validate_node({"made_change": True, "candidate_sql": "SELECT 1",
                            "spark_sql": "SELECT 1", "selected_rules": ["llm_escalation"],
                            "iteration": 2, "applied_rules": ["broadcast_join"],
                            "speedup": 3.0, "log": []})
    assert out["status"] == "optimized" and out["validation_passed"] is True  # prior kept
    assert "llm_escalation" in out["tried_rules"]
    assert "current_sql" not in out          # did NOT replace with the slower rewrite


def test_escalate_node_proposes_candidate(monkeypatch):
    og = OptimizerGraph(spark=None, parquet_dir=".")
    monkeypatch.setattr(og.router, "reason",
                        lambda *a, **k: ("```sql\nSELECT x FROM t WHERE d > 5\n```", None))
    out = og.escalate_node({"current_sql": "SELECT x FROM t WHERE d + 0 > 5", "log": []})
    assert out["made_change"] and out["selected_rules"] == ["llm_escalation"]
    assert out["candidate_sql"] == "SELECT x FROM t WHERE d > 5"
