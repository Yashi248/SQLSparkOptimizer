"""
Phase 3 - LangGraph orchestrator that wires the agents into one flow.

    translate -> analyze -> retrieve -> optimize -> validate -> decide
                    ^                                             |
                    └─────────────── loop (improved, not done) ──┘ -> END

What makes this a *system* rather than a script:
  - Explicit state graph with conditional routing (LangGraph).
  - Retrieval DRIVES optimization: the analyzer names a symptom, pgvector picks
    which fix applies, the optimizer applies exactly that rule.
  - A real loop: each pass re-analyzes the now-optimized query for MORE
    anti-patterns; converges when none remain.
  - Safety: if validation ever fails, revert to the last good SQL.

Observability: every agent call is @traced; the `retrieve` node is @traced too
(this is where MLflow tracing gets added to the vector step, as promised).
pgvector is best-effort  if it's down, retrieval falls back to a direct
symptom->rule map so the pipeline still runs.
"""
from __future__ import annotations

import re
from typing import TypedDict

import sqlglot
from sqlglot import exp

from sqlspark_optimizer.agents.optimizer import Optimizer
from sqlspark_optimizer.agents.plan_analyzer import AnalysisResult, PlanAnalyzer
from sqlspark_optimizer.agents.rules import RuleContext
from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.agents.validator import frames_match
from sqlspark_optimizer.bench import time_query
from sqlspark_optimizer.observability.tracing import traced
from sqlspark_optimizer.routing import ModelRouter

from langgraph.graph import START, END, StateGraph

MAX_ITERS = 3
_UNSET = object()

# Deterministic explanations used when no LLM is configured (the fallback the
# smart-tier router returns to). Keeps the explain stage useful with zero setup.
_RULE_EXPLAIN = {
    "broadcast_join": "The query joined a large table to small lookup tables with "
                      "a shuffle sort-merge join; broadcasting the small tables "
                      "avoids shuffling the large one across the network.",
    "sargable_year": "A YEAR() function wrapped a date column, which blocks "
                     "predicate pushdown; rewriting it to a date range lets Spark "
                     "push the filter into the Parquet scan.",
    "substring_prefix": "A SUBSTRING prefix test blocked pushdown; rewriting it to "
                        "LIKE 'x%' pushes down as a StringStartsWith filter.",
    "llm_escalation": "No deterministic rule matched, so an LLM proposed a novel "
                      "rewrite; it was accepted only after the Validator proved the "
                      "output is identical.",
}


def _norm(sql: str) -> str:
    """Normalize SQL for equality checks: lowercase, collapse whitespace."""
    return re.sub(r"\s+", " ", sql.strip().lower())


def _extract_sql(text: str) -> str:
    """Pull the SQL out of an LLM response — strip ``` fences / prose if present."""
    if not text:
        return ""
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    body = m.group(1) if m else text
    return body.strip().rstrip(";").strip()


class GState(TypedDict, total=False):
    source_sql: str
    spark_sql: str          # translated original (the validation baseline)
    current_sql: str        # evolves as optimizations are applied
    iteration: int
    broadcast_candidates: list[str]
    symptoms: list[dict]    # [{text, fallback}]
    selected_rules: list[str]
    tried_rules: list[str]  # selected but rejected (don't re-pick)
    retrieved: list[dict]   # [{symptom, rule}]
    candidate_sql: str
    made_change: bool
    applied_rules: list[str]
    validation_passed: bool
    speedup: float
    status: str
    log: list[str]
    explanation: str
    cost_summary: dict
    has_shuffle: bool       # is there an inefficiency signal worth escalating?
    escalated: bool         # has the LLM escalation path already run?


def detect_symptoms(spark_sql: str, analysis: AnalysisResult,
                    already: set[str]) -> list[dict]:
    """Turn analysis + a quick AST scan into plain-language symptom descriptions,
    each with a fallback rule (used only if pgvector retrieval is unavailable).
    Skips anything already applied so the loop terminates."""
    syms: list[dict] = []
    if analysis.has_anti_pattern and "broadcast_join" not in already:
        tbls = ", ".join(analysis.broadcast_candidates)
        syms.append({
            "text": f"a large table is joined to small lookup tables ({tbls}) "
                    f"using a sort-merge join",
            "fallback": "broadcast_join",
        })
    tree = sqlglot.parse_one(spark_sql, read="spark")
    if tree.find(exp.Year) and "sargable_year" not in already:
        syms.append({
            "text": "a filter wraps a date column in the YEAR() function which "
                    "blocks predicate pushdown",
            "fallback": "sargable_year",
        })
    if tree.find(exp.Substring) and "substring_prefix" not in already:
        syms.append({
            "text": "a filter uses SUBSTRING on a column to test a leading prefix",
            "fallback": "substring_prefix",
        })
    return syms


class OptimizerGraph:
    def __init__(self, spark, parquet_dir=None, source_dialect: str = "spark",
                 timing_runs: int = 3, use_llm_explain: bool = True):
        self.spark = spark
        self.translator = Translator(source_dialect=source_dialect)
        self.analyzer = PlanAnalyzer(spark, parquet_dir)
        self.optimizer = Optimizer()
        self.router = ModelRouter()   # cost routing across model tiers
        self.timing_runs = timing_runs        # fewer = faster bulk runs
        self.use_llm_explain = use_llm_explain  # False -> template (fast/deterministic)
        self._base = None          # cached (baseline_df, baseline_time)
        self._kb_conn = _UNSET     # lazy pgvector connection

    #nodes
    def translate_node(self, state: GState) -> GState:
        spark_sql = self.translator.translate(state["source_sql"]).spark_sql
        self.router.record_local("translate")  # mechanical -> local tier, $0
        return {"spark_sql": spark_sql, "current_sql": spark_sql,
                "applied_rules": [], "log": ["translated source -> Spark SQL"]}

    def analyze_node(self, state: GState) -> GState:
        analysis = self.analyzer.analyze(state["current_sql"])
        already = set(state.get("applied_rules", []))
        symptoms = detect_symptoms(state["current_sql"], analysis, already)
        log = state.get("log", []) + [
            f"iter {state.get('iteration', 0) + 1}: found symptoms "
            f"{[s['fallback'] for s in symptoms] or '(none)'}"]
        # Signal that there's inefficiency worth escalating to an LLM if the
        # deterministic rules can't handle it (a shuffle join or exchange).
        has_shuffle = bool(analysis.shuffle_joins) or ("Exchange" in analysis.plan_text)
        return {"iteration": state.get("iteration", 0) + 1,
                "broadcast_candidates": analysis.broadcast_candidates,
                "symptoms": symptoms, "has_shuffle": has_shuffle, "log": log}

    @traced("retrieve")
    def retrieve_node(self, state: GState) -> GState:
        """For each symptom, ask pgvector which optimization pattern applies.
        Traced -> shows up in MLflow as part of the pipeline."""
        selected: list[str] = []
        retrieved: list[dict] = []
        # Skip rules already applied AND rules tried-but-rejected (so a rejected
        # rule isn't re-picked, which would loop).
        excluded = set(state.get("applied_rules", [])) | set(state.get("tried_rules", []))
        for s in state.get("symptoms", []):
            rule = self._retrieve_rule(s["text"], s["fallback"])
            retrieved.append({"symptom": s["text"], "rule": rule})
            if rule and rule not in selected and rule not in excluded:
                selected.append(rule)
        return {"selected_rules": selected, "retrieved": retrieved}

    def optimize_node(self, state: GState) -> GState:
        ctx = RuleContext(broadcast_candidates=state.get("broadcast_candidates", []))
        report = self.optimizer.optimize(
            state["current_sql"], ctx, only=state.get("selected_rules", []))
        self.router.record_local("optimize")  # mechanical -> local tier, $0
        changed = report.optimized_sql != state["current_sql"]
        return {"candidate_sql": report.optimized_sql, "made_change": changed}

    def validate_node(self, state: GState) -> GState:
        log = state.get("log", [])
        rules = state.get("selected_rules", [])
        it = state.get("iteration", 0)
        if not state.get("made_change"):
            # No fix applied this pass. `decide` may still route to LLM escalation.
            return {"status": "no_change",
                    "log": log + ["no deterministic fix applied"]}

        base_df, base_t = self._baseline(state["spark_sql"])
        # The candidate may be an untrusted LLM rewrite that doesn't even parse.
        # A candidate that fails to RUN is a failed validation, not a crash.
        try:
            cand_df = self.spark.sql(state["candidate_sql"]).toPandas()
            passed, reason = frames_match(base_df, cand_df)
        except Exception as exc:  # noqa: BLE001 - bad candidate -> reject, don't crash
            passed, reason = False, f"candidate failed to execute: {str(exc)[:60]}"

        speedup = state.get("speedup", 1.0)
        if passed:
            cand_t = time_query(self.spark, state["candidate_sql"], runs=self.timing_runs)
            speedup = base_t / cand_t if cand_t else float("nan")
            # An untrusted LLM rewrite is only worth accepting if it's actually
            # FASTER than what we already have — an output-identical-but-not-faster
            # rewrite must not replace a good deterministic fix (e.g. drop a
            # broadcast hint). Deterministic rules are trusted patterns; accept them
            # on correctness even when runtime is flat.
            prev_best = state.get("speedup", 1.0)
            if rules == ["llm_escalation"] and speedup + 0.05 < prev_best:
                passed = False
                reason = (f"LLM rewrite valid but not faster "
                          f"({speedup:.2f}x < best {prev_best:.2f}x)")

        if passed:
            return {"current_sql": state["candidate_sql"], "validation_passed": True,
                    "speedup": speedup, "status": "optimized",
                    "applied_rules": state.get("applied_rules", []) + rules,
                    "log": log + [f"iter {it}: applied {rules} -> PASS, {speedup:.2f}x"]}

        # Reject the candidate WITHOUT clobbering a prior successful optimization:
        # keep the last-good current_sql/speedup and remember not to re-pick `rules`.
        had_prior = bool(state.get("applied_rules"))
        return {"validation_passed": had_prior,
                "status": "optimized" if had_prior else "reverted",
                "tried_rules": state.get("tried_rules", []) + rules,
                "log": log + [f"iter {it}: {rules} rejected ({reason}) -> kept previous"]}

    def explain_node(self, state: GState) -> GState:
        """Reasoning stage -> routed to the SMART model tier. Explains what was
        wrong and why each fix helps, grounded in the retrieved patterns. Falls
        back to a deterministic template if no LLM is configured."""
        rules = state.get("applied_rules", [])
        speedup = state.get("speedup", 0.0)
        if not rules:
            self.router.record_local("explain", "skipped")
            return {"explanation": "No optimizations were applied.",
                    "cost_summary": self.router.summary()}
        symptoms = "; ".join(r["symptom"] for r in state.get("retrieved", []))
        prompt = (
            "You are a Spark optimization assistant. A SQL query had these issues: "
            f"{symptoms}. We applied these fixes: {', '.join(rules)}, achieving a "
            f"{speedup:.2f}x speedup with identical results. In 3-4 sentences, "
            "explain to a data engineer what was wrong and why each fix helps."
        )
        fallback = (" ".join(_RULE_EXPLAIN.get(r, r) for r in rules)
                    + f" Net result: a {speedup:.2f}x speedup with identical output.")
        if not self.use_llm_explain:
            # Bulk mode: skip the LLM call (fast + deterministic), still record cost.
            self.router.record_local("explain", "template")
            return {"explanation": fallback, "cost_summary": self.router.summary()}
        text, _ = self.router.reason("explain", prompt, fallback)
        return {"explanation": text, "cost_summary": self.router.summary()}

    def escalate_node(self, state: GState) -> GState:
        """No deterministic rule fit — ask the SMART LLM for a NOVEL rewrite. The
        proposal is untrusted, so it goes straight back through the Validator: it
        is accepted ONLY if proven output-identical (and faster). This is where the
        LLM's reach and the Validator's safety net combine."""
        sql = state["current_sql"]
        prompt = (
            "You are a Spark SQL optimization expert. Rewrite the query below to run "
            "faster WITHOUT changing its results (identical rows and values). Prefer "
            "sargable/pushdown-friendly predicates, broadcast hints for small tables, "
            "and removing redundant work. Output ONLY the rewritten SQL, no prose.\n\n"
            f"{sql}"
        )
        text, _ = self.router.reason("escalate", prompt, "")
        candidate = _extract_sql(text)
        log = state.get("log", [])
        if not candidate or _norm(candidate) == _norm(sql):
            return {"escalated": True, "made_change": False,
                    "log": log + ["escalation: LLM proposed no usable change"]}
        return {"escalated": True, "made_change": True, "candidate_sql": candidate,
                "selected_rules": ["llm_escalation"],
                "log": log + ["escalation: validating LLM-proposed rewrite"]}

    def decide(self, state: GState) -> str:
        status = state.get("status")
        if status == "reverted":
            return "explain"        # a fix (incl. a bad LLM one) failed validation
        if status == "optimized" and state.get("validation_passed"):
            if state.get("escalated"):
                return "explain"    # don't loop after an escalated fix
            if state.get("iteration", 0) < MAX_ITERS:
                return "loop"        # look for more deterministic patterns
            return "explain"
        # status == "no_change": deterministic rules found nothing this pass.
        if (not state.get("escalated") and state.get("has_shuffle")
                and self.router.llm_available):
            return "escalate"        # try a novel LLM rewrite
        return "explain"

    # helpers
    def _baseline(self, original_sql: str):
        if self._base is None:
            df = self.spark.sql(original_sql).toPandas()
            self._base = (df, time_query(self.spark, original_sql, runs=self.timing_runs))
        return self._base

    def _retrieve_rule(self, text: str, fallback: str) -> str:
        conn = self._kb()
        if conn is None:
            return fallback
        try:
            from sqlspark_optimizer.knowledge import kb
            top = kb.retrieve(conn, text, 1)[0]
            return top.rule or fallback
        except Exception:  # noqa: BLE001 - retrieval is best-effort
            return fallback

    def _kb(self):
        if self._kb_conn is _UNSET:
            try:
                from sqlspark_optimizer.knowledge import kb
                self._kb_conn = kb.connect()
            except Exception:  # noqa: BLE001
                print("[orchestrator] pgvector unavailable - using direct "
                      "symptom->rule fallback")
                self._kb_conn = None
        return self._kb_conn

    # graph 
    def build(self):
        b = StateGraph(GState)
        b.add_node("translate", self.translate_node)
        b.add_node("analyze", self.analyze_node)
        b.add_node("retrieve", self.retrieve_node)
        b.add_node("optimize", self.optimize_node)
        b.add_node("validate", self.validate_node)
        b.add_node("escalate", self.escalate_node)
        b.add_node("explain", self.explain_node)
        b.add_edge(START, "translate")
        b.add_edge("translate", "analyze")
        b.add_edge("analyze", "retrieve")
        b.add_edge("retrieve", "optimize")
        b.add_edge("optimize", "validate")
        # validate -> loop (more patterns) | escalate (LLM novel fix) | explain (end)
        b.add_conditional_edges("validate", self.decide,
                                {"loop": "analyze", "escalate": "escalate",
                                 "explain": "explain"})
        b.add_edge("escalate", "validate")   # LLM proposal goes through the Validator
        b.add_edge("explain", END)
        return b.compile()
