# Multi-Agent SQL→PySpark Optimizer — Build Roadmap

**What it is:** A multi-agent system that takes a SQL query, converts it to PySpark, optimizes it, and *proves the output is still correct* — with a full observability/evaluation layer that measures routing, cost, and quality at every stage.

**The one-line pitch:** "I built a multi-agent query optimizer where each stage is independently validated and cost-routed, with an evaluation layer that proves the orchestration is correct — the part most multi-agent demos skip."

**Core principle:** Ship something working at the end of every phase. Instrument from day one — observability is not a final phase, it's wired in as you build.

---

## The agents (what each does)

| Agent | Job | Backed by |
|-------|-----|-----------|
| **Translator** | SQL → PySpark | SQLGlot (AST parsing) |
| **Plan-Analyzer** | Read Spark execution plan, detect anti-patterns | Neo4j (plan = DAG) |
| **Optimizer** | Apply fixes, retrieve tuning patterns | pgvector (RAG over Spark docs) |
| **Validator** | Run both queries, assert identical output | DuckDB + Spark |
| **Orchestrator** | Route the query through stages, loop optimize→validate | LangGraph |
| **Eval/Observability layer** | Log routing, cost/stage, before-after benchmarks, convergence | MLflow |

---

## Everything you need (all free)

**Install locally (free, open-source):**
- **Python 3.11+**, a virtualenv
- **PySpark** — `pip install pyspark` (local single-node Spark, UI on `localhost:4040`)
- **DuckDB** — `pip install duckdb` (has a built-in TPC-H extension that generates both the data *and* the 22 standard queries instantly — your data problem solved in one line)
- **SQLGlot** — `pip install sqlglot`
- **LangGraph** — `pip install langgraph langchain`
- **MLflow** — `pip install mlflow` (run the UI locally with `mlflow ui`)
- **sentence-transformers** — `pip install sentence-transformers` (free local embeddings, no API cost)
- **Ollama** — download from ollama.com, run small models locally for free (your "cheap model" tier)

**Free accounts (no card needed):**
- **Neo4j AuraDB Free** *or* run Neo4j in Docker locally (graph for plan analysis)
- **Supabase Free** (Postgres + pgvector) *or* run Postgres+pgvector in Docker locally
- **Google AI Studio (Gemini)** free tier *or* **Groq** free tier — your "expensive/smart model" tier for reasoning steps
- **Render** (backend deploy, free) + **Vercel** (frontend deploy, free)

**The LLM cost strategy (this IS your cost-routing thesis, built for free):**
- **Cheap/mechanical steps** (translation) → **Ollama local model** (free, $0/token)
- **Hard reasoning steps** (optimization decisions) → **Gemini/Groq free tier**
- This literally demonstrates "route each stage to the right-sized model" — and costs nothing.

> Verify current free-tier limits before relying on them (Gemini/Groq rate limits, Supabase/AuraDB caps shift). Local Docker versions of Neo4j/Postgres avoid all caps if you hit them.

---

## Repo structure

```
sql-spark-optimizer/
├── agents/
│   ├── translator.py
│   ├── plan_analyzer.py
│   ├── optimizer.py
│   └── validator.py
├── orchestrator/
│   └── graph.py          # LangGraph orchestration
├── observability/
│   ├── tracing.py        # MLflow instrumentation
│   └── eval.py           # routing accuracy, cost, benchmarks
├── data/
│   └── tpch_setup.py     # DuckDB TPC-H generation
├── knowledge/
│   └── ingest_spark_docs.py  # embed optimization patterns → pgvector
├── api/
│   └── main.py           # FastAPI
├── frontend/             # React/Vite
├── eval_set/
│   └── queries.json      # test queries + known-correct outputs
└── README.md
```

---

## Phase 0 — Environment + data (Day 1–2)

Goal: a working Spark + data setup you can run a query through.

- [ ] Set up the repo + virtualenv, install everything above.
- [ ] Get Spark running locally — `pip install pyspark`, run a trivial `df.show()`, confirm the Spark UI loads at `localhost:4040`.
- [ ] Generate **TPC-H** data + queries via DuckDB's TPC-H extension (`INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);` then `SELECT * FROM tpch_queries();`).
- [ ] Pick **3 starter queries** (start with simpler ones — TPC-H Q1, Q3, Q6) as your frozen test set.
- [ ] Stand up MLflow locally (`mlflow ui`) — even empty, so you instrument from Phase 1.

**Ships:** you can run a TPC-H SQL query in DuckDB and see its result. Foundation laid.

---

## Phase 1 — The spine: translate + validate (Week 1)

Goal: prove the end-to-end path with just two agents and one query. No optimization yet.

- [ ] **Translator agent:** SQL → PySpark using SQLGlot. (SQLGlot can transpile SQL dialects and produce an AST; use it to generate the PySpark DataFrame code.)
- [ ] **Validator agent:** run the original SQL (DuckDB) and the generated PySpark, collect both result sets, sort, and assert they're identical.
- [ ] **Instrument from the start:** wrap every agent call in MLflow tracing — log inputs, outputs, latency, token count, and pass/fail. This is the habit that makes Phase 4 easy.

**Ships:** feed it one TPC-H query → get working PySpark that's *proven* to return the same result. This alone is a respectable project.

**Why validation first:** it's your ground truth. Everything downstream (optimization) is only trustworthy if you can prove correctness — so build the proof before the thing it proves.

---

## Phase 2 — The optimization loop, one pattern (Week 2)

Goal: add real optimization for a *single* anti-pattern, end-to-end. Narrow but complete.

- [ ] **Plan-Analyzer agent:** capture the Spark physical plan (`df.explain(mode="formatted")` or via `_jdf.queryExecution()`), parse it, and write the operator tree to **Neo4j** as a DAG.
- [ ] Detect **one** anti-pattern in the graph — recommended: **a join that should be a broadcast join** (small table joined to large; easy to detect, big measurable speedup).
- [ ] **Optimizer agent:** ingest a handful of Spark tuning docs into **pgvector** (broadcast joins, predicate pushdown, repartitioning, caching, AQE). Retrieve the relevant pattern for the detected anti-pattern, apply the fix to the PySpark code.
- [ ] Loop: optimize → re-run → **Validator** confirms output unchanged → measure runtime before vs. after.
- [ ] Log the before/after benchmark to MLflow.

**Ships:** feed it a query with a non-broadcast join → it detects it, applies the broadcast, proves the result is identical, and shows a faster runtime with numbers. This is the heart of the project.

---

## Phase 3 — Orchestration + cost routing (Week 3, first half)

Goal: a real orchestrator wiring the agents, plus multi-model routing.

- [ ] **Orchestrator in LangGraph:** define the state graph — translate → analyze → optimize → validate → (loop if not converged) → synthesize. Make routing decisions explicit and logged.
- [ ] **Cost routing:** run the **Translator on Ollama (local, free)** and the **Optimizer reasoning on Gemini/Groq**. Log token count + (estimated) cost per stage per query.
- [ ] **Convergence handling:** cap optimization loops, handle "no improvement found" and "validation failed → revert" gracefully.

**Ships:** the full pipeline runs as one orchestrated flow, and you can show cost-per-stage broken down by model. The cost-routing story is now real and demonstrable.

---

## Phase 4 — The observability/evaluation layer (Week 3, second half)

Goal: the differentiator. Turn your logged telemetry into *measured* claims.

- [ ] **Build the eval set:** 8–12 queries with known-correct outputs and known anti-patterns.
- [ ] **Metrics to compute and publish:**
  - **Correctness:** % of optimizations that passed validation (output unchanged).
  - **Routing accuracy:** did the analyzer flag the right anti-pattern?
  - **Performance:** average runtime improvement (before vs. after).
  - **Cost:** tokens + estimated cost per query, and per stage.
  - **Convergence:** how many loops to reach a stable optimized query.
- [ ] **MLflow dashboards:** traces per run, metric comparisons across queries, per-stage cost breakdown.
- [ ] **Guardrails:** the validator must *block* any optimization that changes output — log these "caught" cases; they're proof the safety layer works.

**Ships:** a dashboard + a numbers table that proves the system works and what it costs. This is what makes it "production-minded," not a demo.

---

## Phase 5 — Interface + deploy (Week 4, first half)

Goal: a live URL anyone can try. Plays to your React/FastAPI strengths.

- [ ] **FastAPI** backend exposing the pipeline.
- [ ] **React/Vite** frontend: paste SQL → see generated PySpark, the detected issue, the optimization applied, the before/after numbers, and the validation pass.
- [ ] **Deploy:** backend on Render, frontend on Vercel. (Note Spark is heavy — for the live demo you may run the Spark step in a constrained mode or pre-compute benchmark numbers; document this honestly.)

**Ships:** a public, working demo.

---

## Phase 6 — Portfolio packaging (Week 4, second half)

Goal: make it count.

- [ ] **README:** architecture diagram, the "why multi-agent (and which splits were optional)" reasoning, the eval numbers, the cost-routing explanation.
- [ ] **90-second demo video** solving one query end-to-end.
- [ ] **Clean GitHub:** pin this + LekhaAI + the RAG assistant.
- [ ] **Resume bullet + LinkedIn featured + outreach hook.**

**Ships:** a portfolio-ready flagship and a strong interview story.

---

## Scaling roadmap (after v1)

Ordered by **value-per-effort**. Do **not** start any of these until v1 (one pattern, validated, measured) ships. Pick based on the roles you're targeting: items 1–3 lean data-engineering; items 2, 5, 6 lean applied-AI/FDE.

**1. More optimization patterns** *(highest value — deepens the core)*
Predicate pushdown, partition pruning, skew handling, caching, AQE tuning. Each is just one analyzer rule + one knowledge doc + one validation. This is what proves data-engineering depth, so it's the first thing to widen.

**2. Plan/optimization visualizer** *(high impact, low risk — build this one)*
Render the execution plan as a visual DAG and show the before/after of each optimization — the viewer literally *sees* a shuffle-join become a broadcast-join. Cheap because the plan graph already lives in Neo4j. Use **React Flow (`@xyflow/react`)**, the standard free node-graph library. This is the *output-side* visual — it amplifies the core story.
> Note: this is the visual feature worth building. A *drag-and-drop query builder* (input side) is a much larger lift that shifts the project's identity toward a visual ETL tool and competes for time better spent on optimization depth — treat that as an optional identity-pivot stretch (item 7), not a core feature.

**3. Difficulty-based cost routing** *(strong cost story)*
A lightweight router judges query difficulty first, sending trivial queries down a cheap fast path and hard ones to the full pipeline — so easy queries never pay full multi-agent cost.

**4. Self-improving loop** *(impressive, moderate effort)*
Failed or weak optimizations feed back to improve future routing/optimization decisions. Closes the loop most projects leave open.

**5. Whole-workload mode** *(real-world thinking)*
Optimize a batch of queries at once, rank by impact ("fixing query 7 saves the most"). Mirrors how teams actually prioritize.

**6. Databricks bonus layer** *(resume keyword, zero core risk)*
Wrap the same agents in Databricks Agent Bricks / Omnigent + MLflow 3.0 — adds the Databricks signal *after* the open-source core works, so nothing depends on beta tooling.

**7. Visual query builder (input side)** *(ambitious, identity-shifting stretch)*
A node-based editor to *compose* queries visually (Alteryx/KNIME-style) that then get optimized. Genuinely cool but roughly doubles scope and changes what the project *is* — only pursue if you decide to pivot toward a visual tool. Same library (`@xyflow/react`).

**8. Infra signals** *(production polish)*
Docker, GitHub Actions CI/CD, an Arize Phoenix observability dashboard on top of MLflow.

---

## Critical path

```
0. Env + TPC-H data ──► 1. Translate + Validate (the spine)
   ──► 2. Plan-graph + Optimize (one pattern) ──► 3. Orchestrate + cost-route
   ──► 4. Eval/observability layer ──► 5. Interface + deploy ──► 6. Package
   ──► [scale: more patterns / difficulty routing / Databricks layer]
```

**Rules that keep you on track:**
1. Ship something working every phase.
2. **One** optimization pattern in v1 (broadcast join). Prove the architecture on a narrow slice, then widen.
3. Validation before optimization — correctness is the foundation.
4. Instrument from Phase 1; don't leave observability for the end.
5. Free-first: Ollama for cheap steps, free-tier API for hard steps, local Docker DBs if free tiers pinch.
6. The eval numbers and the README reasoning are the deliverable as much as the code.
