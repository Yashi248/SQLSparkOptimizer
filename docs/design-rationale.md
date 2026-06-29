# Design Rationale — Why This Project Is Different

Companion to [learning-notes.md](learning-notes.md). That doc explains the *tech*;
this doc explains the *decisions* — what we chose, why, the advantage, and the
honest trade-off. Organized by phase. Mine this for the README and interviews.

**One-line thesis:** most multi-agent / query-optimizer demos *transform* a query.
This one **proves each transformation is correct and measures what it costs** —
the part those demos skip.

---

## Cross-cutting differentiators (true at every phase)

- **Correctness is proven, not assumed.** Every optimization is re-run and checked
  against ground truth. We never claim "faster" without "and identical output."
- **Instrumented from line one.** Observability (MLflow traces/runs) was wired in
  Phase 1, not bolted on at the end — so every claim is backed by logged data.
- **Narrow but complete.** One pattern, fully validated and measured, beats ten
  half-built ones. The architecture is proven on a slice, then widened.
- **Each agent is swappable.** Translator, Analyzer, Optimizer, Validator are
  independent. We can upgrade one (e.g. hardcoded fix → RAG retrieval) without
  touching the loop around it.
- **Honest about scope.** Where Spark/AQE already helps, we say so — and aim the
  project at what they *don't* do.

---

## Phase 0 — Environment + data

- **DuckDB generates both data AND queries.** One tool gives us the TPC-H dataset
  *and* the 22 standard queries *and* a trusted correctness oracle. Most projects
  hand-roll fixture data; we get a benchmark-grade, reproducible dataset free.
  - *Advantage:* the "do my outputs match?" question has a built-in answer key.
- **Parquet as the interchange format.** DuckDB writes Parquet; Spark reads it.
  Columnar + pushdown + schema-embedded = realistic analytics I/O.
- *Trade-off:* local single-node Spark isn't a cluster — fine for proving logic,
  not for raw scale claims. We're explicit about that.

---

## Phase 1 — The spine: translate + validate

- **Validation built BEFORE optimization.** The correctness proof exists before the
  thing it proves. This inverts the usual order and is the project's foundation.
  - *Advantage:* every later optimization is trustworthy by construction.
- **Translate via dialect transpilation, not DataFrame code-gen.** SQLGlot rewrites
  SQL into the Spark dialect (run via `spark.sql()`) instead of generating brittle
  `.groupBy().agg()` code.
  - *Advantage:* it actually runs on all 22 queries and still produces a real
    physical plan for Phase 2 to optimize. Nothing is lost.
  - *Honest nuance:* translation is the **commodity** step (for portable queries
    it's nearly a no-op). We don't pretend it's the hard part — validation is.
- **Compare by *meaning*, not bytes.** Row-order, float-drift, and dtype quirks are
  all normalized away, so PASS/FAIL reflects real correctness, not representation.
  - *Advantage:* no false failures, no false passes — the proof is sound.

---

## Phase 2 — Optimize → validate → measure (broadcast join)

### The hint-based optimization — what it is
Instead of rewriting the query's logic, the Optimizer injects a **planner hint**:

    SELECT /*+ BROADCAST(nation, supplier) */ ...

A hint is a directive to Spark's optimizer that **overrides its plan choice**
without changing *what* the query computes — only *how* it executes.

### Why hint-based is the right call (the advantages)
- **Surgical & output-preserving.** It changes execution strategy only. The SELECT
  list, filters, and grouping are untouched → results are guaranteed identical
  (and we still prove it). Far safer than rewriting query logic.
- **Reversible & inspectable.** The optimization is one visible comment in the SQL.
  You can read it, diff it, commit it, or remove it. Compare that to AQE silently
  re-planning at runtime with no artifact.
- **Overrides bad estimates.** The whole anti-pattern exists because Spark
  *mis-estimates* table size (missing stats). A hint bypasses the estimate and
  states the intent directly.
- **Composable.** Hints stack (`BROADCAST`, `MERGE`, `REPARTITION`, `COALESCE`),
  so the same injection mechanism extends to future patterns.
- *Trade-off:* a hint forces a choice, so a *wrong* hint (broadcasting a big table)
  could hurt — which is exactly why the Analyzer checks real size first and the
  Validator + measurement catch any regression.

### Why detect from the *physical plan* (not the SQL text)
We read what Spark will *actually execute*, not what the user wrote.
- *Advantage:* we see Spark's real strategy (SortMergeJoin vs Broadcast) and real
  scanned tables — catching problems invisible at the SQL level.

### Why on-disk size as the broadcast signal
A table's Parquet file size is a concrete, honest proxy for "is it small enough to
broadcast?" — independent of whatever (possibly stale) stats Spark has.
- *Advantage:* we catch the exact case Spark gets wrong (missing stats → no
  broadcast) by using a source of truth Spark ignored.

### Pattern 2 — sargable-predicate rewrite (predicate pushdown)
- **What:** rewrite a non-sargable filter `YEAR(l_shipdate) = 1994` into the
  equivalent sargable range `l_shipdate >= DATE '1994-01-01' AND < '1995-01-01'`,
  so Spark can push it into the Parquet scan (`PushedFilters`).
- **Why this pattern, deliberately:** it's the case **neither Catalyst nor AQE
  fixes** — Catalyst treats `YEAR(col)` as opaque, AQE only reacts to shuffle
  stats. It directly answers "what are we doing that Spark doesn't?"
- **Why it matters for the architecture:** it's a **logical rewrite** (changes the
  plan's meaning if wrong), so the Validator is *load-bearing* here, not
  belt-and-suspenders. This is the "risky transform that genuinely needs
  validation" half of the spectrum from Phase 2's validation-cost note.
- **Advantage:** done over the **AST** (SQLGlot), not string hacks — the optimizer
  now reasons about query *structure*, which is what lets it grow past one trick.
- **Honest trade-off:** the runtime win depends on selectivity and whether row
  groups can be skipped (data isn't sorted by shipdate). The robust, always-true
  result is the predicate moving into `PushedFilters` + fewer rows scanned.

### How this differs from AQE (the key objection)
AQE is reactive, per-query, narrow, and leaves no artifact. It can't prove
correctness, explain itself, persist a fix, or cover non-join patterns (pushdown,
pruning, caching, join reorder, whole-workload). We build the **validated,
observable system around optimization** — broadcast join just proves the loop.
(Full treatment in learning-notes.md → AQE section.)

### The rule registry (generalizing past one-off methods)
Patterns are now pluggable **Rules** (`agents/rules.py`), each a self-contained
`apply(sql, ctx) -> RuleResult | None`. The Optimizer just iterates the registry
and lets rules **chain** (a later rule sees the earlier rule's output). Adding a
pattern = adding a Rule; the optimize→validate→measure loop never changes.
- Categories: `join_strategy` (physical hint, safe) vs `predicate_pushdown`
  (logical rewrite, validation load-bearing).
- Current rules: `broadcast_join`, `sargable_year`, `substring_prefix`
  (`SUBSTRING(col,1,k)='US'` → `col LIKE 'US%'`, pushes as StringStartsWith).
- **Why this matters:** "predicate pushdown" isn't one rewrite — each non-sargable
  shape has its own equivalent (YEAR→range, SUBSTRING→LIKE, CAST-strip, …) and
  some aren't rewritable at all (MONTH, UPPER) where the rule correctly *doesn't
  fire*. A registry is the honest structure for a growing catalog — and it turns
  "which fix applies?" into a *retrieval* problem, which is exactly what pgvector
  will answer.

### Why hardcode the fix before adding RAG
With one pattern, "retrieve the right fix" is retrieving from a list of one —
pgvector RAG would add infrastructure and zero information. We prove the loop
first; retrieval swaps in cleanly when there are many patterns to choose from.

### Measured result (TPC-H sf=1, local single-node)
Demo query (lineitem ⋈ supplier ⋈ nation): SortMergeJoin → BroadcastHashJoin.
- Before: **1927 ms** → After: **686 ms** → **2.81× speedup**, output **identical**.
- Eliminated a ~180 MB shuffle of `lineitem` by broadcasting 25-row `nation` +
  10k-row `supplier`.

### The cost of validation (and why it's not a problem)
Validation runs the query twice (original + optimized) to compare outputs — a ~2×
cost. But it's a **one-time entry fee, amortized over the query's lifetime**: pay
2× once, save ~64% on *every* subsequent run forever. Net positive after a handful
of executions.

Key nuance — **not all optimizations need full validation:**
- **Hint-based changes (broadcast, merge, repartition, cache)** change only the
  *physical* plan, not the logical plan → output is identical *by construction*.
  Validation is belt-and-suspenders; a cheap sample/fingerprint check suffices.
- **Logical rewrites (pushdown we inject, subquery elimination, join reorder)** can
  change results → full validation required.

Corporate-realistic validation (cheap → thorough): sample/`TABLESAMPLE` the data;
run on a dev/staging replica not prod; run off-peak as a batch; replace full
DataFrame compare with a **result fingerprint** (row count + checksum of sorted
output); and **tier rigor by risk** (skip/sample safe hints, full-validate logical
rewrites). This is "difficulty-based cost routing" applied to the validator.

---

## Phases ahead (rationale to fill in as we build)

- **Neo4j (deferred on purpose):** the analyzer already emits a plan graph in
  memory; Neo4j's payoff is the *visualizer*, so we add it when that's the feature,
  not before. Avoids infra that doesn't change the core result.
- **pgvector RAG:** earns its place only once multiple patterns exist to retrieve
  among. Then it's a swap-in, not a rewrite.
- **Cost routing:** run the mechanical step (translation) on a cheap/local model and
  the reasoning step (optimization) on a smarter one — demonstrates "right-sized
  model per stage" and measures cost per stage. (Translation being trivial *proves*
  this thesis rather than undercutting it.)
- **Eval layer:** turns the always-on telemetry into published numbers —
  correctness %, speedup, cost/stage, convergence. The deliverable as much as code.
- **Next patterns to prioritize:** predicate pushdown, partition pruning, caching —
  chosen specifically because AQE does *not* do them, making the value undeniable.
