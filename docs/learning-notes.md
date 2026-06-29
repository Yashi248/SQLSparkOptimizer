# Learning Notes — Concepts & Nuances

A running reference for the tech concepts behind this project. Grows as we build.
Two halves: **Spark / data engineering** and **Python / engineering patterns**.

---

# Part A — Spark & Data Engineering

## Join strategies (the heart of Phase 2)

When Spark joins two tables it picks a *physical* strategy. Which one it picks is
the single biggest performance lever in Spark SQL.

### 1. Broadcast Hash Join (BHJ) — the fast one
- **What:** the *small* table is copied ("broadcast") in full to every executor.
  The big table stays where it is. Each executor joins its local big-table
  partitions against the in-memory copy of the small table.
- **Why it's fast:** the big table is **never shuffled** across the network.
  Shuffling is the expensive part of a join; broadcast skips it entirely.
- **When Spark uses it:** when one side's estimated size is below
  `spark.sql.autoBroadcastJoinThreshold` (default **10 MB**).
- **The classic use case — star schema:** a huge *fact* table (sales, lineitem,
  events) joined to small *dimension* tables (country, product, nation). The
  dimensions are tiny → broadcast them. This is our Phase 2 demo: `lineitem`
  (18 MB) joined to `supplier` (82 KB) and `nation` (3 KB).

### 2. Sort-Merge Join (SMJ) — the default for big↔big
- **What:** **both** sides are shuffled so matching keys land on the same
  partition, each side is sorted by the key, then merged like a zipper.
- **Cost:** two big shuffles + two sorts. Network I/O, disk spill, serialization.
- **When it's right:** both tables are large (neither fits in memory to broadcast).
- **When it's the anti-pattern (our case):** Spark picks SMJ even though one side
  is tiny — because it lacked size stats, or the broadcast threshold was set too
  low / to -1, or AQE was off. The fix is to force a broadcast.

### 3. Shuffle Hash Join (SHJ) — the rare middle ground
- Shuffle both sides, then build an in-memory hash table on the smaller side per
  partition (no sort). Used less often; Spark prefers SMJ for robustness.

### 4. Broadcast Nested Loop / Cartesian — the slow fallbacks
- For non-equi joins (`a.x < b.y`) or cross joins. O(n·m). Avoid on big data.

### How Spark decides (and why it gets it wrong)
Spark's Catalyst optimizer estimates each side's `sizeInBytes`. If an estimate is
below the broadcast threshold → broadcast. Estimates come from table statistics or
file sizes. **Missing/stale stats** or a **conservative threshold** → Spark
over-estimates and falls back to a shuffle (SMJ). That mis-estimate is the
real-world bug our Plan-Analyzer catches.

### Risks of broadcasting
Broadcasting a table that's *not actually small* is dangerous: the driver collects
it and ships it to every executor → **driver OOM** or network storm. That's why a
threshold exists, and why our analyzer only flags tables genuinely under the limit.

---

## Shuffle — the thing optimization is mostly about
A **shuffle** redistributes data across the cluster so rows with the same key end
up together (needed for joins, `groupBy`, `distinct`, repartition). It writes
intermediate data to disk and sends it over the network — the most expensive
operation in Spark. **Most Spark tuning = removing or shrinking shuffles.**
Broadcast join removes a shuffle. That's why it's the first pattern we built.

- `spark.sql.shuffle.partitions` (default **200**) = how many partitions a shuffle
  produces. We set it to **8** because our data is tiny — 200 partitions over a few
  MB means lots of near-empty tasks and overhead. Right-sizing this matters.

## Adaptive Query Execution (AQE)
Spark 3+ feature, **on by default**. It re-plans the query *at runtime* using the
*actual* shuffle sizes it observes, not just compile-time estimates. AQE can:
- convert a SortMergeJoin into a BroadcastHashJoin once it sees a side is small,
- coalesce many small shuffle partitions into fewer,
- split skewed partitions.

**We disabled AQE in the Phase 2 demo** (`spark.sql.adaptive.enabled=false`) for two
reasons: (1) it gives a static, readable physical plan to parse, and (2) it would
"fix" our anti-pattern automatically, hiding the optimization we're demonstrating.
In production AQE is a good default — our project demonstrates the *manual*
reasoning AQE automates, plus patterns AQE doesn't cover.

### "If AQE optimizes, what are we doing?" (the key objection)
Honest answer: for a **single broadcast join on one query**, AQE often fixes it
itself — that's why we disabled it to demonstrate the pattern. Broadcast join is
the *teaching vehicle* that proves the architecture, not the project's value.

AQE is **reactive, per-query, and narrow**. It only: switches join strategy,
coalesces partitions, splits skew — all at shuffle boundaries, *after* a stage has
run (so it still pays part of the shuffle before fixing a join). It does **not**:
rewrite SQL, add predicate/projection pushdown, do partition pruning, choose
caching, reorder joins, prioritize across a workload, **prove a rewrite is
output-preserving, explain *why*, or leave a reviewable artifact.**

What *we* build is the **system around optimization** AQE isn't:
detect → fix → **validate (prove output unchanged)** → **measure** → **route cost**
→ log. It produces an explained, validated, version-controllable optimization
instead of a silent runtime tweak. Implication: future patterns should target what
AQE *can't* do (pushdown, partition pruning, caching, join reorder, whole-workload)
to make the value undeniable.

## The physical plan & Catalyst
- **Catalyst** = Spark's query optimizer. Pipeline: SQL → unresolved logical plan →
  analyzed → optimized logical plan → **physical plan** (the actual operators that
  run: scans, joins, exchanges).
- **Exchange** in a plan = a shuffle boundary. Spotting `Exchange` nodes tells you
  where the network cost is.
- We read the plan via `df._jdf.queryExecution().executedPlan().toString()` — the
  `_jdf` reaches through Py4J into the JVM DataFrame to get the executed plan text.

## Parquet — why all our data is `.parquet`
- **Columnar** storage: values of one column stored together. Reading 2 of 16
  columns reads only those 2 → less I/O.
- **Predicate pushdown:** filters (`WHERE l_shipdate > ...`) are applied *while
  reading* the file, skipping row groups that can't match.
- **Compression + schema embedded.** Standard format for analytics / Spark.

## Lazy evaluation: transformations vs actions
- **Transformations** (`select`, `join`, `filter`, `groupBy`) are *lazy* — they
  build a plan, nothing runs.
- **Actions** (`count`, `collect`, `toPandas`, `write`) trigger execution.
- This is why our timing uses an action. We benchmark with the **`noop` write
  sink** (`.write.format("noop")`) — it forces full execution but discards output,
  so we time the *computation* without `collect`/serialization overhead skewing it.

## TPC-H — our dataset
Industry-standard analytics benchmark: 8 tables (a `lineitem` fact + dimensions)
and 22 reference queries modeling a wholesale supplier. DuckDB generates both the
data and the queries. **Scale factor (sf)** sets size: `sf=1` ≈ 1 GB, `sf=0.1` ≈
100 MB (what we use for speed). Bigger sf → broadcast speedups become more visible.

---

# Part B — Python & Engineering Patterns

## Decorators & `@traced`
A **decorator** wraps a function to add behavior without editing the function.
`@traced("translator")` over `translate()` makes Python do
`translate = traced("translator")(translate)`, so calling `translate()` actually
runs a `wrapper` that times the call, opens an MLflow span, runs the real function
in the middle, records result/latency/status, and returns the original result.
- **Parameterized decorator** = 3 nested functions: `traced(name)` → `decorator(fn)`
  → `wrapper(*args, **kwargs)`. The name is captured by closure.
- `functools.wraps(fn)` copies the real function's name/docstring onto the wrapper
  so tracebacks and debuggers don't all say "wrapper".
- `try/except/finally` records status then **re-raises** — tracing must never
  change behavior. `finally` logs latency even on failure.
- **Why it matters here:** add `@traced` to any new agent → instant observability,
  zero changes to the agent's own code (a cross-cutting concern).

## MLflow: spans vs runs (two scopes)
- **Span** (`mlflow.start_span`, inside `@traced`) = one *agent call*. Lives in the
  Traces / GenAI tab. "What did this one function do, how fast, did it error?"
- **Run** (`mlflow.start_run`, our `pipeline_run`) = one *whole query pipeline*.
  Lives in the runs / Model-training tab. "What was the outcome for this query?"
- Spans fired inside a run **nest under** it: run (query) → spans (agents). That
  hierarchy is the raw material for the Phase 4 eval dashboard.

## SQL dialect transpilation (SQLGlot)
Different SQL engines have slightly different syntax (date math, function names,
type casing). **SQLGlot** parses SQL into an AST and re-emits it in a target
dialect. For *portable* queries (e.g. TPC-H Q6) the output is nearly identical —
it only rewrites what actually differs (e.g. DuckDB `date '..' - interval '90' day`
→ Spark `date_sub(...)`). So the Translator is a **dialect safety net**, not the
project's value — translation is the commodity step; validation + optimization are
the moat.

## Validating by *meaning*, not bytes
Comparing two result sets naively (`df1 == df2`) gives wrong answers because:
- **Row order** isn't guaranteed without `ORDER BY` → sort all rows first
  (stable `mergesort` so ties don't reorder).
- **Float drift:** two engines summing decimals land on `…600001` vs `…6` → round
  to 2 dp and compare with a tolerance (`np.allclose`, `atol`).
- **Dtype quirks:** DuckDB date = Python `date`, Spark date = `datetime64` → same
  value, different type → stringify non-numeric columns before comparing.
The Validator compares *meaning*. This is what makes "the optimization didn't
change the answer" a trustworthy claim — the project's whole foundation.

---

## Glossary (quick lookups)
- **Executor** — a JVM worker process that runs Spark tasks on a slice of data.
- **Driver** — the process that builds the plan and coordinates executors.
- **Partition** — a chunk of a distributed dataset; the unit of parallelism.
- **Wide vs narrow transformation** — narrow (`map`, `filter`) needs no shuffle;
  wide (`groupBy`, `join`) needs a shuffle.
- **Skew** — one partition far larger than others → one slow task drags the job.
- **Spill** — when an operation runs out of memory and writes to disk (slow).
