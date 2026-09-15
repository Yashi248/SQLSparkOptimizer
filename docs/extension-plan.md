# Extension Plan — CI Eval Gate + JOB Real-Data Validation

Two high-ROI additions to the (feature-complete) core. Deliberately **not** rebuilding
what exists (MLflow tracing, eval scoring, validation guardrails, FastAPI service).

## Part 1 — CI eval gate + badge (~1 day)
Wire the *existing* eval (`observability/eval.py`) into GitHub Actions with pass/fail
thresholds + a status badge. It's the cheapest high-signal item.

- `run_evals.py` — CI entrypoint: runs `DEFAULT_EVAL_SET`, asserts thresholds
  (correctness == 1.0, routing_accuracy ≥ 0.8, avg_speedup ≥ 0.8 loose), writes
  `evals_report.json`, exits non-zero on regression.
- `.github/workflows/ci.yml` — `tests` job (pytest, fast gate) + `eval` job
  (Java + generate sf=0.1 data + run_evals, telemetry disabled).
- README status badge.

Deterministic in CI: no LLM (escalation off; eval set is rule-based), no pgvector
(routing falls back to the direct symptom→rule map, still correct).

## Part 2 — JOB real-data validation + one new rule (~2–4 days)
Run on the Join Order Benchmark (real IMDB, skewed correlated joins).

- **2a.** Add one deterministic rule (e.g. `cast_strip`) + kb doc + tests. (Catalog-grew
  signal; broadcast + escalation carry JOB, not this rule.)
- **2b.** `data/job_setup.py` — load IMDB into DuckDB → export 21 tables to
  `data/job/` Parquet (mirrors `tpch_setup.py`). The real plumbing.
- **2c.** Run via the existing loader + workload runner
  (`sqlspark workload --data data/job --queries <job_sql> --dialect postgres --all`);
  aggregate coverage %, broadcast wins, avg/best speedup, correctness, escalation/errors.
- **2d.** Document JOB results + three-tier data taxonomy (synthetic → stress → real-skew).

## Explicitly out of scope
OTel (MLflow covers it), Slack/webhook (FastAPI exists), async serving (Spark is the
bottleneck), GitLab dbt (Jinja time-sink), Streamlit viewer (MLflow UI + web UI exist).

## Future CI speed-up (optional)
Split pyproject deps into a light core + `[rag]`/`[graph]`/`[llm]`/`[api]` extras so CI
skips torch/sentence-transformers (unused in the eval). Deferred to avoid churn now.
