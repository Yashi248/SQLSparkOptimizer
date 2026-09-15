"""
CI eval gate — runs the eval set and FAILS (exit 1) on a regression.

Wraps the existing eval (sqlspark_optimizer.observability.eval) with pass/fail
thresholds so it can gate CI. Deterministic by design: no LLM (escalation off),
and pgvector absent -> retrieval falls back to the direct symptom->rule map, so
routing accuracy is stable in CI.

Run:  python run_evals.py     (exit 0 = pass, 1 = regression)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Deterministic eval: disable the LLM escalation path so results don't depend on
# whether a local Ollama / API key happens to be present. Must be set before the
# router is constructed (i.e. before importing/using the graph).
os.environ.setdefault("SQLSPARK_DISABLE_LLM", "1")
os.environ.setdefault("SQLSPARK_DISABLE_TELEMETRY", "1")

from sqlspark_optimizer.observability.eval import DEFAULT_EVAL_SET, evaluate
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"
REPORT_PATH = Path(__file__).resolve().parent / "evals_report.json"

# Gate on the DETERMINISTIC metrics (reliable in CI). Speedup is timing-noisy at
# sf=0.1, so its floor is loose — a catastrophic-regression catch, not a tight SLA.
THRESHOLDS = {
    "correctness_rate": ("==", 1.0),   # every output must stay identical
    "routing_accuracy": (">=", 0.8),   # right fix chosen for the query
    "avg_speedup": (">=", 0.8),         # loose: catches a broadcast rule breaking
}


def _passes(op: str, value: float, threshold: float) -> bool:
    return value == threshold if op == "==" else value >= threshold


def main() -> None:
    spark = make_local_spark(app_name="ci-eval")      # broadcast exposed
    register_parquet_dir(spark, PARQUET_DIR)
    report = evaluate(spark, DEFAULT_EVAL_SET, PARQUET_DIR)
    spark.stop()

    metrics = report.metrics()
    REPORT_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("\n=== eval gate ===")
    failed = []
    for key, (op, threshold) in THRESHOLDS.items():
        value = metrics[key]
        ok = _passes(op, value, threshold)
        print(f"  {key:<18} {value:<7} {op} {threshold}   [{'PASS' if ok else 'FAIL'}]")
        if not ok:
            failed.append(key)
    for key, value in metrics.items():
        if key not in THRESHOLDS:
            print(f"  {key:<18} {value}   (reported)")

    if failed:
        print(f"\nEVAL GATE FAILED: {failed}")
        sys.exit(1)
    print("\nEVAL GATE PASSED")


if __name__ == "__main__":
    main()
