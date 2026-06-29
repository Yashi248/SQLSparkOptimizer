"""
Observability - MLflow instrumentation wired in from Phase 1.

The whole project's "differentiator" is that every stage is measured. We get
that almost for free by wrapping each agent call in an MLflow *span* and each
end-to-end query in an MLflow *run*. Do this from the first agent and Phase 4
(the eval dashboard) becomes "read what you already logged."

Two tools here:
  - init_mlflow()      : point the SDK at the local MLflow server once, per process.
  - @traced("name")    : decorator - times a function, records inputs/outputs/
                         latency/status as a span. Use it on every agent method.
  - pipeline_run(...)  : context manager for one query end-to-end; lets you
                         log run-level params/metrics (cost, convergence, etc.).
"""
from __future__ import annotations

import functools
import os
import time
from contextlib import contextmanager
from typing import Any, Callable

import mlflow

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "sql-spark-optimizer")

_initialised = False


def init_mlflow() -> None:
    """Call once at the top of any entry-point script."""
    global _initialised
    if _initialised:
        return
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)
    _initialised = True


def traced(name: str) -> Callable:
    """Decorator: record an agent call as an MLflow span.

    Logs the args as inputs, the return value as output, wall-clock latency in
    ms, and OK/ERROR status. Re-raises on failure so behaviour is unchanged.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            init_mlflow()
            start = time.perf_counter()
            with mlflow.start_span(name=name) as span:
                # Skip the bound `self`/`cls` arg when recording inputs.
                recordable = args[1:] if (args and hasattr(args[0], fn.__name__)) else args
                span.set_inputs({"args": _short(recordable), "kwargs": _short(kwargs)})
                try:
                    result = fn(*args, **kwargs)
                    span.set_outputs({"result": _short(result)})
                    span.set_attribute("status", "OK")
                    return result
                except Exception as exc:  # noqa: BLE001 - record then re-raise
                    span.set_attribute("status", "ERROR")
                    span.set_attribute("error", str(exc))
                    raise
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000
                    span.set_attribute("latency_ms", round(latency_ms, 2))
        return wrapper
    return decorator


@contextmanager
def pipeline_run(query_id: str, **params: Any):
    """One MLflow run per query pipeline. Yields the run so callers can
    log_metric / log_param for run-level numbers (cost, loops, correctness)."""
    init_mlflow()
    with mlflow.start_run(run_name=f"query-{query_id}") as run:
        mlflow.log_param("query_id", query_id)
        for k, v in params.items():
            mlflow.log_param(k, v)
        yield run


def _short(value: Any, limit: int = 500) -> str:
    """Stringify and truncate so we never dump a 600k-row DataFrame into a span."""
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + f"... [{len(text)} chars]"
