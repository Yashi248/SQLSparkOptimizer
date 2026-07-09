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

IMPORTANT — observability is BEST-EFFORT. If the MLflow server is down, the
pipeline must still run: monitoring failing should never break the thing it
monitors. So we do a fast reachability check (1s, no multi-minute retry hang); if
the server is unreachable we print one warning and run WITHOUT telemetry. Start
`mlflow ui` to get the spans/runs back.
"""
from __future__ import annotations

import functools
import os
import socket
import time
from contextlib import contextmanager, nullcontext
from typing import Any, Callable
from urllib.parse import urlparse

import mlflow

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "sql-spark-optimizer")

# None = not yet decided; True/False = whether telemetry is available this run.
_enabled: bool | None = None


def _server_reachable(uri: str, timeout: float = 1.0) -> bool:
    """Fast TCP check so a down server fails in ~1s instead of MLflow's minutes-
    long retry loop. Non-http stores (file:./mlruns) need no server -> always ok."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("http", "https"):
        return True
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def init_mlflow() -> bool:
    """Decide once whether telemetry is available. Returns True if enabled."""
    global _enabled
    if _enabled is not None:
        return _enabled
    # Hard off-switch — e.g. on Databricks serverless, where touching MLflow reads
    # a blocked Spark config and logs a noisy (but non-fatal) error.
    if os.environ.get("SQLSPARK_DISABLE_TELEMETRY"):
        _enabled = False
        return False
    if not _server_reachable(TRACKING_URI):
        print(f"[observability] MLflow not reachable at {TRACKING_URI} - "
              f"running WITHOUT telemetry. Start `mlflow ui` to capture it.")
        _enabled = False
        return False
    try:
        mlflow.set_tracking_uri(TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT)
        _enabled = True
    except Exception as exc:  # noqa: BLE001 - never let telemetry setup crash us
        print(f"[observability] MLflow setup failed ({exc}) - running without it.")
        _enabled = False
    return _enabled


def traced(name: str) -> Callable:
    """Decorator: record an agent call as an MLflow span. Best-effort — if
    telemetry is disabled or span logging fails, the wrapped function still runs
    and returns normally (tracing never changes behavior)."""
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not init_mlflow():
                return fn(*args, **kwargs)
            start = time.perf_counter()
            try:
                span_cm = mlflow.start_span(name=name)
            except Exception:  # noqa: BLE001 - degrade to no tracing
                return fn(*args, **kwargs)
            with span_cm as span:
                recordable = args[1:] if (args and hasattr(args[0], fn.__name__)) else args
                _safe(span.set_inputs, {"args": _short(recordable), "kwargs": _short(kwargs)})
                try:
                    result = fn(*args, **kwargs)
                    _safe(span.set_outputs, {"result": _short(result)})
                    _safe(span.set_attribute, "status", "OK")
                    return result
                except Exception as exc:  # noqa: BLE001 - record then re-raise
                    _safe(span.set_attribute, "status", "ERROR")
                    _safe(span.set_attribute, "error", str(exc))
                    raise
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000
                    _safe(span.set_attribute, "latency_ms", round(latency_ms, 2))
        return wrapper
    return decorator


@contextmanager
def pipeline_run(query_id: str, **params: Any):
    """One MLflow run per query pipeline. If telemetry is unavailable this is a
    no-op context, so callers' `mlflow.log_*` calls inside are harmless."""
    if not init_mlflow():
        yield None
        return
    with mlflow.start_run(run_name=f"query-{query_id}") as run:
        _safe(mlflow.log_param, "query_id", query_id)
        for k, v in params.items():
            _safe(mlflow.log_param, k, v)
        yield run


def _safe(fn: Callable, *args: Any) -> None:
    """Run a telemetry call, swallowing any error — logging must never crash."""
    try:
        fn(*args)
    except Exception:  # noqa: BLE001
        pass


def _short(value: Any, limit: int = 500) -> str:
    """Stringify and truncate so we never dump a 600k-row DataFrame into a span."""
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + f"... [{len(text)} chars]"
