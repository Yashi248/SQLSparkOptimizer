"""
Web UI backend (roadmap item B) — a FastAPI server that runs the optimizer and
serves a single-page frontend. Paste SQL -> see the detected fix, before/after
physical plans, validation result, and speedup.

It holds ONE Spark session (started once) with the TPC-H tables registered, so
each request runs the real pipeline in a few seconds. For snappy responses use
sf=0.1 data (python data/tpch_setup.py --sf 0.1).

Run:  python webserver.py      then open http://localhost:8000
"""
from __future__ import annotations

from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel

from sqlspark_optimizer.agents.plan_analyzer import PlanGraph, parse_plan_tree
from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.api import optimize
from sqlspark_optimizer.bench import executed_plan
from sqlspark_optimizer.runtime import make_local_spark, register_parquet_dir

PARQUET_DIR = Path(__file__).resolve().parent / "data" / "tpch"
FRONTEND = Path(__file__).resolve().parent / "frontend" / "index.html"

app = FastAPI(title="SQLSpark Optimizer")
_state: dict = {}


@app.on_event("startup")
def _startup() -> None:
    spark = make_local_spark(app_name="sqlspark-web")   # broadcast exposed
    register_parquet_dir(spark, PARQUET_DIR)
    _state["spark"] = spark


class OptimizeRequest(BaseModel):
    sql: str
    dialect: str = "spark"


def _graph_json(g: PlanGraph) -> dict:
    return {"nodes": [{"id": n.id, "label": n.op} for n in g.nodes],
            "edges": [{"from": p, "to": c} for p, c in g.edges]}


def _result_preview(spark, sql: str, n: int = 10) -> dict:
    """First N rows of the query result — stringified so it's JSON-safe (dates,
    decimals). Makes the 'output identical' claim tangible in the UI."""
    pdf = spark.sql(sql).limit(n).toPandas().astype(str)
    return {"columns": [str(c) for c in pdf.columns], "rows": pdf.values.tolist()}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND)


@app.post("/api/optimize")
def api_optimize(req: OptimizeRequest) -> dict:
    spark = _state["spark"]
    try:
        spark_sql = Translator(source_dialect=req.dialect).translate(req.sql).spark_sql
        plan_before = _graph_json(parse_plan_tree(executed_plan(spark, spark_sql)))
        result = optimize(req.sql, spark, PARQUET_DIR,
                          source_dialect=req.dialect, timing_runs=1)
        plan_after = _graph_json(parse_plan_tree(
            executed_plan(spark, result.optimized_sql)))
        return {
            "ok": True,
            "optimized": result.optimized,
            "optimized_sql": result.optimized_sql,
            "applied_rules": result.applied_rules,
            "speedup": round(result.speedup, 2),
            "status": result.status,
            "explanation": result.explanation,
            "result_preview": _result_preview(spark, result.optimized_sql),
            "plan_before": plan_before,
            "plan_after": plan_after,
        }
    except Exception as exc:  # noqa: BLE001 - surface errors to the UI
        return {"ok": False, "error": str(exc)}


if __name__ == "__main__":
    # Single process (avoids the Windows multi-worker socket issue).
    uvicorn.run(app, host="127.0.0.1", port=8000)
