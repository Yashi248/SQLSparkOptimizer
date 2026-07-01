"""
Phase 1 spine - translate + validate, end to end.

For each starter query (Q1, Q3, Q6):
  1. Translator transpiles the DuckDB SQL into Spark SQL.
  2. Validator runs both engines and proves the outputs match.
  3. Everything is logged to MLflow (one run per query, a span per agent).

Run (with `mlflow ui` already running in another terminal):
    python phase1_spine.py
    python phase1_spine.py --queries 1 3 6 14
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import mlflow
from pyspark.sql import SparkSession

from sqlspark_optimizer.agents.translator import Translator
from sqlspark_optimizer.agents.validator import Validator
from sqlspark_optimizer.observability.tracing import init_mlflow, pipeline_run

DATA_DIR = Path(__file__).resolve().parent / "data"
PARQUET_DIR = DATA_DIR / "tpch"
QUERIES_PATH = DATA_DIR / "tpch_queries.json"
DEFAULT_QUERIES = [1, 3, 6]


def make_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("phase1-spine")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main(query_ids: list[int]) -> None:
    if not QUERIES_PATH.exists():
        raise SystemExit("Run `python data/tpch_setup.py` first (no tpch_queries.json).")

    queries = json.loads(QUERIES_PATH.read_text(encoding="utf-8"))
    init_mlflow()
    spark = make_spark()
    translator = Translator(source_dialect="duckdb")
    validator = Validator(spark, PARQUET_DIR)

    results = []
    for qid in query_ids:
        sql = queries.get(str(qid))
        if not sql:
            print(f"Q{qid}: not found in tpch_queries.json, skipping.")
            continue

        with pipeline_run(query_id=str(qid), phase="1-spine"):
            translation = translator.translate(sql)
            outcome = validator.validate(translation.source_sql, translation.spark_sql)

            mlflow.log_metric("validation_passed", int(outcome.passed))
            mlflow.log_metric("row_count", outcome.duckdb_rows)
            mlflow.log_text(translation.spark_sql, f"q{qid}_spark.sql")

        mark = "PASS" if outcome.passed else "FAIL"
        print(f"Q{qid}: [{mark}] {outcome.reason} "
              f"(rows: duckdb={outcome.duckdb_rows}, spark={outcome.spark_rows})")
        results.append(outcome.passed)

    spark.stop()
    passed = sum(results)
    print(f"\nPhase 1 spine: {passed}/{len(results)} queries validated identical.")
    print("Open http://localhost:5000 to see the runs + agent spans.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, nargs="+", default=DEFAULT_QUERIES)
    main(ap.parse_args().queries)
