"""
Validator agent - the ground truth of the whole project.

Runs the ORIGINAL SQL in DuckDB (the trusted oracle) and the TRANSLATED SQL in
Spark, then proves the two result sets are identical. If this passes, every
later optimization can be trusted; if optimization ever changes the answer,
this is what catches it (Phase 2's safety loop and Phase 4's guardrail metric).

Comparison nuances handled here:
  - Column case: both sides normalised to lowercase, compared by position.
  - Row order: not guaranteed by SQL without ORDER BY -> we sort all rows.
  - Float drift: engines round decimals differently -> round + atol tolerance.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from sqlspark_optimizer.observability.tracing import traced

# TPC-H tables, matching the Parquet files written by data/tpch_setup.py.
TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


@dataclass
class ValidationResult:
    passed: bool
    reason: str
    duckdb_rows: int
    spark_rows: int


def normalize_df(df: pd.DataFrame, round_dp: int = 2) -> pd.DataFrame:
    """Make a result frame comparable across engines/plans: lowercase columns,
    round numerics, stringify dates/text (dodges dtype quirks), sort all rows."""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    num_cols = df.select_dtypes(include=[np.number]).columns
    for c in df.columns:
        if c in num_cols:
            df[c] = df[c].astype(float).round(round_dp)
        else:
            df[c] = df[c].astype(str)
    return df.sort_values(by=list(df.columns), kind="mergesort").reset_index(drop=True)


def frames_match(a: pd.DataFrame, b: pd.DataFrame, round_dp: int = 2) -> tuple[bool, str]:
    """Compare two result frames by *meaning*, not representation. Returns
    (passed, reason). Reused by both DuckDB-vs-Spark and original-vs-optimized."""
    if a.shape != b.shape:
        return False, f"shape mismatch: {a.shape} vs {b.shape}"
    a, b = normalize_df(a, round_dp), normalize_df(b, round_dp)
    num_cols = a.select_dtypes(include=[np.number]).columns
    obj_cols = [c for c in a.columns if c not in num_cols]
    numeric_ok = np.allclose(
        a[num_cols].to_numpy(dtype=float), b[num_cols].to_numpy(dtype=float),
        atol=10 ** (-round_dp), equal_nan=True,
    ) if len(num_cols) else True
    object_ok = a[obj_cols].equals(b[obj_cols]) if obj_cols else True
    if numeric_ok and object_ok:
        return True, "outputs identical"
    return False, f"{'numeric' if not numeric_ok else 'non-numeric'} values differ"


class Validator:
    def __init__(self, spark: SparkSession, parquet_dir: Path, round_dp: int = 2):
        self.spark = spark
        self.parquet_dir = Path(parquet_dir)
        self.round_dp = round_dp
        self._duck = duckdb.connect()
        self._register_views()

    def _register_views(self) -> None:
        """Expose each Parquet file as a view named after its TPC-H table, in
        BOTH engines, so the queries' bare table names resolve."""
        for tbl in TPCH_TABLES:
            path = (self.parquet_dir / f"{tbl}.parquet").as_posix()
            self._duck.execute(
                f"CREATE OR REPLACE VIEW {tbl} AS "
                f"SELECT * FROM read_parquet('{path}')"
            )
            self.spark.read.parquet(path).createOrReplaceTempView(tbl)

    @traced("validator")
    def validate(self, source_sql: str, spark_sql: str) -> ValidationResult:
        duck_df = self._duck.execute(source_sql).fetchdf()
        spark_df = self.spark.sql(spark_sql).toPandas()
        passed, reason = frames_match(duck_df, spark_df, self.round_dp)
        return ValidationResult(passed, reason, len(duck_df), len(spark_df))
