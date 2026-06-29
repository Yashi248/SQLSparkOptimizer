"""
Phase 0

DuckDB's TPC-H extension generates both the dataset AND the 22
standard benchmark queries. We:
  1. Generate TPC-H at a given scale factor (sf=1 ~= 1GB; use sf=0.1 to start small).
  2. Export every table to Parquet under data/tpch/ so PySpark can read it.
  3. Pull the 22 reference queries and freeze our 3 starters (Q1, Q3, Q6).

Run:  python data/tpch_setup.py --sf 0.1
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).resolve().parent
PARQUET_DIR = DATA_DIR / "tpch"
QUERIES_PATH = DATA_DIR / "tpch_queries.json"

# Our frozen starter set: simple aggregations first (Q1, Q6), one join (Q3).
STARTER_QUERIES = [1, 3, 6]

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def build(scale_factor: float) -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # In-memory DB is fine; we persist the output as Parquet, not the DB file.
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")

    print(f"Generating TPC-H data at sf={scale_factor} ...")
    con.execute(f"CALL dbgen(sf={scale_factor})")

    for tbl in TPCH_TABLES:
        out = PARQUET_DIR / f"{tbl}.parquet"
        con.execute(f"COPY {tbl} TO '{out.as_posix()}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        print(f"  wrote {tbl:<10} {n:>10,} rows -> {out.name}")

    # Pull the 22 reference queries. tpch_queries() returns (query_nr, query).
    rows = con.execute(
        "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
    ).fetchall()
    queries = {int(nr): sql.strip() for nr, sql in rows}
    QUERIES_PATH.write_text(json.dumps(queries, indent=2), encoding="utf-8")
    print(f"\nSaved {len(queries)} reference queries -> {QUERIES_PATH.name}")
    print(f"Frozen starter set: {STARTER_QUERIES}")

    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--sf", type=float, default=0.1,
        help="TPC-H scale factor (0.1 = small/fast, 1 = ~1GB).",
    )
    build(ap.parse_args().sf)
