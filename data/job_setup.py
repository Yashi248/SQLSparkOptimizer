"""
Join Order Benchmark (JOB) loader — real IMDB data, the "real-skew" tier.

TPC-H is synthetic and uniform; JOB is a snapshot of the actual IMDB database
(21 tables) with the lopsided, correlated joins that break naive query plans.
This mirrors `tpch_setup.py`: load into DuckDB, then export every table to
Parquet under data/job/ so PySpark reads it exactly like the TPC-H tables.

You supply two things downloaded separately (see README "JOB validation"):
  - the JOB repo's schema.sql + the 113 *.sql queries  (gregrahn/join-order-benchmark)
  - the IMDB CSVs (imdb.tgz -> a folder of 21 headerless *.csv files)

Run:
  python data/job_setup.py --csv  C:/Users/you/Job_data/imdb_csv \\
                           --schema C:/Users/you/Job_data/join-order-benchmark/schema.sql

The CSVs are headerless and PostgreSQL-flavoured (backslash escapes); we lean on
DuckDB's read_csv to parse them against the column list from schema.sql.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

DATA_DIR = Path(__file__).resolve().parent
PARQUET_DIR = DATA_DIR / "job"

# The 21 IMDB tables, in the CSV filenames the imdb.tgz snapshot ships.
JOB_TABLES = [
    "aka_name", "aka_title", "cast_info", "char_name", "comp_cast_type",
    "company_name", "company_type", "complete_cast", "info_type", "keyword",
    "kind_type", "link_type", "movie_companies", "movie_info",
    "movie_info_idx", "movie_keyword", "movie_link", "name", "person_info",
    "role_type", "title",
]


def _column_names(schema_sql: str) -> dict[str, list[str]]:
    """Parse `CREATE TABLE name ( col type, ... )` blocks -> {table: [cols]}.

    The IMDB CSVs are headerless, so we need the column order from the schema to
    name them. We only want the leading identifier of each column definition and
    must skip table-level constraints (PRIMARY KEY / FOREIGN KEY / ...)."""
    out: dict[str, list[str]] = {}
    for m in re.finditer(r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)\s*;",
                          schema_sql, re.IGNORECASE | re.DOTALL):
        table = m.group(1).lower()
        cols: list[str] = []
        for raw in _split_top_level(m.group(2)):
            line = raw.strip()
            if not line:
                continue
            first = line.split()[0]
            if first.upper() in {"PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT",
                                 "CHECK", "KEY"}:
                continue
            cols.append(first.strip('"'))
        out[table] = cols
    return out


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not inside parentheses
    (e.g. character varying(12) must stay one piece)."""
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def build(csv_dir: Path, schema_path: Path) -> None:
    if not schema_path.exists():
        raise SystemExit(f"schema.sql not found: {schema_path}")
    if not csv_dir.exists():
        raise SystemExit(f"CSV dir not found: {csv_dir}")

    columns = _column_names(schema_path.read_text(encoding="utf-8"))
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    missing = []
    for tbl in JOB_TABLES:
        csv = csv_dir / f"{tbl}.csv"
        if not csv.exists():
            missing.append(tbl)
            continue
        cols = columns.get(tbl)
        if not cols:
            raise SystemExit(f"no columns parsed for {tbl} from {schema_path.name}")
        names = ", ".join(f"'{c}'" for c in cols)
        # header=false: the IMDB CSVs have no header row. all_varchar keeps the
        # load robust (types vary and some fields are dirty); Spark infers on read.
        # escape='\\': the snapshot uses PostgreSQL backslash escaping.
        con.execute(
            f"CREATE TABLE {tbl} AS SELECT * FROM read_csv("
            f"'{csv.as_posix()}', header=false, all_varchar=true, "
            f"escape='\\', names=[{names}])"
        )
        out = PARQUET_DIR / f"{tbl}.parquet"
        con.execute(f"COPY {tbl} TO '{out.as_posix()}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        print(f"  wrote {tbl:<16} {n:>12,} rows -> {out.name}")

    con.close()
    if missing:
        print(f"\nWARNING: {len(missing)} table CSV(s) not found and skipped: "
              f"{missing}")
    print(f"\nDone. Parquet written under {PARQUET_DIR}")
    print("Next: point the workload runner at it, e.g.\n"
          "  sqlspark workload --data data/job "
          "--queries <path-to>/join-order-benchmark --dialect postgres --all")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load JOB/IMDB CSVs -> Parquet.")
    ap.add_argument("--csv", required=True, type=Path,
                    help="Folder of the 21 headerless IMDB *.csv files.")
    ap.add_argument("--schema", required=True, type=Path,
                    help="Path to the JOB repo's schema.sql (for column names).")
    args = ap.parse_args()
    build(args.csv, args.schema)
