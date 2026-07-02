"""
Query loaders — turn a real query source into a {id: sql} dict the workload
runner consumes. This is what lets the tool run on *your* queries, not just the
built-in benchmark.

Supported sources (auto-detected):
  - a directory of .sql files      -> {filename_stem: contents}
  - a single .sql file             -> split into statements {q1, q2, ...}
  - a .json file  {id: sql}        -> used as-is
  - a .csv query-log export        -> pull the SQL column {q1, q2, ...}
                                      (Snowflake QUERY_HISTORY, Databricks, etc.)
"""
from __future__ import annotations

import json
from pathlib import Path

import sqlglot

# Column names commonly holding SQL text in query-log exports.
_SQL_COLUMNS = ("query_text", "query", "sql", "statement", "text", "query_string")


def load_queries(source: str | Path, sql_column: str | None = None,
                 dialect: str = "spark") -> dict[str, str]:
    """Load queries from a directory, .sql, .json, or .csv source."""
    p = Path(source)
    if p.is_dir():
        return {f.stem: f.read_text(encoding="utf-8")
                for f in sorted(p.glob("*.sql"))}
    ext = p.suffix.lower()
    if ext == ".json":
        return json.loads(p.read_text(encoding="utf-8"))
    if ext == ".sql":
        return split_statements(p.read_text(encoding="utf-8"), dialect)
    if ext == ".csv":
        return _load_csv(p, sql_column)
    raise ValueError(f"Unsupported query source: {source} "
                     "(expected a dir, .sql, .json, or .csv)")


def split_statements(text: str, dialect: str = "spark") -> dict[str, str]:
    """Split a multi-statement SQL string into {q1: ..., q2: ...}. Uses SQLGlot
    (handles ';' inside strings) and falls back to a naive split."""
    try:
        stmts = [s.sql(dialect=dialect) for s in sqlglot.parse(text, read=dialect) if s]
    except Exception:  # noqa: BLE001 - malformed SQL -> naive split
        stmts = [s.strip() for s in text.split(";") if s.strip()]
    return {f"q{i + 1}": s for i, s in enumerate(stmts)}


def _load_csv(path: Path, sql_column: str | None) -> dict[str, str]:
    import pandas as pd
    df = pd.read_csv(path)
    col = sql_column or _guess_sql_column(df.columns)
    if col is None or col not in df.columns:
        raise ValueError(f"No SQL column found in {path.name} "
                         f"(columns: {list(df.columns)}); pass sql_column=")
    return {f"q{i + 1}": str(v) for i, v in enumerate(df[col].dropna())}


def _guess_sql_column(columns) -> str | None:
    lowered = {c.lower(): c for c in columns}
    for name in _SQL_COLUMNS:
        if name in lowered:
            return lowered[name]
    return None
