"""
Translator agent - SQL -> PySpark-executable SQL via SQLGlot.

Design note (read this): the roadmap frames the Translator as "SQL -> PySpark
DataFrame code." Auto-generating idiomatic DataFrame code (.groupBy().agg()...)
from arbitrary TPC-H SQL (subqueries, windows, correlated predicates) is
brittle and would stall the spine. So v1 transpiles the source SQL into the
*Spark SQL dialect* and we execute it with spark.sql(). Why this is fine:

  - It actually runs and is validatable (the whole point of Phase 1).
  - spark.sql() still builds a real Spark physical plan which is exactly what
    the Plan-Analyzer / Optimizer operate on in Phase 2. Nothing is lost.

SQLGlot does the dialect rewriting (date/interval syntax, function names, etc.)
that would otherwise break a DuckDB query when fed to Spark.
"""
from __future__ import annotations

from dataclasses import dataclass

import sqlglot

from sqlspark_optimizer.observability.tracing import traced


@dataclass
class TranslationResult:
    source_sql: str
    spark_sql: str
    source_dialect: str
    target_dialect: str = "spark"


class Translator:
    """Transpiles SQL from a source dialect into Spark SQL."""

    def __init__(self, source_dialect: str = "duckdb") -> None:
        self.source_dialect = source_dialect

    @traced("translator")
    def translate(self, sql: str) -> TranslationResult:
        # transpile() returns one string per statement; TPC-H queries are single
        # statements, but join with ';' to be safe against trailing semicolons.
        statements = sqlglot.transpile(
            sql, read=self.source_dialect, write="spark", pretty=True
        )
        spark_sql = ";\n".join(s for s in statements if s.strip())
        return TranslationResult(
            source_sql=sql,
            spark_sql=spark_sql,
            source_dialect=self.source_dialect,
        )
