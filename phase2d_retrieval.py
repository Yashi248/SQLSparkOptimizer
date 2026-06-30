"""
Phase 2d - RAG retrieval over the optimization-pattern knowledge base.

Demonstrates the "which fix applies to this problem?" step: we describe a detected
symptom in plain language and pgvector returns the nearest tuning pattern by
cosine similarity including patterns we haven't implemented yet, proving the
retrieval surfaces the right knowledge independent of the rule registry.

Prereqs: pgvector container up + `python -m knowledge.ingest_spark_docs` run once.
Run:  python phase2d_retrieval.py
"""
from __future__ import annotations

from knowledge.kb import connect, retrieve

# Plain-language symptoms (as the analyzer might describe its findings).
PROBLEMS = [
    "Spark is doing a sort-merge join between a giant table and a tiny lookup table",
    "the where clause calls YEAR() on a date column so the filter is not pushed down",
    "filtering by the first two characters of a code column using substring",
    "the same intermediate result is recomputed several times in the query",
]


def main() -> None:
    conn = connect()
    try:
        for problem in PROBLEMS:
            top = retrieve(conn, problem, k=1)[0]
            rule = top.rule or "(no rule yet)"
            print(f"\nPROBLEM: {problem}")
            print(f"  -> matched pattern: {top.title}  [rule: {rule}]  "
                  f"(distance {top.distance:.3f})")
            print(f"     fix: {top.fix}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
