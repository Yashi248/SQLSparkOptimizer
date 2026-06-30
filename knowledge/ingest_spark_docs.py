"""
Ingest the optimization-pattern knowledge base into pgvector.

Prereq: Postgres+pgvector running (host port 5433 to avoid a native Postgres on 5432):
  docker run -d --name pgvector-spark -p 5433:5432 \
    -e POSTGRES_PASSWORD=spark123 -e POSTGRES_DB=sparkopt pgvector/pgvector:pg16

Run:  python -m knowledge.ingest_spark_docs
"""
from __future__ import annotations

from knowledge.kb import connect, init_schema, ingest


def main() -> None:
    conn = connect()
    try:
        init_schema(conn)
        n = ingest(conn)
        print(f"Ingested {n} optimization patterns into pgvector (sparkopt).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
