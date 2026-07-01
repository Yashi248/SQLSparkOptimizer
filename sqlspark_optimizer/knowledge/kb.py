"""
Knowledge base for optimization patterns, backed by Postgres + pgvector.

Each pattern is a short doc: the *symptom* (what the anti-pattern looks like) and
the *fix* (what to do), tied to the rule that implements it. We embed the symptom
text into a vector and store it. At optimize time, the analyzer's finding is
embedded and we retrieve the nearest pattern by cosine similarity, RAG over
Spark tuning knowledge. This is the "which fix applies to this problem?" step,
which only becomes meaningful once there are many patterns to choose among.

Embeddings: sentence-transformers `all-MiniLM-L6-v2` (384-dim, free + local).
"""
from __future__ import annotations

from dataclasses import dataclass

import psycopg2
from pgvector.psycopg2 import register_vector

# all-mpnet-base-v2 (768-dim) over all-MiniLM-L6-v2 (384-dim): the small model
# couldn't separate similar patterns (it matched "sort-merge join" to "partition
# pruning"); mpnet has the resolution to rank correctly. Slower, but for a handful
# of docs that's irrelevant. A real lesson that RAG quality = docs + model.
EMBED_MODEL = "all-mpnet-base-v2"
EMBED_DIM = 768

# Host port 5433 (not 5432) to avoid clashing with a native Postgres install.
DB = dict(host="localhost", port=5433, dbname="sparkopt",
          user="postgres", password="spark123")

# The knowledge. `rule` links a pattern to the registry rule in agents/rules.py;
# rules without an implementation yet (None) show retrieval still finds them.
PATTERNS = [
    {
        "rule": "broadcast_join",
        "title": "Broadcast small-table join",
        "symptom": "A large fact table is joined to a small dimension or lookup "
                   "table using a shuffle sort-merge join (SortMergeJoin), causing "
                   "an expensive shuffle of the large table across the network.",
        "fix": "Broadcast the small table with a /*+ BROADCAST(t) */ hint so the "
               "big table is never shuffled.",
        "keywords": "broadcast join, broadcast hash join, SortMergeJoin, "
                    "sort-merge join, join small table to large table, lookup "
                    "table, avoid shuffle in join, join strategy",
    },
    {
        "rule": "sargable_year",
        "title": "Sargable date predicate",
        "symptom": "A filter wraps a date column in a function such as "
                   "YEAR(col)=1994, which is non-sargable and blocks predicate "
                   "pushdown, so Spark scans every row before filtering.",
        "fix": "Rewrite to a sargable range: col >= DATE 'YYYY-01-01' AND col < "
               "DATE 'YYYY+1-01-01' so the filter pushes into the scan.",
        "keywords": "predicate pushdown, sargable predicate, YEAR function on date, "
                    "function wrapping column, non-sargable filter, date range",
    },
    {
        "rule": "substring_prefix",
        "title": "Substring prefix to LIKE",
        "symptom": "A filter uses SUBSTRING on a string column to test a leading "
                   "prefix, which blocks pushdown.",
        "fix": "Rewrite SUBSTRING(col,1,k)='X' to col LIKE 'X%' so it pushes down "
               "as a StringStartsWith filter.",
        "keywords": "substring prefix, first characters of a column, LIKE prefix, "
                    "starts with, string prefix filter, StringStartsWith",
    },
    {
        "rule": None,  # not implemented yet, proves retrieval still surfaces it
        "title": "Partition pruning",
        "symptom": "Queries filter on a column the table is physically partitioned "
                   "by, but the filter is written so Spark reads all partitions "
                   "instead of pruning to the matching ones.",
        "fix": "Express the filter directly on the partition column so Spark prunes "
               "partitions at planning time.",
        "keywords": "partition pruning, partitioned column, partition column, "
                    "scan all partitions, partition filter, directory partitioning",
    },
    {
        "rule": None,
        "title": "Cache reused subquery",
        "symptom": "The same DataFrame or subquery is referenced multiple times and "
                   "recomputed from scratch each time.",
        "fix": "Persist/cache the shared result so it is computed once and reused.",
        "keywords": "cache, persist, reuse dataframe, recomputed multiple times, "
                    "materialize intermediate result, repeated subquery",
    },
]


def pattern_text(p: dict) -> str:
    """The text we embed for a pattern: title + symptom + keywords. Richer text =
    more distinctive vectors, so retrieval separates similar patterns better."""
    return f"{p['title']}. {p['symptom']} Keywords: {p['keywords']}"


@dataclass
class Retrieved:
    rule: str | None
    title: str
    fix: str
    distance: float


_model = None


def get_model():
    """Lazy-load the embedding model (first call downloads ~80MB once)."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(EMBED_MODEL)
    return _model


def embed(text: str):
    return get_model().encode(text)


def connect():
    conn = psycopg2.connect(**DB)
    # The `vector` type must exist before register_vector() can bind it, so ensure
    # the extension is present on connect (idempotent).
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.commit()
    register_vector(conn)
    return conn


def init_schema(conn) -> None:
    # Drop + recreate so the embedding column always matches EMBED_DIM (the dim
    # changes if we swap models). Safe: ingest() fully repopulates the table.
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("DROP TABLE IF EXISTS optimization_patterns")
        cur.execute(
            f"""
            CREATE TABLE optimization_patterns (
                id        SERIAL PRIMARY KEY,
                rule      TEXT,
                title     TEXT NOT NULL,
                symptom   TEXT NOT NULL,
                fix       TEXT NOT NULL,
                embedding vector({EMBED_DIM})
            )
            """
        )
    conn.commit()


def ingest(conn) -> int:
    """(Re)load all patterns with fresh embeddings. Idempotent: clears first."""
    model = get_model()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE optimization_patterns RESTART IDENTITY")
        for p in PATTERNS:
            vec = model.encode(pattern_text(p))
            cur.execute(
                "INSERT INTO optimization_patterns (rule, title, symptom, fix, embedding) "
                "VALUES (%s, %s, %s, %s, %s)",
                (p["rule"], p["title"], p["symptom"], p["fix"], vec),
            )
    conn.commit()
    return len(PATTERNS)


def retrieve(conn, problem: str, k: int = 1) -> list[Retrieved]:
    """Nearest patterns to a described problem, by cosine distance (<=>)."""
    vec = embed(problem)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT rule, title, fix, embedding <=> %s AS distance "
            "FROM optimization_patterns ORDER BY distance ASC LIMIT %s",
            (vec, k),
        )
        return [Retrieved(*row) for row in cur.fetchall()]
