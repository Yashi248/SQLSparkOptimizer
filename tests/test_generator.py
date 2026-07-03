from sqlspark_optimizer.generator import generate_workload


def test_generates_requested_count():
    q = generate_workload(200)
    assert len(q) == 200
    assert all(isinstance(sql, str) and sql for sql in q.values())


def test_deterministic_for_seed():
    assert generate_workload(50, seed=1) == generate_workload(50, seed=1)


def test_covers_all_templates():
    q = generate_workload(60)
    kinds = {qid.rsplit("_", 1)[0] for qid in q}
    assert {"bcast_sup", "bcast_part", "year", "substr", "arith", "noop"} <= kinds


def test_queries_reference_tpch_tables():
    q = generate_workload(30)
    joined = " ".join(q.values()).lower()
    assert "lineitem" in joined and "join" in joined
