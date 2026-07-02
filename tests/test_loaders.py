import json

from sqlspark_optimizer.loaders import load_queries, split_statements


def test_load_from_directory(tmp_path):
    (tmp_path / "a.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "b.sql").write_text("SELECT 2", encoding="utf-8")
    q = load_queries(tmp_path)
    assert q == {"a": "SELECT 1", "b": "SELECT 2"}


def test_load_from_json(tmp_path):
    f = tmp_path / "q.json"
    f.write_text(json.dumps({"1": "SELECT a", "2": "SELECT b"}), encoding="utf-8")
    assert load_queries(f) == {"1": "SELECT a", "2": "SELECT b"}


def test_load_from_multi_statement_sql(tmp_path):
    f = tmp_path / "many.sql"
    f.write_text("SELECT 1; SELECT 2; SELECT 3", encoding="utf-8")
    q = load_queries(f)
    assert list(q) == ["q1", "q2", "q3"] and len(q) == 3


def test_split_statements_ignores_semicolons_in_strings():
    q = split_statements("SELECT 'a;b' AS x; SELECT 2")
    assert len(q) == 2


def test_load_from_csv(tmp_path):
    f = tmp_path / "log.csv"
    f.write_text("query_text,duration\nSELECT 1,10\nSELECT 2,20\n", encoding="utf-8")
    q = load_queries(f)
    assert list(q.values()) == ["SELECT 1", "SELECT 2"]


def test_unsupported_source_raises(tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("nope", encoding="utf-8")
    try:
        load_queries(f)
        assert False, "expected ValueError"
    except ValueError:
        pass
