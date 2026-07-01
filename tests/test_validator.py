import pandas as pd

from sqlspark_optimizer.agents.validator import frames_match


def test_identical_frames_match():
    a = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    b = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    assert frames_match(a, b)[0]


def test_row_order_ignored():
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [3, 1, 2]})
    assert frames_match(a, b)[0]  # sorted before compare


def test_float_tolerance():
    a = pd.DataFrame({"v": [1.234561]})
    b = pd.DataFrame({"v": [1.23]})
    assert frames_match(a, b)[0]  # rounds to 2dp within atol


def test_value_mismatch_fails():
    a = pd.DataFrame({"x": [1, 2]})
    b = pd.DataFrame({"x": [1, 3]})
    assert not frames_match(a, b)[0]


def test_shape_mismatch_fails():
    a = pd.DataFrame({"x": [1]})
    b = pd.DataFrame({"x": [1, 2]})
    ok, reason = frames_match(a, b)
    assert not ok and "shape" in reason
