import duckdb


def test_version():
    assert duckdb.__version__ != "0.0.0"
