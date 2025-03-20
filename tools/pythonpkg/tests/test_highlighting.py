import duckdb
import pytest

def test_highlighting_enabled():
    con = duckdb.connect_with_highlighting(highlight_enabled=True)
    try:
        con.sql("SELECT 42 AS x")  # Should print highlighted SQL
    except ImportError:
        pytest.skip("Pygments not installed")

def test_highlighting_disabled():
    con = duckdb.connect_with_highlighting(highlight_enabled=False)
    result = con.sql("SELECT 42 AS x")  # No highlighting
    assert result.fetchone() == (42,)

def test_default_connect():
    con = duckdb.connect()  # Original connect, no highlighting
    result = con.sql("SELECT 42 AS x")
    assert result.fetchone() == (42,)
    