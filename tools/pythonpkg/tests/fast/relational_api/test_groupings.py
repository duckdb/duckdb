import duckdb
import pytest


@pytest.fixture
def con():
    conn = duckdb.connect()
    conn.execute(
        """
		create table tbl as (SELECT * FROM (VALUES
			(1, 'a', 12),
			(1, 'a', 10),
			(2, 'b', 5),
			(2, 'a', 7),
			(3, 'a', 5),
			(5, 'c', 2)
		) AS tbl(a, b, c))
	"""
    )
    yield conn


class TestGroupings(object):
    def test_basic_grouping(self, con):
        rel = con.table('tbl').sum("a", "b")
        res = rel.fetchall()
        assert res == [(7,), (2,), (5,)]

        rel = con.sql("select sum(a) from tbl GROUP BY b")
        res2 = rel.fetchall()
        assert res == res2

    def test_cubed(self, con):
        rel = con.table('tbl').sum("a", "CUBE (b)").order("ALL")
        res = rel.fetchall()
        assert res == [(2,), (5,), (7,), (14,)]

        rel = con.sql("select sum(a) from tbl GROUP BY CUBE (b) ORDER BY ALL")
        res2 = rel.fetchall()
        assert res == res2

    def test_rollup(self, con):
        rel = con.table('tbl').sum("a", "ROLLUP (b, c)").order("ALL")
        res = rel.fetchall()
        assert res == [(1,), (1,), (2,), (2,), (2,), (3,), (5,), (5,), (7,), (14,)]

        rel = con.sql("select sum(a) from tbl GROUP BY ROLLUP (b, c) ORDER BY ALL")
        res2 = rel.fetchall()
        assert res == res2
