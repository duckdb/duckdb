import duckdb
import pytest
from duckdb import ColumnExpression


@pytest.fixture
def con():
    conn = duckdb.connect()
    # Main relation
    conn.execute(
        """
        create table tbl_a as (SELECT * FROM (VALUES
            (1, 1),
            (2, 1),
            (3, 2)
        ) AS t(a, b))
    """
    )

    # Other relation
    conn.execute(
        """
        create table tbl_b as (SELECT * FROM (VALUES
            (1, 4),
            (3, 5),
        ) AS t(a, b))
    """
    )
    yield conn


class TestRAPIJoins(object):
    def test_outer_join(self, con):
        a = con.table('tbl_a')
        b = con.table('tbl_b')
        expr = ColumnExpression('tbl_a.b') == ColumnExpression('tbl_b.a')
        rel = a.join(b, expr, 'outer')
        res = rel.fetchall()
        assert res == [(1, 1, 1, 4), (2, 1, 1, 4), (3, 2, None, None), (None, None, 3, 5)]
