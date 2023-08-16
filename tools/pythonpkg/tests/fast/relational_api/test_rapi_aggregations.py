import duckdb
from decimal import Decimal
import pytest


@pytest.fixture(autouse=True)
def setup_and_teardown_of_table(duckdb_cursor):
    duckdb_cursor.execute("create table agg(id int, v int, t int);")
    duckdb_cursor.execute(
        """
		insert into agg values
		(1, 1, 2),
		(1, 1, 1),
		(1, 2, 3),
		(2, 10, 4),
		(2, 11, -1),
		(3, -1, 0),
		(3, 5, -2),
		(3, null, 10);
		"""
    )
    yield
    duckdb_cursor.execute("drop table agg")


@pytest.fixture()
def table(duckdb_cursor):
    return duckdb_cursor.table("agg")


class TestRAPIAggregations(object):
    def test_any_value(self, table):
        result = table.order("id, t").any_value("v").execute().fetchall()
        expected = [(1,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.order("id, t").any_value("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        )
        expected = [(1, 1), (2, 11), (3, 5)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_arg_max(self, table):
        result = table.arg_max("t", "v").execute().fetchall()
        expected = [(-1,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.arg_max("t", "v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 3), (2, -1), (3, -2)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    # def test_describe(self, table):
    #    assert table.describe().fetchall() is not None
