import duckdb
from decimal import Decimal
import pytest


@pytest.fixture(autouse=True)
def setup_and_teardown_of_table(duckdb_cursor):
    duckdb_cursor.execute("create table agg(id int, v int, t int, f float);")
    duckdb_cursor.execute(
        """
		insert into agg values
		(1, 1, 2, 0.54),
		(1, 1, 1, 0.21),
		(1, 2, 3, 0.001),
		(2, 10, 4, 0.04),
		(2, 11, -1, 10.45),
		(3, -1, 0, 13.32),
		(3, 5, -2, 9.87),
		(3, null, 10, 6.56);
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

    def test_arg_min(self, table):
        result = table.arg_min("t", "v").execute().fetchall()
        expected = [(0,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.arg_min("t", "v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 2), (2, 4), (3, 0)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_avg(self, table):
        result = table.avg("v").execute().fetchall()
        expected = [(4.14,)]
        assert len(result) == len(expected)
        assert round(result[0][0], 2) == expected[0][0]
        result = [
            (r[0], round(r[1], 2))
            for r in table.avg("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        ]
        expected = [(1, 1.33), (2, 10.5), (3, 2)]

    def test_bit_and(self, table):
        result = table.bit_and("v").execute().fetchall()
        expected = [(0,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bit_and("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 0), (2, 10), (3, 5)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bit_or(self, table):
        result = table.bit_or("v").execute().fetchall()
        expected = [(-1,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bit_or("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 3), (2, 11), (3, -1)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bit_xor(self, table):
        result = table.bit_xor("v").execute().fetchall()
        expected = [(-7,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bit_xor("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 2), (2, 1), (3, -6)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bitstring_agg(self, table):
        result = table.bitstring_agg("v").execute().fetchall()
        expected = [("1011001000011",)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bitstring_agg("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, "0011000000000"), (2, "0000000000011"), (3, "1000001000000")]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        with pytest.raises(duckdb.InvalidInputException):
            table.bitstring_agg("v", min="1")
        with pytest.raises(duckdb.InvalidTypeException):
            table.bitstring_agg("v", min="1", max=11)

    def test_bool_and(self, table):
        result = table.bool_and("v::BOOL").execute().fetchall()
        expected = [(True,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bool_and("t::BOOL", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, True), (2, True), (3, False)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bool_or(self, table):
        result = table.bool_or("v::BOOL").execute().fetchall()
        expected = [(True,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.bool_or("v::BOOL", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, True), (2, True), (3, True)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_count(self, table):
        result = table.count("*").execute().fetchall()
        expected = [(8,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.count("*", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [(1, 3), (2, 2), (3, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_favg(self, table):
        result = [round(r[0], 2) for r in table.favg("f").execute().fetchall()]
        expected = [5.12]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = [
            (r[0], round(r[1], 2))
            for r in table.favg("f", groups="id", projected_columns="id").order("id").execute().fetchall()
        ]
        expected = [(1, 0.25), (2, 5.24), (3, 9.92)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_first(self, table):
        result = table.first("v").execute().fetchall()
        expected = [(1,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.first("v", "id", "id").order("id").execute().fetchall()
        expected = [(1, 1), (2, 10), (3, -1)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_last(self, table):
        result = table.last("v").execute().fetchall()
        expected = [(None,)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.last("v", "id", "id").order("id").execute().fetchall()
        expected = [(1, 2), (2, 11), (3, None)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_fsum(self, table):
        result = [round(r[0], 2) for r in table.fsum("f").execute().fetchall()]
        expected = [40.99]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = [
            (r[0], round(r[1], 2))
            for r in table.fsum("f", groups="id", projected_columns="id").order("id").execute().fetchall()
        ]
        expected = [(1, 0.75), (2, 10.49), (3, 29.75)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_geomean(self, table):
        result = [round(r[0], 2) for r in table.geomean("f").execute().fetchall()]
        expected = [0.67]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = [
            (r[0], round(r[1], 2))
            for r in table.geomean("f", groups="id", projected_columns="id").order("id").execute().fetchall()
        ]
        expected = [(1, 0.05), (2, 0.65), (3, 9.52)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_histogram(self, table):
        result = table.histogram("v").execute().fetchall()
        expected = [({'key': [-1, 1, 2, 5, 10, 11], 'value': [1, 2, 1, 1, 1, 1]},)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = table.histogram("v", groups="id", projected_columns="id").order("id").execute().fetchall()
        expected = [
            (1, {'key': [1, 2], 'value': [2, 1]}),
            (2, {'key': [10, 11], 'value': [1, 1]}),
            (3, {'key': [-1, 5], 'value': [1, 1]}),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

        # def test_describe(self, table):

    #    assert table.describe().fetchall() is not None
