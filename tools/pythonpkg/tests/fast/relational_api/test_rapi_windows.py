import duckdb
import pytest


@pytest.fixture(autouse=True)
def setup_and_teardown_of_table(duckdb_cursor):
    duckdb_cursor.execute("create table win(id int, v int, t int);")
    duckdb_cursor.execute(
        """
		insert into win values
		(1, 1, 2),
		(1, 1, 1),
		(1, 2, 3),
		(2, 10, 4),
		(2, 11, -1),
		(3, -1, 0),
		(3, 5, -2),
		(3, 45, 10);
		"""
    )
    yield
    duckdb_cursor.execute("drop table win")


@pytest.fixture()
def table(duckdb_cursor):
    return duckdb_cursor.table("win")


class TestRAPIWindows:
    def test_row_number(self, table):
        result = table.row_number("over ()").execute().fetchall()
        expected = list(range(1, 9))
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = table.row_number("over (partition by id order by t asc)", "id, v, t").order("id").execute().fetchall()
        expected = [
            (1, 1, 1, 1),
            (1, 1, 2, 2),
            (1, 2, 3, 3),
            (2, 11, -1, 1),
            (2, 10, 4, 2),
            (3, 5, -2, 1),
            (3, -1, 0, 2),
            (3, 45, 10, 3),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_rank(self, table):
        result = table.rank("over ()").execute().fetchall()
        expected = [1] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = table.rank("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [(1, 1, 1), (1, 1, 1), (1, 2, 3), (2, 10, 1), (2, 11, 2), (3, -1, 1), (3, 5, 2), (3, 45, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    @pytest.mark.parametrize("f", ["dense_rank", "rank_dense"])
    def test_dense_rank(self, table, f):
        result = getattr(table, f)("over ()").execute().fetchall()
        expected = [1] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = getattr(table, f)("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [(1, 1, 1), (1, 1, 1), (1, 2, 2), (2, 10, 1), (2, 11, 2), (3, -1, 1), (3, 5, 2), (3, 45, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
