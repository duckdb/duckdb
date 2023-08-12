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

    def test_percent_rank(self, table):
        result = table.percent_rank("over ()").execute().fetchall()
        expected = [0.0] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = table.percent_rank("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [
            (1, 1, 0.0),
            (1, 1, 0.0),
            (1, 2, 1.0),
            (2, 10, 0.0),
            (2, 11, 1.0),
            (3, -1, 0.0),
            (3, 5, 0.5),
            (3, 45, 1.0),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_cume_dist(self, table):
        result = table.cume_dist("over ()").execute().fetchall()
        expected = [1.0] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = table.cume_dist("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [
            (1, 1, 2 / 3),
            (1, 1, 2 / 3),
            (1, 2, 1.0),
            (2, 10, 0.5),
            (2, 11, 1.0),
            (3, -1, 1 / 3),
            (3, 5, 2 / 3),
            (3, 45, 1.0),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_ntile(self, table):
        result = table.n_tile("over (order by v)", 3, "v").execute().fetchall()
        expected = [(-1, 1), (1, 1), (1, 1), (2, 2), (5, 2), (10, 2), (11, 3), (45, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_lag(self, table):
        result = (
            table.lag("v", "over (partition by id order by t asc)", projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, None),
            (1, 1, 2, 1),
            (1, 2, 3, 1),
            (2, 11, -1, None),
            (2, 10, 4, 11),
            (3, 5, -2, None),
            (3, -1, 0, 5),
            (3, 45, 10, -1),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.lag("v", "over (partition by id order by t asc)", default_value="-1", projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, -1),
            (1, 1, 2, 1),
            (1, 2, 3, 1),
            (2, 11, -1, -1),
            (2, 10, 4, 11),
            (3, 5, -2, -1),
            (3, -1, 0, 5),
            (3, 45, 10, -1),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.lag("v", "over (partition by id order by t asc)", offset=2, projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, None),
            (1, 1, 2, None),
            (1, 2, 3, 1),
            (2, 11, -1, None),
            (2, 10, 4, None),
            (3, 5, -2, None),
            (3, -1, 0, None),
            (3, 45, 10, 5),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_lead(self, table):
        result = (
            table.lead("v", "over (partition by id order by t asc)", projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, 1),
            (1, 1, 2, 2),
            (1, 2, 3, None),
            (2, 11, -1, 10),
            (2, 10, 4, None),
            (3, 5, -2, -1),
            (3, -1, 0, 45),
            (3, 45, 10, None),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.lead("v", "over (partition by id order by t asc)", default_value="-1", projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, 1),
            (1, 1, 2, 2),
            (1, 2, 3, -1),
            (2, 11, -1, 10),
            (2, 10, 4, -1),
            (3, 5, -2, -1),
            (3, -1, 0, 45),
            (3, 45, 10, -1),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.lead("v", "over (partition by id order by t asc)", offset=2, projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, 2),
            (1, 1, 2, None),
            (1, 2, 3, None),
            (2, 11, -1, None),
            (2, 10, 4, None),
            (3, 5, -2, 45),
            (3, -1, 0, None),
            (3, 45, 10, None),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    @pytest.mark.parametrize("f", ["first_value", "first"])
    def test_first_value(self, table, f):
        result = (
            getattr(table, f)("v", "over (partition by id order by t asc)", "id, v, t").order("id").execute().fetchall()
        )
        expected = [
            (1, 1, 1, 1),
            (1, 1, 2, 1),
            (1, 2, 3, 1),
            (2, 11, -1, 11),
            (2, 10, 4, 11),
            (3, 5, -2, 5),
            (3, -1, 0, 5),
            (3, 45, 10, 5),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    @pytest.mark.parametrize("f", ["last_value", "last"])
    def test_last_value(self, table, f):
        result = (
            getattr(table, f)("v", "over (partition by id order by t asc)", "id, v, t").order("id").execute().fetchall()
        )
        expected = [
            (1, 1, 1, 2),
            (1, 1, 2, 2),
            (1, 2, 3, 2),
            (2, 11, -1, 10),
            (2, 10, 4, 10),
            (3, 5, -2, 45),
            (3, -1, 0, 45),
            (3, 45, 10, 45),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
