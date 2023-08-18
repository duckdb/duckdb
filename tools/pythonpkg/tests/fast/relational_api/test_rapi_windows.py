import duckdb
import pytest


@pytest.fixture(autouse=True)
def setup_and_teardown_of_table(duckdb_cursor):
    duckdb_cursor.execute("create table win(id int, v int, t int, f float);")
    duckdb_cursor.execute(
        """
        insert into win values
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
    duckdb_cursor.execute("drop table win")


@pytest.fixture()
def table(duckdb_cursor):
    return duckdb_cursor.table("win")


class TestRAPIWindows:
    # general purpose win functions
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
            (3, None, 10, 3),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_rank(self, table):
        result = table.rank("over ()").execute().fetchall()
        expected = [1] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = table.rank("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [(1, 1, 1), (1, 1, 1), (1, 2, 3), (2, 10, 1), (2, 11, 2), (3, -1, 1), (3, 5, 2), (3, None, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    @pytest.mark.parametrize("f", ["dense_rank", "rank_dense"])
    def test_dense_rank(self, table, f):
        result = getattr(table, f)("over ()").execute().fetchall()
        expected = [1] * 8
        assert len(result) == len(expected)
        assert all([r[0] == e for r, e in zip(result, expected)])
        result = getattr(table, f)("over (partition by id order by v asc)", "id, v").order("id").execute().fetchall()
        expected = [(1, 1, 1), (1, 1, 1), (1, 2, 2), (2, 10, 1), (2, 11, 2), (3, -1, 1), (3, 5, 2), (3, None, 3)]
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
            (3, None, 1.0),
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
            (3, None, 1.0),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_ntile(self, table):
        result = table.n_tile("over (order by v)", 3, "v").execute().fetchall()
        expected = [(-1, 1), (1, 1), (1, 1), (2, 2), (5, 2), (10, 2), (11, 3), (None, 3)]
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
            (3, None, 10, -1),
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
            (3, None, 10, -1),
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
            (3, None, 10, 5),
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
            (3, -1, 0, None),
            (3, None, 10, None),
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
            (3, -1, 0, None),
            (3, None, 10, -1),
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
            (3, 5, -2, None),
            (3, -1, 0, None),
            (3, None, 10, None),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_first_value(self, table):
        result = (
            table.first_value("v", "over (partition by id order by t asc)", projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, 1),
            (1, 1, 2, 1),
            (1, 2, 3, 1),
            (2, 11, -1, 11),
            (2, 10, 4, 11),
            (3, 5, -2, 5),
            (3, -1, 0, 5),
            (3, None, 10, 5),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_last_value(self, table):
        result = (
            table.last_value(
                "v",
                "over (partition by id order by t asc range between unbounded preceding and unbounded following) ",
                projected_columns="id, v, t",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, 2),
            (1, 1, 2, 2),
            (1, 2, 3, 2),
            (2, 11, -1, 10),
            (2, 10, 4, 10),
            (3, 5, -2, None),
            (3, -1, 0, None),
            (3, None, 10, None),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_nth_value(self, table):
        result = (
            table.nth_value("v", "over (partition by id order by t asc)", offset=2, projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, None),
            (1, 1, 2, 1),
            (1, 2, 3, 1),
            (2, 11, -1, None),
            (2, 10, 4, 10),
            (3, 5, -2, None),
            (3, -1, 0, -1),
            (3, None, 10, -1),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
        result = (
            table.nth_value("v", "over (partition by id order by t asc)", offset=4, projected_columns="id, v, t")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, 1, 1, None),
            (1, 1, 2, None),
            (1, 2, 3, None),
            (2, 11, -1, None),
            (2, 10, 4, None),
            (3, 5, -2, None),
            (3, -1, 0, None),
            (3, None, 10, None),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    # agg functions within win
    def test_any_value(self, table):
        result = (
            table.any_value("v", window_spec="over (partition by id order by t asc)", projected_columns="id")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 1), (1, 1), (1, 1), (2, 11), (2, 11), (3, 5), (3, 5), (3, 5)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_arg_max(self, table):
        result = (
            table.arg_max("t", "v", window_spec="over (partition by id)", projected_columns="id")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 3), (1, 3), (1, 3), (2, -1), (2, -1), (3, -2), (3, -2), (3, -2)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_arg_min(self, table):
        result = (
            table.arg_min("t", "v", window_spec="over (partition by id)", projected_columns="id")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 2), (1, 2), (1, 2), (2, 4), (2, 4), (3, 0), (3, 0), (3, 0)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_avg(self, table):
        result = [
            (r[0], round(r[1], 2))
            for r in (
                table.avg(
                    "v",
                    window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                    projected_columns="id",
                )
                .order("id")
                .execute()
                .fetchall()
            )
        ]
        expected = [(1, 1.0), (1, 1.0), (1, 1.33), (2, 11.0), (2, 10.5), (3, 5.0), (3, 2.0), (3, 2.0)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bit_and(self, table):
        result = (
            table.bit_and(
                "v",
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 1), (1, 1), (1, 0), (2, 11), (2, 10), (3, 5), (3, 5), (3, 5)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bit_or(self, table):
        result = (
            table.bit_or(
                "v",
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 1), (1, 1), (1, 3), (2, 11), (2, 11), (3, 5), (3, -1), (3, -1)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bit_xor(self, table):
        result = (
            table.bit_xor(
                "v",
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 1), (1, 0), (1, 2), (2, 11), (2, 1), (3, 5), (3, -6), (3, -6)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bitstring_agg(self, table):
        with pytest.raises(duckdb.BinderException, match="Could not retrieve required statistics"):
            result = (
                table.bitstring_agg(
                    "v",
                    window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                    projected_columns="id",
                )
                .order("id")
                .execute()
                .fetchall()
            )
        result = (
            table.bitstring_agg(
                "v",
                min=-1,
                max=11,
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [
            (1, '0010000000000'),
            (1, '0010000000000'),
            (1, '0011000000000'),
            (2, '0000000000001'),
            (2, '0000000000011'),
            (3, '0000001000000'),
            (3, '1000001000000'),
            (3, '1000001000000'),
        ]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bool_and(self, table):
        result = (
            table.bool_and("t::BOOL", window_spec="over (partition by id)", projected_columns="id")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, True), (1, True), (1, True), (2, True), (2, True), (3, False), (3, False), (3, False)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_bool_or(self, table):
        result = (
            table.bool_or("t::BOOL", window_spec="over (partition by id)", projected_columns="id")
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, True), (1, True), (1, True), (2, True), (2, True), (3, True), (3, True), (3, True)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_count(self, table):
        result = (
            table.count(
                "id",
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        )
        expected = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])

    def test_favg(self, table):
        result = [
            (r[0], round(r[1], 2))
            for r in table.favg(
                "f",
                window_spec="over (partition by id order by t asc rows between unbounded preceding and current row)",
                projected_columns="id",
            )
            .order("id")
            .execute()
            .fetchall()
        ]
        expected = [(1, 0.21), (1, 0.38), (1, 0.25), (2, 10.45), (2, 5.24), (3, 9.87), (3, 11.59), (3, 9.92)]
        assert len(result) == len(expected)
        assert all([r == e for r, e in zip(result, expected)])
