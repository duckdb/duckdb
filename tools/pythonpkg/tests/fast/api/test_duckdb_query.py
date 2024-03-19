import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas
from duckdb import Value


class TestDuckDBQuery(object):
    def test_duckdb_query(self, duckdb_cursor):
        # we can use duckdb_cursor.sql to run both DDL statements and select statements
        duckdb_cursor.sql('create view v1 as select 42 i')
        rel = duckdb_cursor.sql('select * from v1')
        assert rel.fetchall()[0][0] == 42

        # also multiple statements
        duckdb_cursor.sql('create view v2 as select i*2 j from v1; create view v3 as select j * 2 from v2;')
        rel = duckdb_cursor.sql('select * from v3')
        assert rel.fetchall()[0][0] == 168

        # we can run multiple select statements - we get only the last result
        res = duckdb_cursor.sql('select 42; select 84;').fetchall()
        assert res == [(84,)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_duckdb_from_query_multiple_statements(self, pandas):
        tst_df = pandas.DataFrame({'a': [1, 23, 3, 5]})

        res = duckdb.sql(
            '''
        select 42; select *
        from tst_df
        union all
        select *
        from tst_df;
        '''
        ).fetchall()
        assert res == [(1,), (23,), (3,), (5,), (1,), (23,), (3,), (5,)]

    def test_duckdb_query_empty_result(self):
        con = duckdb.connect()
        # show tables on empty connection does not produce any tuples
        res = con.query('show tables').fetchall()
        assert res == []

    def test_named_param(self):
        con = duckdb.connect()

        original_res = con.execute(
            """
            select
                count(*) FILTER (WHERE i >= $1),
                sum(i) FILTER (WHERE i < $2),
                avg(i) FILTER (WHERE i < $1)
            from
                range(100) tbl(i)
        """,
            [5, 10],
        ).fetchall()

        res = con.execute(
            """
            select
                count(*) FILTER (WHERE i >= $param),
                sum(i) FILTER (WHERE i < $other_param),
                avg(i) FILTER (WHERE i < $param)
            from
                range(100) tbl(i)
        """,
            {'param': 5, 'other_param': 10},
        ).fetchall()

        assert res == original_res

    def test_named_param_not_dict(self):
        con = duckdb.connect()

        with pytest.raises(
            duckdb.InvalidInputException,
            match="Values were not provided for the following prepared statement parameters: name1, name2, name3",
        ):
            con.execute("select $name1, $name2, $name3", ['name1', 'name2', 'name3'])

    def test_named_param_basic(self):
        con = duckdb.connect()

        res = con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3, 'name3': 'a'}).fetchall()
        assert res == [
            (5, 3, 'a'),
        ]

    def test_named_param_not_exhaustive(self):
        con = duckdb.connect()

        with pytest.raises(
            duckdb.InvalidInputException,
            match="Invalid Input Error: Values were not provided for the following prepared statement parameters: name3",
        ):
            con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3})

    def test_named_param_excessive(self):
        con = duckdb.connect()

        with pytest.raises(
            duckdb.InvalidInputException,
            match="Values were not provided for the following prepared statement parameters: name3",
        ):
            con.execute("select $name1, $name2, $name3", {'name1': 5, 'name2': 3, 'not_a_named_param': 5})

    def test_named_param_not_named(self):
        con = duckdb.connect()

        with pytest.raises(
            duckdb.InvalidInputException,
            match="Values were not provided for the following prepared statement parameters: 1, 2",
        ):
            con.execute("select $1, $1, $2", {'name1': 5, 'name2': 3})

    def test_named_param_mixed(self):
        con = duckdb.connect()

        with pytest.raises(
            duckdb.NotImplementedException, match="Mixing named and positional parameters is not supported yet"
        ):
            con.execute("select $name1, $1, $2", {'name1': 5, 'name2': 3})

    def test_named_param_strings_with_dollarsign(self):
        con = duckdb.connect()

        res = con.execute("select '$name1', $name1, $name1, '$name1'", {'name1': 5}).fetchall()
        assert res == [('$name1', 5, 5, '$name1')]

    def test_named_param_case_insensivity(self):
        con = duckdb.connect()

        res = con.execute(
            """
                select $NaMe1, $NAME2, $name3
            """,
            {'name1': 5, 'nAmE2': 3, 'NAME3': 'a'},
        ).fetchall()
        assert res == [
            (5, 3, 'a'),
        ]

    def test_named_param_keyword(self):
        con = duckdb.connect()

        result = con.execute("SELECT $val", {"val": 42}).fetchone()
        assert result == (42,)

        result = con.execute("SELECT $value", {"value": 42}).fetchone()
        assert result == (42,)

    def test_conversion_from_tuple(self):
        con = duckdb.connect()

        # Tuple converts to list
        result = con.execute("select $1", [(21, 22, 42)]).fetchall()
        assert result == [([21, 22, 42],)]

        # If wrapped in a Value, it can convert to a struct
        result = con.execute("select $1", [Value(('a', 21, True), {'a': str, 'b': int, 'c': bool})]).fetchall()
        assert result == [({'a': 'a', 'b': 21, 'c': True},)]

        # If the amount of items in the tuple and the children of the struct don't match
        # we throw an error
        with pytest.raises(
            duckdb.InvalidInputException,
            match='Tried to create a STRUCT value from a tuple containing 3 elements, but the STRUCT consists of 2 children',
        ):
            result = con.execute("select $1", [Value(('a', 21, True), {'a': str, 'b': int})]).fetchall()

        # If we try to create anything other than a STRUCT or a LIST out of the tuple, we throw an error
        with pytest.raises(duckdb.InvalidInputException, match="Can't convert tuple to a Value of type VARCHAR"):
            result = con.execute("select $1", [Value((21, 42), str)])

    def test_column_name_behavior(self, duckdb_cursor):
        _ = pytest.importorskip("pandas")

        expected_names = ['one', 'ONE_1']

        df = duckdb_cursor.execute('select 1 as one, 2 as "ONE"').fetchdf()
        assert expected_names == list(df.columns)

        duckdb_cursor.register('tbl', df)
        df = duckdb_cursor.execute("select * from tbl").fetchdf()
        assert expected_names == list(df.columns)

        df = duckdb_cursor.execute('with t as (select 1 as one, 2 as "ONE") select * from t').fetchdf()
        assert expected_names == list(df.columns)
