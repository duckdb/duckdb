import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas


class TestParameterList(object):
    def test_bool(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table bool_table (a bool)")
        conn.execute("insert into bool_table values (TRUE)")
        res = conn.execute("select count(*) from bool_table where a =?", [True])
        assert res.fetchone()[0] == 1

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_exception(self, duckdb_cursor, pandas):
        conn = duckdb.connect()
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 3, 4, 5],
            }
        )
        conn.execute("create table bool_table (a bool)")
        conn.execute("insert into bool_table values (TRUE)")
        with pytest.raises(duckdb.NotImplementedException, match='Unable to transform'):
            res = conn.execute("select count(*) from bool_table where a =?", [df_in])

    def test_explicit_nan_param(self):
        con = duckdb.default_connection()
        res = con.execute('select isnan(cast(? as double))', (float("nan"),))
        assert res.fetchone()[0] == True

    def test_string_parameter(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("create table orders (o_orderdate date)")
        conn.execute("insert into orders values (date '1992-01-01'), (date '1994-01-01')")
        conn.execute(
            """
            SELECT COUNT(*)
            FROM ORDERS
            WHERE O_ORDERDATE BETWEEN ? AND ?
            """,
            ["1994-01-01", "1996-01-01"],
        )
        res = conn.fetchall()
        assert res == [(1,)]
