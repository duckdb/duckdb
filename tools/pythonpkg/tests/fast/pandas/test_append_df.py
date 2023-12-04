import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas


class TestAppendDF(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_df_to_table_append(self, duckdb_cursor, pandas):
        conn = duckdb.connect()
        conn.execute("Create table integers (i integer)")
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 3, 4, 5],
            }
        )
        conn.append('integers', df_in)
        assert conn.execute('select count(*) from integers').fetchone()[0] == 5

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append_by_name(self, pandas):
        con = duckdb.connect()
        con.execute("create table tbl (a integer, b bool, c varchar)")
        df_in = pandas.DataFrame({'c': ['duck', 'db'], 'b': [False, True], 'a': [4, 2]})
        # By default we append by position, causing the following exception:
        with pytest.raises(
            duckdb.ConversionException, match="Conversion Error: Could not convert string 'duck' to INT32"
        ):
            con.append('tbl', df_in)

        # When we use 'by_name' we instead append by name
        con.append('tbl', df_in, by_name=True)
        res = con.table('tbl').fetchall()
        assert res == [(4, False, 'duck'), (2, True, 'db')]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append_by_name_quoted(self, pandas):
        con = duckdb.connect()
        con.execute(
            """
            create table tbl ("needs to be quoted" integer, other varchar)
        """
        )
        df_in = pandas.DataFrame({"needs to be quoted": [1, 2, 3]})
        con.append('tbl', df_in, by_name=True)
        res = con.table('tbl').fetchall()
        assert res == [(1, None), (2, None), (3, None)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append_by_name_no_exact_match(self, pandas):
        con = duckdb.connect()
        con.execute("create table tbl (a integer, b bool)")
        df_in = pandas.DataFrame({'c': ['a', 'b'], 'b': [True, False], 'a': [42, 1337]})
        # Too many columns raises an error, because the columns cant be found in the targeted table
        with pytest.raises(duckdb.BinderException, match='Table "tbl" does not have a column with name "c"'):
            con.append('tbl', df_in, by_name=True)

        df_in = pandas.DataFrame({'b': [False, False, False]})

        # Not matching all columns is not a problem, as they will be filled with NULL instead
        con.append('tbl', df_in, by_name=True)
        res = con.table('tbl').fetchall()
        # 'a' got filled by NULL automatically because it wasn't inserted into
        assert res == [(None, False), (None, False), (None, False)]

        # Empty the table
        con.execute("create or replace table tbl (a integer, b bool)")

        df_in = pandas.DataFrame({'a': [1, 2, 3]})
        con.append('tbl', df_in, by_name=True)
        res = con.table('tbl').fetchall()
        # Also works for missing columns *after* the supplied ones
        assert res == [(1, None), (2, None), (3, None)]
