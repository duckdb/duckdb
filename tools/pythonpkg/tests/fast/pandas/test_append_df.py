import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas

class TestAppendDF(object):

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_df_to_table_append(self, duckdb_cursor, pandas):
        conn = duckdb.connect()
        conn.execute("Create table integers (i integer)")
        df_in = pandas.DataFrame({'numbers': [1,2,3,4,5],})
        conn.append('integers',df_in)
        assert conn.execute('select count(*) from integers').fetchone()[0] == 5

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_append_by_name(self, pandas):
        con = duckdb.connect()
        con.execute("create table tbl (a integer, b bool, c varchar)")
        df_in = pandas.DataFrame({
            'c': ['duck', 'db'],
            'b': [False, True],
            'a': [4,2]
        })
        # By default we append by position, causing the following exception:
        with pytest.raises(duckdb.ConversionException, match="Conversion Error: Could not convert string 'duck' to INT32"):
            con.append('tbl', df_in)

        # When we use 'by_name' we instead append by name
        con.append('tbl', df_in, by_name=True)
        res = con.table('tbl').fetchall()
        assert res == [(4, False, 'duck'), (2, True, 'db')]
