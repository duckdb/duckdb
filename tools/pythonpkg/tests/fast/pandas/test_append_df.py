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