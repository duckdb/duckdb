import duckdb
import pandas as pd

class TestAppendDF(object):

    def test_df_to_table_append(self, duckdb_cursor):
        conn = duckdb.connect()
        conn.execute("Create table integers (i integer)")
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        conn.append('integers',df_in)
        assert conn.execute('select count(*) from integers').fetchone()[0] == 5