import pandas as pd
import duckdb



class TestPandasAcceptFloat16(object):
    def test_pandas_accept_float16(self, duckdb_cursor):
        df = pd.DataFrame({'col': [1,2,3]})
        df16 = df.astype({'col':'float16'})
        con = duckdb.connect()
        con.execute('CREATE TABLE tbl AS SELECT * FROM df16')
        con.execute('select * from tbl')
        df_result = con.fetchdf()
        df32 = df.astype({'col':'float32'})
        assert((df32['col'] == df_result['col']).all())