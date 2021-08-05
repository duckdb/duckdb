import pandas as pd
import duckdb
import sys

class TestPandasObject(object):
    def test_object_to_string(self, duckdb_cursor):
        if sys.version_info.major < 3:
            return
            
        con = duckdb.connect(database=':memory:', read_only=False)
        x = pd.DataFrame([[1, 'a', 2],[1, None, 2], [1, 1.1, 2], [1, 1.1, 2], [1, 1.1, 2]])
        x = x.iloc[1:].copy()       # middle col now entirely native float items
        con.register('view2', x)
        df = con.execute('select * from view2').fetchall()
        assert df == [(1, None, 2),(1, '1.1', 2), (1, '1.1', 2), (1, '1.1', 2)]
