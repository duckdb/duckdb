import duckdb
import pandas as pd
import pytest

class TestPandasLimit(object):
    def test_pandas_limit(self, duckdb_cursor):
        con = duckdb.connect()
        df = con.execute('select * from range(10000000) tbl(i)').df()

        con.execute('SET threads=8')

        limit_df = con.execute('SELECT * FROM df WHERE i=334 OR i>9967864 LIMIT 5').df()
        assert list(limit_df['i']) == [334, 9967865, 9967866, 9967867, 9967868]
