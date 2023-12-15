import duckdb
import os
import datetime
import pytest
import pandas as pd
import io


class TestPandasStringNull(object):
    def test_pandas_string_null(self, duckdb_cursor):
        csv = u'''what,is_control,is_test
,0,0
foo,1,0'''
        df = pd.read_csv(io.StringIO(csv))
        duckdb_cursor.register("c", df)
        duckdb_cursor.execute('select what, count(*) from c group by what')
        df_result = duckdb_cursor.fetchdf()
        assert True  # Should not crash ^^
