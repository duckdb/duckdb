import duckdb
import pandas as pd

class TestPandasUpdateList(object):
	def test_pandas_update_list(self, duckdb_cursor):
		duckdb_cursor = duckdb.connect(':memory:')
		duckdb_cursor.execute('create table t (l int[])')
		duckdb_cursor.execute('insert into t values ([1, 2]), ([3,4])')
		duckdb_cursor.execute('update t set l = [5, 6]')
		assert duckdb_cursor.execute('select * from t').fetchdf()['l'].tolist()  == [[5, 6], [5, 6]]


