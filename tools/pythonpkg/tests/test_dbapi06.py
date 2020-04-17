#simple DB API testcase

import pandas as pd

class TestSimpleDBAPI(object):
	def test_to_sql(self, duckdb_cursor):
		con = duckdb_cursor
		df = pd.DataFrame([[None, 10, 1.0], ['nick', None, 1.5], ['juli', 14, None]], columns = ['Name', 'Age', 'Numeric'])
		df.to_sql('ages', con)

		result = duckdb_cursor.execute('SELECT count(*), sum("Age"), sum("Numeric") FROM ages').fetchone()
		assert result == (3, 24, 2.5,)
