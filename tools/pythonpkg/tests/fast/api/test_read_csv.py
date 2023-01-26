import numpy
import datetime
import pandas

class TestReadCSV(object):
	def test_read_csv(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', dtype={'category_id': 'string'})
		assert rel.fetchone() == ('1', 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

