import numpy
import datetime
import pandas
import pytest
import duckdb

class TestReadCSV(object):
	def test_no_options(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv')
		res = rel.fetchone()
		print(res)
		assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

	def test_dtype(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', dtype={'category_id': 'string'})
		res = rel.fetchone()
		print(res)
		assert res == ('1', 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

	def test_sep(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', sep=" ")
		res = rel.fetchone()
		print(res)
		assert res == ('1|Action|2006-02-15', datetime.time(4, 46, 27))

	def test_delimiter(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', delimiter=" ")
		res = rel.fetchone()
		print(res)
		assert res == ('1|Action|2006-02-15', datetime.time(4, 46, 27))

	def test_delimiter_and_sep(self, duckdb_cursor):
		with pytest.raises(duckdb.InvalidInputException, match="read_csv takes either 'delimiter' or 'sep', not both"):
			rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', delimiter=" ", sep=" ")

	def test_header_true(self, duckdb_cursor):
		rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', header=True)
		res = rel.fetchone()
		print(res)
		assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

	# uncomment after issue #6011 is fixed
	#def test_header_false(self, duckdb_cursor):
	#	rel = duckdb_cursor.read_csv('test/sakila/data/category.csv', header=False)
	#	res = rel.fetchone()
	#	print(res)
	#	assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))
