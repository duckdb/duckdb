import numpy
import datetime
import pandas
import pytest
import duckdb

def TestFile(name):
	import os
	filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','data',name)
	return filename

class TestReadJSON(object):
	def test_using_connection_wrapper(self):
		rel = duckdb.read_json(TestFile('example.json'), {'id':'integer', 'name':'varchar'})
		res = rel.fetchone()
		print(res)
		assert res == (1, 'O Brother, Where Art Thou?')

	def test_no_options(self, duckdb_cursor):
		rel = duckdb_cursor.read_json(TestFile('example.json'), {'id':'integer', 'name':'varchar'})
		res = rel.fetchone()
		print(res)
		assert res == (1, 'O Brother, Where Art Thou?')
