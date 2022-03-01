import duckdb
import os


class TestReplacementScan(object):

	def test_csv_replacement(self, duckdb_cursor):
		filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','integers.csv')
		res = duckdb_cursor.execute("select count(*) from '%s'"%(filename))
		assert res.fetchone()[0] == 2

	def test_parquet_replacement(self, duckdb_cursor):
		filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','binary_string.parquet')
		res = duckdb_cursor.execute("select count(*) from '%s'"%(filename))
		assert res.fetchone()[0] == 3