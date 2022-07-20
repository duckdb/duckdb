import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest

def create_generic_dataframe(data):
    return pd.DataFrame({'col0': pd.Series(data=data, dtype='object')})

class TestResolveObjectColumns(object):
	def test_sample_low_correct(self, duckdb_cursor):
		duckdb_conn = duckdb.connect()
		duckdb_conn.execute("SET GLOBAL pandas_analyze_sample=3")
		data = [1000008, 6, 9, 4, 1, 6]
		df = create_generic_dataframe(data)
		roundtripped_df = duckdb.query_df(df, "x", "select * from x", connection=duckdb_conn).df()
		duckdb_df = duckdb_conn.query("select * FROM (VALUES (1000008), (6), (9), (4), (1), (6)) as '0'").df()
		pd.testing.assert_frame_equal(duckdb_df, roundtripped_df)

	def test_sample_low_incorrect_detected(self, duckdb_cursor):
		duckdb_conn = duckdb.connect()
		duckdb_conn.execute("SET GLOBAL pandas_analyze_sample=2")
		# size of list (6) divided by 'pandas_analyze_sample' (2) is the increment used
		# in this case index 0 (1000008) and index 3 ([4]) are checked, which dont match
		data = [1000008, 6, 9, [4], 1, 6]
		df = create_generic_dataframe(data)
		roundtripped_df = duckdb.query_df(df, "x", "select * from x", connection=duckdb_conn).df()
		# Sample high enough to detect mismatch in types, fallback to VARCHAR
		assert(roundtripped_df['col0'].dtype == np.dtype('object'))

	def test_sample_zero(self, duckdb_cursor):
		duckdb_conn = duckdb.connect()
		# Disable dataframe analyze
		duckdb_conn.execute("SET GLOBAL pandas_analyze_sample=0")
		data = [1000008, 6, 9, 3, 1, 6]
		df = create_generic_dataframe(data)
		roundtripped_df = duckdb.query_df(df, "x", "select * from x", connection=duckdb_conn).df()
		# Always converts to VARCHAR
		assert(roundtripped_df['col0'].dtype == np.dtype('object'))

	def test_sample_low_incorrect_undetected(self, duckdb_cursor):
		duckdb_conn = duckdb.connect()
		duckdb_conn.execute("SET GLOBAL pandas_analyze_sample=1")
		data = [1000008, 6, 9, [4], [1], 6]
		df = create_generic_dataframe(data)
		# Sample size is too low to detect the mismatch, exception is raised when trying to convert
		with pytest.raises(Exception, match="Invalid Input Error: Failed to cast value: Unimplemented type for cast"):
			roundtripped_df = duckdb.query_df(df, "x", "select * from x", connection=duckdb_conn).df()