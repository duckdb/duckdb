import pandas as pd
import numpy as np
import datetime
import duckdb

class TestPandasObjectInteger(object):
	# Signed Masked Integer types
    def test_object_integer(self, duckdb_cursor):
        df_in = pd.DataFrame({
                'int8': pd.Series([None, 1, -1], dtype="Int8"),
                'int16': pd.Series([None, 1, -1], dtype="Int16"),
                'int32': pd.Series([None, 1, -1], dtype="Int32"),
                'int64': pd.Series([None, 1, -1], dtype="Int64")}
        )
		# These are float64 because pandas would force these to be float64 even if we set them to int8, int16, int32, int64 respectively
        df_expected_res = pd.DataFrame({
                'int8': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int16': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int32': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int64': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),}
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(df_expected_res, df_out)

	# Unsigned Masked Integer types
    def test_object_uinteger(self, duckdb_cursor):
        df_in = pd.DataFrame({
                'uint8': pd.Series([None, 1, 255], dtype="UInt8"),
                'uint16': pd.Series([None, 1, 65535], dtype="UInt16"),
                'uint32': pd.Series([None, 1, 4294967295], dtype="UInt32"),
                'uint64': pd.Series([None, 1, 18446744073709551615], dtype="UInt64")}
        )
		# These are float64 because pandas would force these to be float64 even if we set them to uint8, uint16, uint32, uint64 respectively
        df_expected_res = pd.DataFrame({
                'uint8': np.ma.masked_array([0,1,255], mask=[True,False,False], dtype='float64'),
                'uint16': np.ma.masked_array([0,1,65535], mask=[True,False,False], dtype='float64'),
                'uint32': np.ma.masked_array([0,1,4294967295], mask=[True,False,False], dtype='float64'),
                'uint64': np.ma.masked_array([0,1,18446744073709551615], mask=[True,False,False], dtype='float64'),}
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(df_expected_res, df_out)

	# Unsigned Masked float/double types
    def test_object_uinteger(self, duckdb_cursor):
        df_in = pd.DataFrame({
                'float32': pd.Series([None, 1, 4294967295], dtype="Float32"),
                'float64': pd.Series([None, 1, 18446744073709551615], dtype="Float64")}
        )
        df_expected_res = pd.DataFrame({
                'float32': np.ma.masked_array([0,1,4294967295], mask=[True,False,False], dtype='float32'),
                'float64': np.ma.masked_array([0,1,18446744073709551615], mask=[True,False,False], dtype='float64'),}
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(df_expected_res, df_out)
