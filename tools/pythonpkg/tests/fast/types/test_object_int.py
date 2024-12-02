import numpy as np
import datetime
import duckdb
import pytest
import warnings
from contextlib import suppress


class TestPandasObjectInteger(object):
    # Signed Masked Integer types
    def test_object_integer(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        df_in = pd.DataFrame(
            {
                'int8': pd.Series([None, 1, -1], dtype="Int8"),
                'int16': pd.Series([None, 1, -1], dtype="Int16"),
                'int32': pd.Series([None, 1, -1], dtype="Int32"),
                'int64': pd.Series([None, 1, -1], dtype="Int64"),
            }
        )
        warnings.simplefilter(action='ignore', category=RuntimeWarning)
        df_expected_res = pd.DataFrame(
            {
                'int8': pd.Series(np.ma.masked_array([0, 1, -1], mask=[True, False, False]), dtype='Int8'),
                'int16': pd.Series(np.ma.masked_array([0, 1, -1], mask=[True, False, False]), dtype='Int16'),
                'int32': pd.Series(np.ma.masked_array([0, 1, -1], mask=[True, False, False]), dtype='Int32'),
                'int64': pd.Series(np.ma.masked_array([0, 1, -1], mask=[True, False, False]), dtype='Int64'),
            }
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        warnings.resetwarnings()
        pd.testing.assert_frame_equal(df_expected_res, df_out)

    # Unsigned Masked Integer types
    def test_object_uinteger(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        with suppress(TypeError):
            df_in = pd.DataFrame(
                {
                    'uint8': pd.Series([None, 1, 255], dtype="UInt8"),
                    'uint16': pd.Series([None, 1, 65535], dtype="UInt16"),
                    'uint32': pd.Series([None, 1, 4294967295], dtype="UInt32"),
                    'uint64': pd.Series([None, 1, 18446744073709551615], dtype="UInt64"),
                }
            )
            warnings.simplefilter(action='ignore', category=RuntimeWarning)
            df_expected_res = pd.DataFrame(
                {
                    'uint8': pd.Series(np.ma.masked_array([0, 1, 255], mask=[True, False, False]), dtype='UInt8'),
                    'uint16': pd.Series(np.ma.masked_array([0, 1, 65535], mask=[True, False, False]), dtype='UInt16'),
                    'uint32': pd.Series(
                        np.ma.masked_array([0, 1, 4294967295], mask=[True, False, False]), dtype='UInt32'
                    ),
                    'uint64': pd.Series(
                        np.ma.masked_array([0, 1, 18446744073709551615], mask=[True, False, False]), dtype='UInt64'
                    ),
                }
            )
            df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
            warnings.resetwarnings()
            pd.testing.assert_frame_equal(df_expected_res, df_out)

    # Unsigned Masked float/double types
    def test_object_float(self, duckdb_cursor):
        # Require pandas 1.2.0 >= for this, because Float32|Float64 was not added before this version
        pd = pytest.importorskip("pandas", '1.2.0')
        df_in = pd.DataFrame(
            {
                'float32': pd.Series([None, 1, 4294967295], dtype="Float32"),
                'float64': pd.Series([None, 1, 18446744073709551615], dtype="Float64"),
            }
        )
        df_expected_res = pd.DataFrame(
            {
                'float32': pd.Series(
                    np.ma.masked_array([0, 1, 4294967295], mask=[True, False, False]), dtype='float32'
                ),
                'float64': pd.Series(
                    np.ma.masked_array([0, 1, 18446744073709551615], mask=[True, False, False]), dtype='float64'
                ),
            }
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(df_expected_res, df_out)
