import pandas as pd
import numpy as np
import datetime
import duckdb
from pandas._testing import assert_frame_equal

class TestPandasObjectInteger(object):
    def test_object_integer(self, duckdb_cursor):
        df_in = pd.DataFrame({
                'int8': pd.Series([pd.NA, 1, -1], dtype="Int8"),
                'int16': pd.Series([pd.NA, 1, -1], dtype="Int16"),
                'int32': pd.Series([pd.NA, 1, -1], dtype="Int32"),
                'int64': pd.Series([pd.NA, 1, -1], dtype="Int64")}
        )
        df_expected_res = pd.DataFrame({
                'int8': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int16': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int32': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),
                'int64': np.ma.masked_array([0,1,-1], mask=[True,False,False], dtype='float64'),}
        )
        # RuntimeError: unsupported python type Int64
        df_out = duckdb.query(df_in, "data", "SELECT * FROM data").df()
        assert_frame_equal(df_expected_res, df_out)

