import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest

def create_generic_dataframe(data):
    return pd.DataFrame({0: pd.Series(data=data, dtype='object')})

class TestResolveObjectColumns(object):

    def test_integers(self, duckdb_cursor):
        data = [5, 0, 3]
        df_in = create_generic_dataframe(data)
        # These are float64 because pandas would force these to be float64 even if we set them to int8, int16, int32, int64 respectively
        df_expected_res = pd.DataFrame({0: pd.Series(data=data)})
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(df_out)
        pd.testing.assert_frame_equal(df_expected_res, df_out)
