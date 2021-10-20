import pandas as pd
import numpy as np
import datetime
import duckdb

class TestPandasTimestampTz(object):
    def test_timestamp_tz(self, duckdb_cursor):
        df_in = pd.DataFrame({
                'datetime': [pd.Timestamp('20180310T11:17:54Z')],
                'string': ['foo']}
        )
        df_expected_res = pd.DataFrame({
                'datetime': [pd.Timestamp('20180310T11:17:54')],
                'string': ['foo']}
        )
        print(df_in)
        print(df_expected_res)
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(df_out)
        pd.testing.assert_frame_equal(df_expected_res, df_out)
