import pandas as pd
import duckdb
import numpy as np

class TestPandasStride(object):

    def test_stride(self, duckdb_cursor): 
        expected_df = pd.DataFrame(np.arange(20).reshape(5, 4), columns=["a", "b", "c", "d"])
        con = duckdb.connect()
        con.register('df_view', expected_df)
        output_df = con.execute("SELECT * FROM df_view;").fetchdf()
        pd.testing.assert_frame_equal(expected_df, output_df)
   