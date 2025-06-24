import pandas as pd
import duckdb
import numpy as np
import datetime


class TestPandasStride(object):
    def test_stride(self, duckdb_cursor):
        expected_df = pd.DataFrame(np.arange(20).reshape(5, 4), columns=["a", "b", "c", "d"])
        con = duckdb.connect()
        con.register('df_view', expected_df)
        output_df = con.execute("SELECT * FROM df_view;").fetchdf()
        pd.testing.assert_frame_equal(expected_df, output_df)

    def test_stride_fp32(self, duckdb_cursor):
        expected_df = pd.DataFrame(np.arange(20, dtype='float32').reshape(5, 4), columns=["a", "b", "c", "d"])
        con = duckdb.connect()
        con.register('df_view', expected_df)
        output_df = con.execute("SELECT * FROM df_view;").fetchdf()
        for col in output_df.columns:
            assert str(output_df[col].dtype) == 'float32'
        pd.testing.assert_frame_equal(expected_df, output_df)

    def test_stride_datetime(self, duckdb_cursor):
        df = pd.DataFrame({'date': pd.Series(pd.date_range("2024-01-01", freq="D", periods=100))})
        df = df.loc[::23,]

        roundtrip = duckdb_cursor.sql("select * from df").df()
        expected = pd.DataFrame(
            {
                'date': [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 24),
                    datetime.datetime(2024, 2, 16),
                    datetime.datetime(2024, 3, 10),
                    datetime.datetime(2024, 4, 2),
                ]
            }
        )
        pd.testing.assert_frame_equal(roundtrip, expected)

    def test_stride_timedelta(self, duckdb_cursor):
        df = pd.DataFrame({'date': [datetime.timedelta(days=i) for i in range(100)]})
        df = df.loc[::23,]

        roundtrip = duckdb_cursor.sql("select * from df").df()
        expected = pd.DataFrame(
            {
                'date': [
                    datetime.timedelta(days=0),
                    datetime.timedelta(days=23),
                    datetime.timedelta(days=46),
                    datetime.timedelta(days=69),
                    datetime.timedelta(days=92),
                ]
            }
        )
        pd.testing.assert_frame_equal(roundtrip, expected)

    def test_stride_fp64(self, duckdb_cursor):
        expected_df = pd.DataFrame(np.arange(20, dtype='float64').reshape(5, 4), columns=["a", "b", "c", "d"])
        con = duckdb.connect()
        con.register('df_view', expected_df)
        output_df = con.execute("SELECT * FROM df_view;").fetchdf()
        for col in output_df.columns:
            assert str(output_df[col].dtype) == 'float64'
        pd.testing.assert_frame_equal(expected_df, output_df)
