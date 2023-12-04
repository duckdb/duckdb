import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest


class TestTimedelta(object):
    def test_timedelta_positive(self, duckdb_cursor):
        duckdb_interval = duckdb.query(
            "SELECT '2290-01-01 23:59:00'::TIMESTAMP - '2000-01-01 23:59:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=9151574400000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)

    def test_timedelta_coverage(self, duckdb_cursor):
        duckdb_interval = duckdb.query(
            "SELECT '2290-08-30 23:53:40'::TIMESTAMP - '2000-02-01 01:56:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=9169797460000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)

    def test_timedelta_negative(self, duckdb_cursor):
        duckdb_interval = duckdb.query(
            "SELECT '2000-01-01 23:59:00'::TIMESTAMP - '2290-01-01 23:59:00'::TIMESTAMP AS '0'"
        ).df()
        data = [datetime.timedelta(microseconds=-9151574400000000)]
        df_in = pd.DataFrame({0: pd.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_interval)
