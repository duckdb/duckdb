import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest


class TestDateTimeTimeStamp(object):

    def test_timestamp_high(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT '2260-01-01 23:59:00'::TIMESTAMP AS '0'").df()
        df_in = pd.DataFrame(
            {0: pd.Series(data=[datetime.datetime(year=2260, month=1, day=1, hour=23, minute=59)], dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_timestamp_low(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT '1680-01-01 23:59:00'::TIMESTAMP AS '0'").df()
        df_in = pd.DataFrame(
            {0: pd.Series(data=[datetime.datetime(year=1680, month=1, day=1, hour=23, minute=59)], dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_timestamp_timezone_regular(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT '2022-01-01 12:00:00'::TIMESTAMP AS '0'").df()
        # time is 3 hours ahead of UTC
        offset = datetime.timedelta(hours=3)
        timezone = datetime.timezone(offset)
        df_in = pd.DataFrame(
            {0: pd.Series(data=[datetime.datetime(year=2022, month=1, day=1, hour=15, tzinfo=timezone)], dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_timestamp_timezone_negative_extreme(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT '2022-01-01 12:00:00'::TIMESTAMP AS '0'").df()
        # time is 14 hours behind UTC
        offset = datetime.timedelta(hours=-14)
        timezone = datetime.timezone(offset)
        df_in = pd.DataFrame(
            {0: pd.Series(data=[datetime.datetime(year=2021, month=12, day=31, hour=22, tzinfo=timezone)], dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_timestamp_timezone_positive_extreme(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT '2021-12-31 23:00:00'::TIMESTAMP AS '0'").df()
        # time is 20 hours ahead of UTC
        offset = datetime.timedelta(hours=20)
        timezone = datetime.timezone(offset)
        df_in = pd.DataFrame(
            {0: pd.Series(data=[datetime.datetime(year=2022, month=1, day=1, hour=19, tzinfo=timezone)], dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)
