import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest


class TestDateTimeTime(object):

    def test_time_high(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT make_time(23, 1, 34.234345) AS '0'").df()
        data = [datetime.time(hour=23, minute=1, second=34, microsecond=234345)]
        df_in = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_time_low(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT make_time(00, 01, 1.000) AS '0'").df()
        data = [datetime.time(hour=0, minute=1, second=1)]
        df_in = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_time_timezone_regular(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT make_time(00, 01, 1.000) AS '0'").df()
        # time is 3 hours ahead of UTC
        offset = datetime.timedelta(hours=3)
        timezone = datetime.timezone(offset)
        data = [datetime.time(hour=3, minute=1, second=1, tzinfo=timezone)]
        df_in = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_time_timezone_negative_extreme(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT make_time(12, 01, 1.000) AS '0'").df()
        # time is 14 hours behind UTC
        offset = datetime.timedelta(hours=-14)
        timezone = datetime.timezone(offset)
        data = [datetime.time(hour=22, minute=1, second=1, tzinfo=timezone)]
        df_in = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_time_timezone_positive_extreme(self, duckdb_cursor):
        duckdb_time = duckdb.query("SELECT make_time(12, 01, 1.000) AS '0'").df()
        # time is 20 hours ahead of UTC
        offset = datetime.timedelta(hours=20)
        timezone = datetime.timezone(offset)
        data = [datetime.time(hour=8, minute=1, second=1, tzinfo=timezone)]
        df_in = pd.DataFrame(
            {'0': pd.Series(data=data, dtype='object')}
        )
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pd.testing.assert_frame_equal(df_out, duckdb_time)

    def test_pandas_datetime_overflow(self):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table test (date DATE)")
        duckdb_con.execute("INSERT INTO TEST VALUES ('2263-02-28')")

        with pytest.raises(duckdb.ConversionException):
            res = duckdb_con.execute("select * from test").df()

    def test_timezone_datetime(self):
        module = pytest.importorskip("datetime")
        timezone = module.timezone
        datetime = module.datetime

        con = duckdb.connect()

        dt = datetime.now(timezone.utc).replace(microsecond=0)

        original = dt
        stringified = str(dt)

        original_res = con.execute('select ?::TIMESTAMPTZ', [original]).fetchone()
        stringified_res = con.execute('select ?::TIMESTAMPTZ', [stringified]).fetchone()
        assert original_res == stringified_res
