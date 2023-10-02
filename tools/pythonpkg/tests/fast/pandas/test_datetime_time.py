import duckdb
import numpy as np
import pytest
from conftest import NumpyPandas, ArrowPandas
from datetime import datetime, timezone, time, timedelta


class TestDateTimeTime(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_time_high(self, duckdb_cursor, pandas):
        duckdb_time = duckdb.query("SELECT make_time(23, 1, 34.234345) AS '0'").df()
        data = [time(hour=23, minute=1, second=34, microsecond=234345)]
        df_in = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_time_low(self, duckdb_cursor, pandas):
        duckdb_time = duckdb.query("SELECT make_time(00, 01, 1.000) AS '0'").df()
        data = [time(hour=0, minute=1, second=1)]
        df_in = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_pandas_datetime_overflow(self, pandas):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table test (date DATE)")
        duckdb_con.execute("INSERT INTO TEST VALUES ('2263-02-28')")

        with pytest.raises(duckdb.ConversionException):
            res = duckdb_con.execute("select * from test").df()

    def test_timezone_datetime(self):
        con = duckdb.connect()

        dt = datetime.now(timezone.utc).replace(microsecond=0)

        original = dt
        stringified = str(dt)

        original_res = con.execute('select ?::TIMESTAMPTZ', [original]).fetchone()
        stringified_res = con.execute('select ?::TIMESTAMPTZ', [stringified]).fetchone()
        assert original_res == stringified_res
