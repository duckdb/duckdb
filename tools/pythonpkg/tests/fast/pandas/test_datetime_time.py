import duckdb
import numpy as np
import pytest
from conftest import NumpyPandas, ArrowPandas
from datetime import datetime, timezone, time, timedelta

_ = pytest.importorskip("pandas", minversion="2.0.0")


class TestDateTimeTime(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_time_high(self, duckdb_cursor, pandas):
        duckdb_time = duckdb_cursor.sql("SELECT make_time(23, 1, 34.234345) AS '0'").df()
        data = [time(hour=23, minute=1, second=34, microsecond=234345)]
        df_in = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_time_low(self, duckdb_cursor, pandas):
        duckdb_time = duckdb_cursor.sql("SELECT make_time(00, 01, 1.000) AS '0'").df()
        data = [time(hour=0, minute=1, second=1)]
        df_in = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        df_out = duckdb.query_df(df_in, "df", "select * from df").df()
        pandas.testing.assert_frame_equal(df_out, duckdb_time)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    @pytest.mark.parametrize('input', ['2263-02-28', '9999-01-01'])
    def test_pandas_datetime_big(self, pandas, input):
        duckdb_con = duckdb.connect()

        duckdb_con.execute("create table test (date DATE)")
        duckdb_con.execute(f"INSERT INTO TEST VALUES ('{input}')")

        res = duckdb_con.execute("select * from test").df()
        date_value = np.array([f'{input}'], dtype='datetime64[us]')
        df = pandas.DataFrame({'date': date_value})
        pandas.testing.assert_frame_equal(res, df)

    def test_timezone_datetime(self):
        con = duckdb.connect()

        dt = datetime.now(timezone.utc).replace(microsecond=0)

        original = dt
        stringified = str(dt)

        original_res = con.execute('select ?::TIMESTAMPTZ', [original]).fetchone()
        stringified_res = con.execute('select ?::TIMESTAMPTZ', [stringified]).fetchone()
        assert original_res == stringified_res
