import duckdb
import datetime
import os
import pytest
import pandas as pd
import platform
from conftest import pandas_2_or_higher


class TestPandasTimestamps(object):
    @pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
    def test_timestamp_types_roundtrip(self, unit):
        d = {
            'time': pd.Series(
                [pd.Timestamp(datetime.datetime(2020, 6, 12, 14, 43, 24, 394587), unit=unit)],
                dtype=f'datetime64[{unit}]',
            )
        }
        df = pd.DataFrame(data=d)
        df_from_duck = duckdb.from_df(df).df()
        assert df_from_duck.equals(df)

    @pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
    def test_timestamp_timezone_roundtrip(self, unit):
        if pandas_2_or_higher():
            dtype = pd.core.dtypes.dtypes.DatetimeTZDtype(unit=unit, tz='UTC')
            expected_dtype = pd.core.dtypes.dtypes.DatetimeTZDtype(unit='us', tz='UTC')
        else:
            # Older versions of pandas only support 'ns' as timezone unit
            expected_dtype = pd.core.dtypes.dtypes.DatetimeTZDtype(unit='ns', tz='UTC')
            dtype = pd.core.dtypes.dtypes.DatetimeTZDtype(unit='ns', tz='UTC')

        conn = duckdb.connect()
        conn.execute("SET TimeZone =UTC")
        d = {
            'time': pd.Series(
                [pd.Timestamp(datetime.datetime(2020, 6, 12, 14, 43, 24, 394587), unit=unit, tz='UTC')],
                dtype=dtype,
            )
        }
        df = pd.DataFrame(data=d)

        # Our timezone aware type is in US (microseconds), when we scan a timestamp column that isn't US and has timezone info,
        # we convert the time unit to US
        expected = pd.DataFrame(data=d, dtype=expected_dtype)
        df_from_duck = conn.from_df(df).df()
        assert df_from_duck.equals(expected)

    @pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
    def test_timestamp_nulls(self, unit):
        d = {'time': pd.Series([pd.Timestamp(None, unit=unit)], dtype=f'datetime64[{unit}]')}
        df = pd.DataFrame(data=d)
        df_from_duck = duckdb.from_df(df).df()
        assert df_from_duck.equals(df)

    def test_timestamp_timedelta(self):
        df = pd.DataFrame(
            {
                'a': [pd.Timedelta(1, unit='s')],
                'b': [pd.Timedelta(None, unit='s')],
                'c': [pd.Timedelta(1, unit='us')],
                'd': [pd.Timedelta(1, unit='ms')],
            }
        )
        df_from_duck = duckdb.from_df(df).df()
        assert df_from_duck.equals(df)

    @pytest.mark.xfail(
        condition=platform.system() == "Emscripten" and os.environ.get("TZ") != "UTC",
        reason="time zones other than UTC don't seem to work on Pyodide",
    )
    def test_timestamp_timezone(self, duckdb_cursor):
        rel = duckdb_cursor.query(
            """
            SELECT
                '2019-01-01 00:00:00+00'::TIMESTAMPTZ AS dateTime,
                '2019-01-01 00:00:00+00'::TIMESTAMPTZ AS dateTime
        """
        )
        res = rel.df()
        assert res['dateTime'][0] == res['dateTime_1'][0]
