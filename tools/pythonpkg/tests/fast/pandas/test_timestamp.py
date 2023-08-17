import duckdb
import os
import datetime
import pytest
import pandas as pd


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
