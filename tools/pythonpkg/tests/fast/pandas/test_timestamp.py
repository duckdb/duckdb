import duckdb
import os
import datetime
import pytest
import pandas as pd


class TestPandasTimestamps(object):
    @pytest.mark.parametrize('unit', ['s', 'ms', 'us', 'ns'])
    def test_timestamp_types_roundtrip(self, unit):
        d = {
            'time': [pd.Timestamp(datetime.datetime.now(), unit=unit)],
        }
        df = pd.DataFrame(data=d)
        print(df)
        df_from_duck = duckdb.from_df(df).df()
        assert df_from_duck.equals(df)

    def test_timestamp_nulls(self):
        d = {
            'a': [pd.Timestamp(None, unit='s')],
            'b': [pd.Timestamp(None, unit='ms')],
            'c': [pd.Timestamp(None, unit='us')],
            'd': [pd.Timestamp(None, unit='ns')],
        }
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
