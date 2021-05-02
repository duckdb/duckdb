import duckdb
import os
import sys
import datetime
import pytest
import pandas as pd

class TestPandasTimestamps(object):
    def test_timestamp_types_roundtrip(self, duckdb_cursor):
        d = {'a': [pd.Timestamp(datetime.datetime.now(), unit='s')], 'b': [pd.Timestamp(datetime.datetime.now(), unit='ms')], 'c': [pd.Timestamp(datetime.datetime.now(), unit='us')], 'd': [pd.Timestamp(datetime.datetime.now(), unit='ns')]}
        df = pd.DataFrame(data=d)
        df_from_duck = duckdb.from_df(df).df()
        assert(df_from_duck.equals(df))

    def test_timestamp_nulls(self, duckdb_cursor):
        d = {'a': [pd.Timestamp(None, unit='s')], 'b': [pd.Timestamp(None, unit='ms')], 'c': [pd.Timestamp(None, unit='us')], 'd': [pd.Timestamp(None, unit='ns')]}
        df = pd.DataFrame(data=d)
        df_from_duck = duckdb.from_df(df).df()
        assert (df_from_duck.equals(df))

    def test_timestamp_timedelta(self, duckdb_cursor):
        df = pd.DataFrame({'a': [pd.Timedelta(1, unit='s')], 'b': [pd.Timedelta(None, unit='s')], 'c': [pd.Timedelta(1, unit='us')] , 'd': [pd.Timedelta(1, unit='ms')]})
        df_from_duck = duckdb.from_df(df).df()
        assert (df_from_duck.equals(df))