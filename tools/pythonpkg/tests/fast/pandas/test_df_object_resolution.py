import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest

def create_generic_dataframe(data):
    return pd.DataFrame({'0': pd.Series(data=data, dtype='object')})

class IntString:
    def __init__(self, value: int):
        self.value = value
    def __str__(self):
        return str(self.value)

class TestResolveObjectColumns(object):

    def test_integers(self, duckdb_cursor):
        data = [5, 0, 3]
        df_in = create_generic_dataframe(data)
        # These are float64 because pandas would force these to be float64 even if we set them to int8, int16, int32, int64 respectively
        df_expected_res = pd.DataFrame({'0': pd.Series(data=data)})
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(df_out)
        pd.testing.assert_frame_equal(df_expected_res, df_out)

    def test_correct_struct(self, duckdb_cursor):
        data = [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
        df = pd.DataFrame({'0': pd.Series(data=data)})
        duckdb_col = duckdb.query("SELECT {a: 1, b: 3, c: 3, d: 7} as '0'").df()
        converted_col = duckdb.query_df(df, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_incorrect_struct_keys(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'e': 7}], #'e' instead of 'd' as key
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        converted_df = duckdb.query("SELECT * FROM x").df()
        y = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'e'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb.query("SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_incorrect_struct_key_amount(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3}],         #incorrect amount of keys
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        converted_df = duckdb.query("SELECT * FROM x").df()
        y = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c'], 'value': [1, 3, 3]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb.query("SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_struct_key_conversion(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{
                    IntString(5) :      1,
                    IntString(-25):     3,
                    IntString(32):      3,
                    IntString(32456):   7
                }],
            ]
        )
        duckdb_col = duckdb.query("select {'5':1, '-25':3, '32':3, '32456':7} as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_fallthrough_object_conversion(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [IntString(4)],
                [IntString(2)],
                [IntString(0)],
            ]
        )
        duckdb_col = duckdb.query("select * from x").df()
        df_expected_res = pd.DataFrame({'0': pd.Series(['4','2','0'])})
        pd.testing.assert_frame_equal(duckdb_col, df_expected_res)
