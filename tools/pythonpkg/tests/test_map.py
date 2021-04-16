import duckdb
import pandas as pd
import numpy
import pytest

class TestMap(object):
    def test_map(self, duckdb_cursor):
        testrel = duckdb.values([1, 2])
        
        newdf1 = testrel.map(lambda df : df['col0'].add(42).to_frame())
        newdf2 = testrel.map(lambda df : df['col0'].astype('string').to_frame())
        newdf3 = testrel.map(lambda df : df)

        # column count differs from bind
        def evil1(df):
            if len(df) == 0:
                return df['col0'].to_frame()
            else:
                return df

        # column type differs from bind
        def evil2(df):
            if len(df) == 0:
                df['col0'] = df['col0'].astype('string')
            return df

        # column name differs from bind
        def evil3(df):
            if len(df) == 0:
                df = df.rename(columns={"col0" : "col42"})
            return df

        # does not return a df
        def evil4(df):
            return 42

        # straight up throws exception
        def evil5(df):
            this_makes_no_sense()

        with pytest.raises(RuntimeError):
            print(testrel.map(evil1).df())

        with pytest.raises(RuntimeError):
            print(testrel.map(evil2).df())

        with pytest.raises(RuntimeError):
            print(testrel.map(evil3).df())

        with pytest.raises(AttributeError):
            print(testrel.map(evil4).df())

        with pytest.raises(RuntimeError):
            print(testrel.map(evil5).df())

        # not a function
        with pytest.raises(TypeError):
            print(testrel.map(42).df())

        # nothing passed to map
        with pytest.raises(TypeError):
            print(testrel.map().df())