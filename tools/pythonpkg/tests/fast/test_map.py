import duckdb
import pandas as pd
import numpy
import pytest
from datetime import date, timedelta

class TestMap(object):
    def test_map(self, duckdb_cursor):
        testrel = duckdb.values([1, 2])
        conn = duckdb.connect()
        conn.execute('CREATE TABLE t (a integer)')
        empty_rel = conn.table('t')

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

        def return_dataframe(df):
            return pd.DataFrame({'A' : [1]})

        def return_big_dataframe(df):
            return pd.DataFrame({'A' : [1]*5000})

        def return_none(df):
            return None

        def return_empty_df(df):
            return pd.DataFrame()

        with pytest.raises(RuntimeError):
            print(testrel.map(evil1).df())

        with pytest.raises(RuntimeError):
            print(testrel.map(evil2).df())

        with pytest.raises(RuntimeError):
            print(testrel.map(evil3).df())

        with pytest.raises(AttributeError):
            print(testrel.map(evil4).df())

        with pytest.raises(duckdb.Error):
            print(testrel.map(evil5).df())

        # not a function
        with pytest.raises(TypeError):
            print(testrel.map(42).df())

        # nothing passed to map
        with pytest.raises(TypeError):
            print(testrel.map().df())

        testrel.map(return_dataframe).df().equals(pd.DataFrame({'A' : [1]}))
        
        with pytest.raises(Exception):
            testrel.map(return_big_dataframe).df()

        empty_rel.map(return_dataframe).df().equals(pd.DataFrame({'A' : []}))

        with pytest.raises(Exception):
            testrel.map(return_none).df()

        with pytest.raises(Exception):
            testrel.map(return_empty_df).df()

    def test_isse_3237(self, duckdb_cursor):
        def process(rel):
            def mapper(x):
                dates = x['date'].to_numpy("datetime64[us]")
                days = x['days_to_add'].to_numpy("int")
                x["result1"] = pd.Series([pd.to_datetime(y[0]).date() + timedelta(days=y[1].item()) for y in zip(dates,days)], dtype='datetime64[us]')
                x["result2"] = pd.Series([pd.to_datetime(y[0]).date() + timedelta(days=-y[1].item()) for y in zip(dates,days)], dtype='datetime64[us]')
                return x

            rel = rel.map(mapper)
            rel = rel.project("*, datediff('day', date, result1) as one")
            rel = rel.project("*, datediff('day', date, result2) as two")
            rel = rel.project("*, IF(ABS(one) > ABS(two), one, two) as three")            
            return rel

        df = pd.DataFrame({'date': pd.Series([date(2000,1,1), date(2000,1,2)], dtype="datetime64[us]"), 'days_to_add': [1,2]})
        rel = duckdb.from_df(df)
        rel = process(rel)
        x = rel.execute().fetchdf()
        assert x['days_to_add'].to_numpy()[0] == 1
