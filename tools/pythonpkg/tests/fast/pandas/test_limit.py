import duckdb
import pandas as pd


class TestLimitPandas(object):

    def test_limit_df(self, duckdb_cursor):
        df_in = pd.DataFrame({'numbers': [1,2,3,4,5],})
        limit_df = duckdb.limit(df_in,2)
        assert len(limit_df.execute().fetchall()) == 2

    def test_aggregate_df(self, duckdb_cursor):
        df_in = pd.DataFrame({'numbers': [1,2,2,2],})
        aggregate_df = duckdb.aggregate(df_in,'count(numbers)','numbers')
        assert aggregate_df.execute().fetchall() == [(1,), (3,)]