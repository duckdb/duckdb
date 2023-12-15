import duckdb
import pandas as pd
import numpy


class TestPandasString(object):
    def test_pandas_string(self, duckdb_cursor):
        strings = numpy.array(['foo', 'bar', 'baz'])

        # https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html
        df_in = pd.DataFrame(
            {
                'object': pd.Series(strings, dtype='object'),
            }
        )
        # Only available in pandas 1.0.0
        if hasattr(pd, 'StringDtype'):
            df_in['string'] = pd.Series(strings, dtype=pd.StringDtype())

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()

        assert numpy.all(df_out['object'] == strings)
        if hasattr(pd, 'StringDtype'):
            assert numpy.all(df_out['string'] == strings)

    def test_bug_2467(self, duckdb_cursor):
        N = 1_000_000
        # Create DataFrame with string attribute
        df = pd.DataFrame({"city": ["Amsterdam", "New York", "London"] * N})
        # Copy Dataframe to DuckDB
        con = duckdb.connect()
        con.register("df", df)
        con.execute(
            f"""
            CREATE TABLE t1 AS SELECT * FROM df
        """
        )
        assert (
            con.execute(
                f"""
            SELECT count(*) from t1
        """
            ).fetchall()
            == [(3000000,)]
        )
