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

    def test_pandas_fixed_width_string(self, duckdb_cursor):
        strings = numpy.array(['f', 'bar', 'bazzz'], dtype='S3')
        objects = numpy.array(['f', 'bar', 'bazzz'], dtype='U3')

        # https://numpy.org/doc/stable/user/basics.strings.html#fixed-width-data-types
        df_in = pd.DataFrame(
            {
                'str': strings,
                'obj': objects,
            },
            copy=False,
        )
        assert df_in['str'].dtype == numpy.dtype('S3') and str(df_in['str'].dtype) == '|S3'
        assert df_in['obj'].dtype == numpy.dtype('O')

        out = duckdb.query_df(df_in, "data", "SELECT str FROM data").fetchall()
        assert out == [('f',), ('bar',), ('baz',)]

        df_out = duckdb.query_df(df_in, "data", "SELECT length(str) AS len FROM data").df()
        assert numpy.all(df_out['len'] == [1, 3, 3])

        df_in = df_in[df_in['str'].str.decode('utf-8').str.startswith('b')]
        out = duckdb.query_df(df_in, "data", "SELECT str FROM data").fetchall()
        assert out == [('bar',), ('baz',)]

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
