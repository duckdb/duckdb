import duckdb
import pandas as pd
import numpy

class TestPandasString(object):
    def test_pandas_string(self, duckdb_cursor):
        strings = numpy.array(['foo', 'bar', 'baz'])

        # https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html
        df_in = pd.DataFrame({
            'object': pd.Series(strings, dtype='object'),
        })
        # Only available in pandas 1.0.0
        if hasattr(pd, 'StringDtype'):
            df_in['string'] = pd.Series(strings, dtype=pd.StringDtype())

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()

        assert numpy.all(df_out['object'] == strings)
        if hasattr(pd, 'StringDtype'):
            assert numpy.all(df_out['string'] == strings)
