import duckdb
import pandas as pd
import numpy

class TestCategory(object):
    def test_category_simple(self, duckdb_cursor):
        df_in = pd.DataFrame({
            'float': [1.0, 2.0, 1.0],
            'int': pd.Series([1, 2, 1], dtype="category")
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        assert numpy.all(df_out['float'] == numpy.array([1.0, 2.0, 1.0]))
        assert numpy.all(df_out['int'] == numpy.array([1, 2, 1]))

    def test_category_nulls(self, duckdb_cursor):
        df_in = pd.DataFrame({
            'string': pd.Series(["foo", "bar", None], dtype="category"),
            'int': pd.Series([1, 2, None], dtype="category")
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        assert df_out['string'][0] == "foo"
        assert df_out['string'][1] == "bar"
        assert numpy.isnan(df_out['string'][2])
        assert df_out['int'][0] == 1
        assert df_out['int'][1] == 2
        assert numpy.isnan(df_out['int'][2])

df_in = pd.DataFrame({
    'x': pd.Categorical(['foo','bla','zoo', 'foo',None, 'foo', 'bla',None]),
})
print (pd.Categorical(['foo','bla','zoo', 'foo',None, 'foo', 'bla',None]))
print (df_in)
df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
print (df_out)