import duckdb
import pandas as pd
import numpy

def check_category_equal(category):
    df_in = pd.DataFrame({
    'x': pd.Categorical(category, ordered=True),
    })
    print (df_in)
    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    
    print (duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall())
    print (df_out)
    assert df_in.equals(df_out)

class TestCategory(object):

    # def test_category_simple(self, duckdb_cursor):
    #     df_in = pd.DataFrame({
    #         'float': [1.0, 2.0, 1.0],
    #         'int': pd.Series([1, 2, 1], dtype="category")
    #     })

    #     df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    #     assert numpy.all(df_out['float'] == numpy.array([1.0, 2.0, 1.0]))
    #     assert numpy.all(df_out['int'] == numpy.array([1, 2, 1]))

    # def test_category_nulls(self, duckdb_cursor):
    #     df_in = pd.DataFrame({
    #         'int': pd.Series([1, 2, None], dtype="category")
    #     })
    #     df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    #     print (duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall())
    #     assert df_out['int'][0] == 1
    #     assert df_out['int'][1] == 2
    #     assert numpy.isnan(df_out['int'][2])

    def test_category_string(self, duckdb_cursor):
        check_category_equal(['foo','bla','zoo', 'foo', 'foo', 'bla'])

    # def test_category_string_null(self, duckdb_cursor):
    #     check_category_equal(['foo','bla',None,'zoo', 'foo', 'foo',None, 'bla'])

    def test_categorical_fetchall(self, duckdb_cursor):
        df_in = pd.DataFrame({
        'x': pd.Categorical(['foo','bla',None,'zoo', 'foo', 'foo',None, 'bla'], ordered=True),
        })
        assert duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall() == [('foo',), ('bla',), (None,), ('zoo',), ('foo',), ('foo',), (None,), ('bla',)]
    
    def test_category_string_int16(self, duckdb_cursor):
        category = []
        for i in range (300):
            category.append(str(i))
        check_category_equal(category)

    # def test_category_string_int32(self, duckdb_cursor):
    #     category = []
    #     for i in range (33000):
    #         category.append(str(i))
    #     check_category_equal(category)

    # def test_category_string_int32(self, duckdb_cursor):
    #     category = []
    #     for i in range (2147483800):
    #         category.append(str(i))
    #     check_category_equal(category)
