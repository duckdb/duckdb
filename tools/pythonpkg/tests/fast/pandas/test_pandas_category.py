import duckdb
import pandas as pd
import numpy
import pytest


def check_category_equal(category):
    df_in = pd.DataFrame(
        {
            'x': pd.Categorical(category, ordered=True),
        }
    )
    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    assert df_in.equals(df_out)


def check_result_list(category, res):
    for i in range(len(category)):
        assert category[i][0] == res[i]


def check_create_table(category):
    conn = duckdb.connect()

    conn.execute("PRAGMA enable_verification")
    df_in = pd.DataFrame({'x': pd.Categorical(category, ordered=True), 'y': pd.Categorical(category, ordered=True)})

    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    assert df_in.equals(df_out)

    conn.execute("CREATE TABLE t1 AS SELECT * FROM df_in")
    conn.execute("CREATE TABLE t2 AS SELECT * FROM df_in")

    # Check fetchall
    res = conn.execute("SELECT t1.x FROM t1").fetchall()
    check_result_list(res, category)

    # Do a insert to trigger string -> cat
    conn.execute("INSERT INTO t1 VALUES ('2','2')")

    res = conn.execute("SELECT x FROM t1 where x = '1'").fetchall()
    assert res == [('1',)]

    res = conn.execute("SELECT t1.x FROM t1 inner join t2 on (t1.x = t2.x)").fetchall()
    assert res == conn.execute("SELECT x FROM t1").fetchall()

    res = conn.execute("SELECT t1.x FROM t1 inner join t2 on (t1.x = t2.y)").fetchall()
    assert res == conn.execute("SELECT x FROM t1").fetchall()

    assert res == conn.execute("SELECT x FROM t1").fetchall()
    # Triggering the cast with ENUM as a src
    conn.execute("ALTER TABLE t1 ALTER x SET DATA TYPE VARCHAR")
    # We should be able to drop the table without any dependencies
    conn.execute("DROP TABLE t1")


class TestCategory(object):
    def test_category_simple(self, duckdb_cursor):
        df_in = pd.DataFrame({'float': [1.0, 2.0, 1.0], 'int': pd.Series([1, 2, 1], dtype="category")})

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall())
        print(df_out['int'])
        assert numpy.all(df_out['float'] == numpy.array([1.0, 2.0, 1.0]))
        assert numpy.all(df_out['int'] == numpy.array([1, 2, 1]))

    def test_category_nulls(self, duckdb_cursor):
        df_in = pd.DataFrame({'int': pd.Series([1, 2, None], dtype="category")})
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall())
        assert df_out['int'][0] == 1
        assert df_out['int'][1] == 2
        assert pd.isna(df_out['int'][2])

    def test_category_string(self, duckdb_cursor):
        check_category_equal(['foo', 'bla', 'zoo', 'foo', 'foo', 'bla'])

    def test_category_string_null(self, duckdb_cursor):
        check_category_equal(['foo', 'bla', None, 'zoo', 'foo', 'foo', None, 'bla'])

    def test_category_string_null_bug_4747(self, duckdb_cursor):
        check_category_equal([str(i) for i in range(160)] + [None])

    def test_categorical_fetchall(self, duckdb_cursor):
        df_in = pd.DataFrame(
            {
                'x': pd.Categorical(['foo', 'bla', None, 'zoo', 'foo', 'foo', None, 'bla'], ordered=True),
            }
        )
        assert duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchall() == [
            ('foo',),
            ('bla',),
            (None,),
            ('zoo',),
            ('foo',),
            ('foo',),
            (None,),
            ('bla',),
        ]

    def test_category_string_uint8(self, duckdb_cursor):
        category = []
        for i in range(10):
            category.append(str(i))
        check_create_table(category)

    def test_empty_categorical(self, duckdb_cursor):
        empty_categoric_df = pd.DataFrame({'category': pd.Series(dtype='category')})
        duckdb_cursor.execute("CREATE TABLE test AS SELECT * FROM empty_categoric_df")
        res = duckdb_cursor.table('test').fetchall()
        assert res == []

        with pytest.raises(duckdb.ConversionException, match="Could not convert string 'test' to UINT8"):
            duckdb_cursor.execute("insert into test VALUES('test')")
        duckdb_cursor.execute("insert into test VALUES(NULL)")
        res = duckdb_cursor.table('test').fetchall()
        assert res == [(None,)]

    def test_category_fetch_df_chunk(self, duckdb_cursor):
        con = duckdb.connect()
        categories = ['foo', 'bla', None, 'zoo', 'foo', 'foo', None, 'bla']
        result = categories * 256
        categories = result * 2
        df_result = pd.DataFrame(
            {
                'x': pd.Categorical(result, ordered=True),
            }
        )
        df_in = pd.DataFrame(
            {
                'x': pd.Categorical(categories, ordered=True),
            }
        )
        con.register("data", df_in)

        query = con.execute("SELECT * FROM data")
        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk.equals(df_result)

        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk.equals(df_result)

        cur_chunk = query.fetch_df_chunk()
        assert cur_chunk.empty

    def test_category_mix(self, duckdb_cursor):
        df_in = pd.DataFrame(
            {
                'float': [1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 0.0],
                'x': pd.Categorical(['foo', 'bla', None, 'zoo', 'foo', 'foo', None, 'bla'], ordered=True),
            }
        )

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        assert df_out.equals(df_in)
