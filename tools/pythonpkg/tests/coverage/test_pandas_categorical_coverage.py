import duckdb
import numpy
import pytest
from conftest import NumpyPandas, ArrowPandas


def check_result_list(res):
    for res_item in res:
        assert res_item[0] == res_item[1]


def check_create_table(category, pandas):
    conn = duckdb.connect()

    conn.execute("PRAGMA enable_verification")
    df_in = pandas.DataFrame(
        {
            'x': pandas.Categorical(category, ordered=True),
            'y': pandas.Categorical(category, ordered=True),
            'z': category,
        }
    )

    category.append('bla')

    df_in_diff = pandas.DataFrame(
        {
            'k': pandas.Categorical(category, ordered=True),
        }
    )

    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data")
    df_out = df_out.df()
    assert df_in.equals(df_out)

    conn.execute("CREATE TABLE t1 AS SELECT * FROM df_in")
    conn.execute("CREATE TABLE t2 AS SELECT * FROM df_in")

    # Check fetchall
    res = conn.execute("SELECT x,z FROM t1").fetchall()
    check_result_list(res)

    # Do a insert to trigger string -> cat
    conn.execute("INSERT INTO t1 VALUES ('2','2','2')")

    res = conn.execute("SELECT x FROM t1 where x = '1'").fetchall()
    assert res == [('1',)]

    res = conn.execute("SELECT t1.x FROM t1 inner join t2 on (t1.x = t2.x) order by t1.x").fetchall()
    assert res == conn.execute("SELECT x FROM t1 order by t1.x").fetchall()

    res = conn.execute("SELECT t1.x FROM t1 inner join t2 on (t1.x = t2.y) order by t1.x").fetchall()
    correct_res = conn.execute("SELECT x FROM t1 order by x").fetchall()
    assert res == correct_res

    # Run equal ENUM comparison
    res = conn.execute("SELECT t1.x FROM t1,t2 where t1.x = t2.x order by t1.x").fetchall()
    assert res == correct_res

    # Run different ENUM comparison
    conn.execute("CREATE TABLE t3 AS SELECT * FROM df_in_diff")
    res = conn.execute("SELECT t1.x FROM t1,t3 where t3.k = t1.x order by t1.x").fetchall()
    assert res == correct_res

    # Triggering the cast with ENUM as a src
    conn.execute("ALTER TABLE t1 ALTER x SET DATA TYPE VARCHAR")
    # We should be able to drop the table without any dependencies
    conn.execute("DROP TABLE t1")


# TODO: extend tests with ArrowPandas
class TestCategory(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_category_string_uint16(self, duckdb_cursor, pandas):
        category = []
        for i in range(300):
            category.append(str(i))
        check_create_table(category, pandas)

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_category_string_uint32(self, duckdb_cursor, pandas):
        category = []
        for i in range(70000):
            category.append(str(i))
        check_create_table(category, pandas)
