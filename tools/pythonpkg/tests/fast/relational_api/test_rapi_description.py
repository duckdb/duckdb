import duckdb
import pytest

class TestRAPIDescription(object):
    def test_rapi_description(self):
        res = duckdb.query('select 42::INT AS a, 84::BIGINT AS b')
        desc = res.description
        names = [x[0] for x in desc]
        types = [x[1] for x in desc]
        assert names == ['a', 'b']
        assert types == ['NUMBER', 'NUMBER']

    def test_rapi_describe(self):
        np = pytest.importorskip("numpy")
        pd = pytest.importorskip("pandas")
        res = duckdb.query('select 42::INT AS a, 84::BIGINT AS b')
        duck_describe = res.describe().df()
        pandas_describe = res.df().describe()
        for name in ['a', 'b']:
            np.testing.assert_array_equal(np.array(duck_describe[name]), np.array(pandas_describe[name]))

        # now with more values
        res = duckdb.query('select CASE WHEN i%2=0 THEN i ELSE NULL END AS i, i * 10 AS j, (i * 23 / 27)::DOUBLE AS k FROM range(10000) t(i)')
        duck_describe = res.describe().df()
        pandas_describe = res.df().describe()
        for name in ['i', 'j', 'k']:
            np.testing.assert_allclose(np.array(duck_describe[name]), np.array(pandas_describe[name]))

        # describe data with other (non-numeric) types
        res = duckdb.query("select 'hello world' AS a, [1, 2, 3] AS b")
        duck_describe = res.describe().df()
        np.testing.assert_array_equal(duck_describe['a'], [1, 'hello world', 1])
        np.testing.assert_array_equal(duck_describe['b'], [1, '[1, 2, 3]', 1])

        # describe mixed table
        res = duckdb.query("select 42::INT AS a, 84::BIGINT AS b, 'hello world' AS c")
        duck_describe = res.describe().df()
        pandas_describe = res.df().describe()
        for name in ['a', 'b']:
            np.testing.assert_array_equal(np.array(duck_describe[name]), np.array(pandas_describe[name]))

        # timestamps
        res = duckdb.query("select timestamp '1992-01-01', date '2000-01-01'")
        duck_describe = res.describe().df()
        assert len(duck_describe) > 0

        # describe empty result
        res = duckdb.query("select 42 AS a LIMIT 0")
        duck_describe = res.describe().df()
        pandas_describe = res.df().describe()
        np.testing.assert_array_equal(np.array(duck_describe['a']), np.array(pandas_describe['a']))
