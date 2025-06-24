import duckdb
import pytest


class TestRAPIDescription(object):
    def test_rapi_description(self, duckdb_cursor):
        res = duckdb_cursor.query('select 42::INT AS a, 84::BIGINT AS b')
        desc = res.description
        names = [x[0] for x in desc]
        types = [x[1] for x in desc]
        assert names == ['a', 'b']
        assert types == ['NUMBER', 'NUMBER']

    def test_rapi_describe(self, duckdb_cursor):
        np = pytest.importorskip("numpy")
        pd = pytest.importorskip("pandas")
        res = duckdb_cursor.query('select 42::INT AS a, 84::BIGINT AS b')
        duck_describe = res.describe().df()
        np.testing.assert_array_equal(duck_describe['aggr'], ['count', 'mean', 'stddev', 'min', 'max', 'median'])
        np.testing.assert_array_equal(duck_describe['a'], [1, 42, float('nan'), 42, 42, 42])
        np.testing.assert_array_equal(duck_describe['b'], [1, 84, float('nan'), 84, 84, 84])

        # now with more values
        res = duckdb_cursor.query(
            'select CASE WHEN i%2=0 THEN i ELSE NULL END AS i, i * 10 AS j, (i * 23 // 27)::DOUBLE AS k FROM range(10000) t(i)'
        )
        duck_describe = res.describe().df()
        np.testing.assert_allclose(duck_describe['i'], [5000.0, 4999.0, 2887.0400066504103, 0.0, 9998.0, 4999.0])
        np.testing.assert_allclose(duck_describe['j'], [10000.0, 49995.0, 28868.956799071675, 0.0, 99990.0, 49995.0])
        np.testing.assert_allclose(duck_describe['k'], [10000.0, 4258.3518, 2459.207430770227, 0.0, 8517.0, 4258.5])

        # describe data with other (non-numeric) types
        res = duckdb_cursor.query("select 'hello world' AS a, [1, 2, 3] AS b")
        duck_describe = res.describe().df()
        assert len(duck_describe) > 0

        # describe mixed table
        res = duckdb_cursor.query("select 42::INT AS a, 84::BIGINT AS b, 'hello world' AS c")
        duck_describe = res.describe().df()
        np.testing.assert_array_equal(duck_describe['a'], [1, 42, float('nan'), 42, 42, 42])
        np.testing.assert_array_equal(duck_describe['b'], [1, 84, float('nan'), 84, 84, 84])

        # timestamps
        res = duckdb_cursor.query("select timestamp '1992-01-01', date '2000-01-01'")
        duck_describe = res.describe().df()
        assert len(duck_describe) > 0

        # describe empty result
        res = duckdb_cursor.query("select 42 AS a LIMIT 0")
        duck_describe = res.describe().df()
        assert len(duck_describe) > 0
