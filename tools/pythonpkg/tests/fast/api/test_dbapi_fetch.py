import duckdb
import pytest


class TestDBApiFetch(object):
    def test_multiple_fetch_one(self, duckdb_cursor):
        con = duckdb.connect()
        c = con.execute('SELECT 42')
        assert c.fetchone() == (42,)
        assert c.fetchone() is None
        assert c.fetchone() is None

    def test_multiple_fetch_all(self, duckdb_cursor):
        con = duckdb.connect()
        c = con.execute('SELECT 42')
        assert c.fetchall() == [(42,)]
        assert c.fetchall() == []
        assert c.fetchall() == []

    def test_multiple_fetch_many(self, duckdb_cursor):
        con = duckdb.connect()
        c = con.execute('SELECT 42')
        assert c.fetchmany(1000) == [(42,)]
        assert c.fetchmany(1000) == []
        assert c.fetchmany(1000) == []

    def test_multiple_fetch_df(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        con = duckdb.connect()
        c = con.execute('SELECT 42::BIGINT AS a')
        pd.testing.assert_frame_equal(c.df(), pd.DataFrame.from_dict({'a': [42]}))
        assert c.df() is None
        assert c.df() is None

    def test_multiple_fetch_arrow(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        arrow = pytest.importorskip("pyarrow")
        con = duckdb.connect()
        c = con.execute('SELECT 42::BIGINT AS a')
        table = c.arrow()
        df = table.to_pandas()
        pd.testing.assert_frame_equal(df, pd.DataFrame.from_dict({'a': [42]}))
        assert c.arrow() is None
        assert c.arrow() is None

    def test_multiple_close(self, duckdb_cursor):
        con = duckdb.connect()
        c = con.execute('SELECT 42')
        c.close()
        c.close()
        c.close()
        with pytest.raises(duckdb.InvalidInputException, match='No open result set'):
            c.fetchall()

    def test_multiple_fetch_all_relation(self, duckdb_cursor):
        res = duckdb_cursor.query('SELECT 42')
        assert res.fetchall() == [(42,)]
        assert res.fetchall() == [(42,)]
        assert res.fetchall() == [(42,)]

    def test_multiple_fetch_many_relation(self, duckdb_cursor):
        res = duckdb_cursor.query('SELECT 42')
        assert res.fetchmany(10000) == [(42,)]
        assert res.fetchmany(10000) == []
        assert res.fetchmany(10000) == []

    def test_fetch_one_relation(self, duckdb_cursor):
        res = duckdb_cursor.query('SELECT * FROM range(3)')
        assert res.fetchone() == (0,)
        assert res.fetchone() == (1,)
        assert res.fetchone() == (2,)
        assert res.fetchone() is None
        assert res.fetchone() is None
        assert res.fetchone() is None
        assert res.fetchall() == []
        # we can execute explicitly to reset the result
        res.execute()
        assert res.fetchone() == (0,)
        res.execute()
        assert res.fetchone() == (0,)
        assert res.fetchone() == (1,)
        assert res.fetchone() == (2,)
        assert res.fetchone() is None
