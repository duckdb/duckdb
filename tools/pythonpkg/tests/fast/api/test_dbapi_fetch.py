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

    @pytest.mark.parametrize(
        'test_case',
        [
            ('BOOLEAN', False),
            ('VARCHAR', '🦆🦆🦆🦆🦆🦆'),
            ('TINYINT', -128),
            ('SMALLINT', -32768),
            ('INTEGER', -2147483648),
            ('BIGINT', -9223372036854775808),
            ('HUGEINT', -170141183460469231731687303715884105728),
            ('UTINYINT', 0),
            ('USMALLINT', 0),
            ('UINTEGER', 0),
            ('UBIGINT', 0),
            ('UHUGEINT', 0),
            ('BITSTRING', '0010001001011100010101011010111'),
        ],
    )
    def test_fetch_dict_coverage(self, duckdb_cursor, test_case):
        key_type, expected = test_case
        query = f"""
        with map_cte as (
            select * from test_vector_types(
                NULL::MAP(
                    {key_type},
                    INTEGER
                )
            ) t(a) limit 1
        )
        select a from map_cte;
        """
        res = duckdb_cursor.sql(query).fetchone()
        print(res)
        assert res[0][expected] == -2147483648

    @pytest.mark.parametrize('test_case', ['VARCHAR[]'])
    def test_fetch_dict_key_not_hashable(self, duckdb_cursor, test_case):
        key_type = test_case
        query = f"""
        with map_cte as (
            select * from test_vector_types(
                NULL::MAP(
                    {key_type},
                    INTEGER
                )
            ) t(a) limit 1
        )
        select a from map_cte;
        """
        res = duckdb_cursor.sql(query).fetchone()
        assert 'key' in res[0].keys()
