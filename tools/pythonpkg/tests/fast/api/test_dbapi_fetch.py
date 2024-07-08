import duckdb
import pytest
from uuid import UUID
import datetime
from decimal import Decimal


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
            (False, 'BOOLEAN', False),
            (-128, 'TINYINT', -128),
            (-32768, 'SMALLINT', -32768),
            (-2147483648, 'INTEGER', -2147483648),
            (-9223372036854775808, 'BIGINT', -9223372036854775808),
            (-170141183460469231731687303715884105728, 'HUGEINT', -170141183460469231731687303715884105728),
            (0, 'UTINYINT', 0),
            (0, 'USMALLINT', 0),
            (0, 'UINTEGER', 0),
            (0, 'UBIGINT', 0),
            (0, 'UHUGEINT', 0),
            (1.3423423767089844, 'FLOAT', 1.3423424),
            (1.3423424, 'DOUBLE', 1.3423424),
            (Decimal('1.342342'), 'DECIMAL(10, 6)', 1.342342),
            ('hello', "ENUM('world', 'hello')", 'hello'),
            ('🦆🦆🦆🦆🦆🦆', 'VARCHAR', '🦆🦆🦆🦆🦆🦆'),
            (b'thisisalongblob\x00withnullbytes', 'BLOB', 'thisisalongblob\\x00withnullbytes'),
            ('0010001001011100010101011010111', 'BITSTRING', '0010001001011100010101011010111'),
            ('290309-12-22 (BC) 00:00:00', 'TIMESTAMP', '290309-12-22 (BC) 00:00:00'),
            ('290309-12-22 (BC) 00:00:00', 'TIMESTAMP_MS', '290309-12-22 (BC) 00:00:00'),
            (datetime.datetime(1677, 9, 22, 0, 0), 'TIMESTAMP_NS', '1677-09-22 00:00:00'),
            ('290309-12-22 (BC) 00:00:00', 'TIMESTAMP_S', '290309-12-22 (BC) 00:00:00'),
            ('290309-12-22 (BC) 00:00:30+00', 'TIMESTAMPTZ', '290309-12-22 (BC) 00:17:30+00:17'),
            (
                datetime.time(0, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=57599))),
                'TIMETZ',
                '00:00:00+15:59:59',
            ),
            ('5877642-06-25 (BC)', 'DATE', '5877642-06-25 (BC)'),
            (UUID('cd57dfbd-d65f-4e15-991e-2a92e74b9f79'), 'UUID', 'cd57dfbd-d65f-4e15-991e-2a92e74b9f79'),
            (datetime.timedelta(days=90), 'INTERVAL', '3 months'),
            ('🦆🦆🦆🦆🦆🦆', 'UNION(a int, b bool, c varchar)', '🦆🦆🦆🦆🦆🦆'),
        ],
    )
    def test_fetch_dict_coverage(self, duckdb_cursor, test_case):
        duckdb_cursor.execute("set timezone='UTC'")
        duckdb_cursor.execute("set calendar='gregorian'")

        python_key, key_type, expected = test_case
        query = f"""
        with map_cte as (
            select MAP {{'{expected}'::{key_type}: -2147483648}} a
        )
        select a from map_cte;
        """
        res = duckdb_cursor.sql(query).fetchone()
        print(res)
        print(res[0].keys())
        assert res[0][python_key] == -2147483648

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
