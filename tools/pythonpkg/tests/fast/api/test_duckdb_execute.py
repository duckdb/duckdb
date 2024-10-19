import duckdb
import pytest


class TestDuckDBExecute(object):
    def test_execute_basic(self, duckdb_cursor):
        duckdb_cursor.execute('create table t as select 5')
        res = duckdb_cursor.table('t').fetchall()
        assert res == [(5,)]

    def test_execute_many_basic(self, duckdb_cursor):
        duckdb_cursor.execute("create table t(x int);")

        # This works because prepared parameter is only present in the last statement
        duckdb_cursor.execute(
            """
            delete from t where x=5;
            insert into t(x) values($1);
        """,
            (99,),
        )
        res = duckdb_cursor.table('t').fetchall()
        assert res == [(99,)]

    @pytest.mark.parametrize(
        'rowcount',
        [
            50,
            2048,
            5000,
            100000,
            1000000,
            10000000,
        ],
    )
    def test_large_execute(self, duckdb_cursor, rowcount):
        def generator(rowcount):
            count = 0
            while count < rowcount:
                yield min(2048, rowcount - count)
                count += 2048

        # FIXME: perhaps we want to test with different buffer sizes?
        # duckdb_cursor.execute("set streaming_buffer_size='1mb'")
        duckdb_cursor.execute(f"create table tbl as from range({rowcount})")
        duckdb_cursor.execute("select * from tbl")
        for rows in generator(rowcount):
            tuples = duckdb_cursor.fetchmany(rows)
            assert len(tuples) == rows

    def test_execute_many_error(self, duckdb_cursor):
        duckdb_cursor.execute("create table t(x int);")

        # Prepared parameter used in a statement that is not the last
        with pytest.raises(
            duckdb.NotImplementedException, match='Prepared parameters are only supported for the last statement'
        ):
            duckdb_cursor.execute(
                """
                delete from t where x=$1;
                insert into t(x) values($1);
            """,
                (99,),
            )

    def test_execute_many_generator(self, duckdb_cursor):
        to_insert = [[1], [2], [3]]

        def to_insert_from_generator(what):
            for x in what:
                yield x

        gen = to_insert_from_generator(to_insert)
        duckdb_cursor.execute("CREATE TABLE unittest_generator (a INTEGER);")
        duckdb_cursor.executemany("INSERT into unittest_generator (a) VALUES (?)", gen)
        assert duckdb_cursor.table('unittest_generator').fetchall() == [(1,), (2,), (3,)]

    def test_execute_multiple_statements(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({'a': [5, 6, 7, 8]})
        sql = """
            select * from df;
            select * from VALUES (1),(2),(3),(4) t(a);
        """
        duckdb_cursor.execute(sql)
        res = duckdb_cursor.fetchall()
        assert res == [(1,), (2,), (3,), (4,)]
