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
