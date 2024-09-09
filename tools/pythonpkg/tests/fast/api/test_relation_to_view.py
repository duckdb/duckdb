import pytest
import duckdb


class TestRelationToView(object):
    def test_values_to_view(self, duckdb_cursor):
        rel = duckdb_cursor.values(['test', 'this is a long string'])
        res = rel.fetchall()
        assert res == [('test', 'this is a long string')]

        rel.to_view('vw1')

        view = duckdb_cursor.table('vw1')
        res = view.fetchall()
        assert res == [('test', 'this is a long string')]

    def test_relation_to_view(self, duckdb_cursor):
        rel = duckdb_cursor.sql("select 'test', 'this is a long string'")

        res = rel.fetchall()
        assert res == [('test', 'this is a long string')]

        rel.to_view('vw1')

        view = duckdb_cursor.table('vw1')
        res = view.fetchall()
        assert res == [('test', 'this is a long string')]

    def test_registered_relation(self, duckdb_cursor):
        rel = duckdb_cursor.sql("select 'test', 'this is a long string'")

        con = duckdb.connect()
        # Register on a different connection is not allowed
        with pytest.raises(
            duckdb.InvalidInputException,
            match='was created by another Connection and can therefore not be used by this Connection',
        ):
            con.register('cross_connection', rel)

        # Register on the same connection just creates a view
        duckdb_cursor.register('same_connection', rel)
        view = duckdb_cursor.table('same_connection')
        res = view.fetchall()
        assert res == [('test', 'this is a long string')]
