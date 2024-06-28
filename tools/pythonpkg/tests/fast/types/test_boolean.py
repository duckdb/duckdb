import duckdb
import numpy


class TestBoolean(object):
    def test_bool(self, duckdb_cursor):
        duckdb_cursor.execute("SELECT TRUE")
        results = duckdb_cursor.fetchall()
        assert results[0][0] == True
