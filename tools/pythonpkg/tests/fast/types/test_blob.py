import duckdb
import numpy

class TestBlob(object):
    def test_blob(self, duckdb_cursor):
        duckdb_cursor.execute("SELECT BLOB 'hello'")
        results = duckdb_cursor.fetchall()
        assert results[0][0] == b'hello'

        duckdb_cursor.execute("SELECT BLOB 'hello' AS a")
        results = duckdb_cursor.fetchnumpy()
        assert results['a'] == numpy.array([b'hello'], dtype=object)
