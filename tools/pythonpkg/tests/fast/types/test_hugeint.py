import numpy
import pandas


class TestHugeint(object):
    def test_hugeint(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT 437894723897234238947043214')
        result = duckdb_cursor.fetchall()
        assert result == [(437894723897234238947043214,)]

    def test_hugeint_numpy(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT 1::HUGEINT AS i')
        result = duckdb_cursor.fetchnumpy()
        assert result == {'i': numpy.array([1.0])}
