import numpy
import pandas


class TestUhugeint(object):
    def test_uhugeint(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT 437894723897234238947043214::UHUGEINT')
        result = duckdb_cursor.fetchall()
        assert result == [(437894723897234238947043214,)]

    def test_uhugeint_numpy(self, duckdb_cursor):
        duckdb_cursor.execute('SELECT 1::UHUGEINT AS i')
        result = duckdb_cursor.fetchnumpy()
        assert result == {'i': numpy.array([1.0])}
