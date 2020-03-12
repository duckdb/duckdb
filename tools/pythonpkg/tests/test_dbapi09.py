# date type

import numpy 
import datetime

class TestNumpyDate(object):
    def test_fetchall_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchall()
        assert isinstance(res[0][0], datetime.date)

    def test_fetchnumpy_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchnumpy()
        assert res['test_date'].dtype == numpy.dtype('datetime64[s]')

    def test_fetchdf_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchdf()
        assert res['test_date'].dtype == numpy.dtype('datetime64[ns]')
