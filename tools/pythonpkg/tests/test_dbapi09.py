# date type

import numpy
import datetime
import pandas

class TestNumpyDate(object):
    def test_fetchall_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchall()
        assert res == [(datetime.date(2020, 1, 10),)]

    def test_fetchnumpy_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchnumpy()
        arr = numpy.array(['2020-01-10'], dtype="datetime64[s]")
        arr = numpy.ma.masked_array(arr)
        numpy.testing.assert_array_equal(res['test_date'], arr)

    def test_fetchdf_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT DATE '2020-01-10' as test_date").fetchdf()
        ser = pandas.Series(numpy.array(['2020-01-10'], dtype="datetime64[ns]"), name="test_date")
        pandas.testing.assert_series_equal(res['test_date'], ser)
