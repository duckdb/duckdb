# time type

import numpy
import datetime
import pandas

class TestNumpyTime(object):
    def test_fetchall_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIME '13:06:40' as test_time").fetchall()
        assert res == [(datetime.time(13, 6, 40),)]

    def test_fetchnumpy_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIME '13:06:40' as test_time").fetchnumpy()
        arr = numpy.array(['13:06:40'], dtype="object")
        arr = numpy.ma.masked_array(arr)
        numpy.testing.assert_array_equal(res['test_time'], arr)

    def test_fetchdf_date(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIME '13:06:40' as test_time").fetchdf()
        ser = pandas.Series(numpy.array(['13:06:40'], dtype="object"), name="test_time")
        pandas.testing.assert_series_equal(res['test_time'], ser)
