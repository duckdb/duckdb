# timestamp ms precision

import numpy
from datetime import datetime


class TestNumpyTimestampMilliseconds(object):
    def test_numpy_timestamp(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIMESTAMP '2019-11-26 21:11:42.501' as test_time").fetchnumpy()
        assert res['test_time'] == numpy.datetime64('2019-11-26 21:11:42.501')


class TestTimestampMilliseconds(object):
    def test_numpy_timestamp(self, duckdb_cursor):
        res = duckdb_cursor.execute("SELECT TIMESTAMP '2019-11-26 21:11:42.501' as test_time").fetchone()[0]
        assert res == datetime.strptime('2019-11-26 21:11:42.501', '%Y-%m-%d %H:%M:%S.%f')
